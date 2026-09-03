# Устройство CC Reporter

## Жизненный цикл отчёта

```text
                                      ┌── временная ошибка ──> pending(next_attempt_at)
                                      │
pending ── claim ──> processing ──────┼── успех ──> created ── TTL ──> expired
   │                                  │
   └── cancel ──> canceled            ├── закончились попытки ──> failed
                                      ├── hard timeout ──> timed_out
                                      └── cancel ──> canceled
```

Переход `pending -> processing` выполняется атомарно. DAO выбирает только задания, для которых наступил
`next_attempt_at`, блокирует строку через `FOR UPDATE SKIP LOCKED`, увеличивает `attempt`, записывает `started_at`
и очищает ошибку предыдущей попытки. Поэтому несколько экземпляров сервиса могут разбирать одну очередь без
двойной обработки одного задания.

Число одновременно выполняемых отчётов задаёт `report.worker-concurrency`. Одна попытка ограничена
`report.processing-timeout-ms`. При превышении лимита worker получает interrupt, а запись условно переводится
из `processing` в `timed_out`. Позднее завершение worker не может перезаписать уже установленный терминальный статус.

При временной ошибке отчёт возвращается в `pending` с новым `next_attempt_at`. После исчерпания попыток он
переходит в `failed`. Завершение отчёта и добавление `report_file` выполняются в одной транзакции.

Scheduler переводит готовые отчёты в `expired` после `expires_at`. `GetReport` и `GetReports` перед чтением также
выполняют идемпотентное истечение просроченных `created`-отчётов, поэтому корректность API не зависит от точности
срабатывания фонового scheduler. `GeneratePresignedUrl` дополнительно разрешает скачивание только пока отчёт
не просрочен.

## Согласованность current-state

`payment_txn_current` и `withdrawal_txn_current` содержат не историю, а последнее известное состояние сущности.
Обновление принимается только если `MachineEvent.eventId` больше уже сохранённого. Повторные и более старые события
не откатывают состояние назад.

Событие смены статуса является авторитетным для полей, которые зависят от статуса:

- `status` заменяется значением более нового status-event;
- для терминального статуса `finalized_at` становится временем этого события;
- если новый статус нетерминальный, `finalized_at` очищается;
- `error_summary` заменяется значением нового status-event и очищается, если новая ошибка отсутствует.

События, которые статус не меняют, эти поля сохраняют. Это важно для корректировок, меняющих один финальный статус
на другой.

## Payments

`PaymentEventProjector` обрабатывает изменения платежа по порядку внутри `MachineEvent`.

- `InvoicePaymentStarted` задаёт исходные идентификаторы, сумму, валюту, маршрут и статус `pending`.
- `InvoicePaymentRouteChanged` обновляет provider/terminal.
- `InvoicePaymentCashChanged` обновляет `amount` и `currency`.
- `InvoicePaymentCashFlowChanged` пересчитывает `amount` и `fee` через `CashFlowAmountExtractor`.
- `InvoicePaymentStatusChanged` обновляет статус, `finalized_at`, `error_summary` и данные `capturedCost`.
- `SessionTransactionBound` сохраняет `trx_id`, RRN, approval code и данные конвертации.
- `SessionProxyStateChanged` используется как fallback для `trx_id`.

Если в одном `MachineEvent` есть несколько изменений одного платежа, они объединяются в порядке поступления.
При нескольких status-event последнее изменение статуса определяет `status`, `finalized_at` и `error_summary`.

## Withdrawals

`WithdrawalEventProjector` обрабатывает:

- `created`: исходные данные вывода, body, маршрут и quote;
- `body_changed`: заменяет текущие `amount` и `currency` значениями из `new_body`;
- `route`: обновляет provider/terminal;
- `status_changed`: обновляет статус, `finalized_at` и `error_summary`;
- `transfer.payload.created.transfer.cashflow`: обновляет `fee`.

`original_amount`, `original_currency`, `provider_amount`, `provider_currency` и внутренний курс первоначально
вычисляются из quote события создания. Текущая сумма вывода при этом может измениться отдельным `body_changed`.

`WithdrawalSessionEventProjector` хранит связь с выводом и транзакционные данные сессии (`trx_id`, `trx_search`).
В CSV используется последняя подходящая сессия.

## Построение CSV

Один отчёт выполняется в транзакции `READ ONLY REPEATABLE READ` и использует один согласованный снимок PostgreSQL.
Данные читаются курсором, поэтому весь набор строк не загружается в память.

Денежные значения хранятся в minor units и при записи CSV переводятся в decimal по exponent валюты.
`exchange_rate_internal` записывается как обычное десятичное число без экспоненциальной формы.

Локальный временный CSV удаляется при ошибке генерации. После успешной загрузки жизненный цикл файла контролируется
через запись отчёта и TTL внешнего хранилища.
