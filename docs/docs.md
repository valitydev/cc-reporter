## Как живёт отчёт

```text
                                      ┌── временная ошибка ──> pending(next_attempt_at)
                                      │
pending ── claim ──> processing ──────┼── успех ──> created ── TTL ──> expired
   │                                  │
   └── cancel ──> canceled            ├── закончились попытки ──> failed
                                      ├── hard timeout ──> timed_out
                                      └── cancel ──> canceled
```

`pending → processing` выполняется атомарно. Claim выбирает только наступившие по `next_attempt_at` задания,
блокирует строку через `FOR UPDATE SKIP LOCKED`, увеличивает `attempt`, записывает новый `started_at` и очищает ошибку
предыдущей попытки.

Scheduler запускает до `ccr.report.worker-concurrency` отчётов одновременно. Executor имеет фиксированный размер,
равный этому параметру. Следующая порция заданий выбирается после завершения текущей.

Одна попытка ограничена `ccr.report.processing-timeout-ms`. По достижении deadline worker task сначала получает
`cancel(true)`, затем строка условно переводится из `processing` в `timed_out`. Генерация CSV проверяет interrupt между
строками, а SQL работает с локальным PostgreSQL `statement_timeout`.

Stale cleanup использует тот же timeout и закрывает оставшиеся `processing` после остановки процесса или потери
instance. Позднее завершение не может перезаписать `timed_out`, `canceled` или другой терминальный статус:
`completeReport`, retry, failure и timeout обновляют строку только из ожидаемого исходного статуса.

При временной ошибке отчёт возвращается в `pending`. `next_attempt_at` рассчитывается от фактического времени ошибки.
После исчерпания `max-attempts` отчёт переходит в `failed`.

`processing → created` и добавление `report_file` выполняются в одной транзакции. Готовый отчёт переходит в `expired`
после `expires_at`.

Cancel разрешён только для `pending` и `processing`, устанавливает `finished_at` и очищает `next_attempt_at`.

## Соединения при построении отчёта

Один отчёт удерживает одно соединение на время транзакции `READ ONLY REPEATABLE READ`.

Параметры по умолчанию:

- report workers: `2`;
- Hikari maximum pool size: `8`;
- Hikari connection timeout: `5` секунд;
- PostgreSQL connect timeout: `5` секунд;
- PostgreSQL socket timeout: `1260` секунд;
- PostgreSQL cancel signal timeout: `5` секунд;
- PostgreSQL TCP keepalive: включён.

При старте сервис проверяет условие `maximum-pool-size >= worker-concurrency + 2`. Резервные соединения требуются для
условного обновления статуса после отмены worker task и прочих транзакций instance. Для смешанной нагрузки
рекомендуется резерв `+4`.

## Вычитывание полей `amount`, `provider`, `original` из потока событий

### Платежи

Класс: `PaymentEventProjector`

- `InvoicePaymentStarted`
    - при первом старте платежа заполняются основные денежные поля;
    - `amount` и `currency` берутся из `payment.cost`;
    - `providerId` и `terminalId` берутся из `started.route`, если маршрут уже есть;
    - `originalAmount` и `originalCurrency` на этом шаге тоже ставятся из `payment.cost`.

- `InvoicePaymentRouteChanged`
    - обновляет только маршрут;
    - `providerId` и `terminalId` перечитываются из нового `route`.

- `InvoicePaymentCashChanged`
    - обновляет сумму платежа;
    - `amount` и `currency` берутся из `newCash`.

- `InvoicePaymentCashFlowChanged`
    - пересчитывает денежные значения по проводкам;
    - `amount` берётся через `DomainCashFlowExtractor.extractPaymentAmount(...)`;
    - дополнительно здесь же обновляется `fee`;
    - `originalAmount` и `originalCurrency` это событие не меняет.

- `InvoicePaymentStatusChanged`
    - не меняет пользовательскую сумму платежа;
    - обновляет провайдерскую сторону расчёта:
    - `providerAmount` и `providerCurrency` берутся из `capturedCost`, если он есть в статусе.

Итого по платежу:

- обычная сумма платежа сначала приходит из `InvoicePaymentStarted`, потом может поменяться через
  `InvoicePaymentCashChanged` или `InvoicePaymentCashFlowChanged`;
- провайдер определяется сначала в `InvoicePaymentStarted`, потом может быть переопределён через
  `InvoicePaymentRouteChanged`;
- `originalAmount` и `originalCurrency` выставляются только на старте платежа и дальше этим проектором не обновляются.

### Выводы

Класс: `WithdrawalEventProjector`

- `Created`
    - на создании вывода заполняются и текущая сумма, и исходная сумма, и маршрут;
    - `amount` и `currency` берутся из `withdrawal.body`;
    - `providerId` и `terminalId` берутся из `withdrawal.route`, если маршрут уже известен;
    - `originalAmount` и `originalCurrency` берутся из `quote.cashFrom`, если есть котировка;
    - дополнительно провайдерская сумма заполняется из `quote.cashTo`;
    - курс `exchangeRateInternal` считается как отношение `cashTo / cashFrom`.

- `Route`
    - обновляет только маршрут;
    - `providerId` и `terminalId` перечитываются из нового `route`.

- `StatusChanged`
    - сумму, маршрут и исходную сумму не меняет.

- `Transfer -> payload.created.transfer.cashflow`
    - обновляет только `fee`;
    - `amount`, `providerId`, `originalAmount` и связанные валюты не трогает.

Итого по выводу:

- `amount` приходит из события создания и дальше этим проектором не меняется;
- провайдер приходит из события создания и может обновиться отдельным событием смены маршрута;
- `originalAmount` и `originalCurrency` приходят из котировки в событии создания и дальше не меняются.

### Сессия вывода

Класс: `WithdrawalSessionEventProjector`

Этот проектор не работает с `amount`, `provider` и `original`. Он сохраняет связь с выводом и данные по транзакции (
`trxId`, `trxSearch`).
