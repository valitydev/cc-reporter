# CC Reporter

Сервис асинхронного построения отчётов по платежам и выводам. Очередь заданий, попытки, таймауты и терминальные
состояния хранятся в PostgreSQL; отчёты обрабатываются ограниченным пулом воркеров по согласованному снимку данных.

## CSV

### Формат

| Параметр                 | Значение                                         |
|--------------------------|--------------------------------------------------|
| Кодировка                | UTF-8                                            |
| Разделитель              | `,`                                              |
| Конец строки             | CRLF                                             |
| Экранирование            | RFC 4180                                         |
| `null`                   | Пустое поле                                      |
| Дата                     | `yyyy-MM-dd`                                     |
| Время                    | `HH:mm:ss`                                       |
| Timezone                 | `CreateReportRequest.timezone`, по умолчанию UTC |
| Денежные значения        | Decimal по exponent соответствующей валюты       |
| `exchange_rate_internal` | Decimal без экспоненциальной записи              |

### Payments

| CSV-поле                 | Значение                              | Источник                                     |
|--------------------------|---------------------------------------|----------------------------------------------|
| `created_date`           | Дата создания платежа                 | `payment_txn_current.created_at`             |
| `created_time`           | Время создания платежа                | `payment_txn_current.created_at`             |
| `finalized_date`         | Дата первого терминального состояния  | `payment_txn_current.finalized_at`           |
| `finalized_time`         | Время первого терминального состояния | `payment_txn_current.finalized_at`           |
| `invoice_id`             | Идентификатор инвойса                 | `payment_txn_current.invoice_id`             |
| `payment_id`             | Идентификатор платежа                 | `payment_txn_current.payment_id`             |
| `status`                 | Статус платежа                        | `payment_txn_current.status`                 |
| `amount`                 | Сумма платежа                         | `payment_txn_current.amount`                 |
| `currency`               | Валюта платежа                        | `payment_txn_current.currency`               |
| `trx_id`                 | Идентификатор транзакции провайдера   | `payment_txn_current.trx_id`                 |
| `provider_id`            | Идентификатор провайдера              | `payment_txn_current.provider_id`            |
| `terminal_id`            | Идентификатор терминала               | `payment_txn_current.terminal_id`            |
| `shop_id`                | Идентификатор магазина                | `payment_txn_current.shop_id`                |
| `exchange_rate_internal` | Внутренний курс конвертации           | `payment_txn_current.exchange_rate_internal` |
| `provider_amount`        | Сумма на стороне провайдера           | `payment_txn_current.provider_amount`        |
| `provider_currency`      | Валюта суммы провайдера               | `payment_txn_current.provider_currency`      |
| `original_amount`        | Исходная сумма до конвертации         | `payment_txn_current.original_amount`        |
| `original_currency`      | Валюта исходной суммы                 | `payment_txn_current.original_currency`      |
| `converted_amount`       | Сконвертированная сумма               | `payment_txn_current.converted_amount`       |

### Withdrawals

| CSV-поле                 | Значение                                  | Источник                                        |
|--------------------------|-------------------------------------------|-------------------------------------------------|
| `created_date`           | Дата создания вывода                      | `withdrawal_txn_current.created_at`             |
| `created_time`           | Время создания вывода                     | `withdrawal_txn_current.created_at`             |
| `finalized_date`         | Дата первого терминального состояния      | `withdrawal_txn_current.finalized_at`           |
| `finalized_time`         | Время первого терминального состояния     | `withdrawal_txn_current.finalized_at`           |
| `withdrawal_id`          | Идентификатор вывода                      | `withdrawal_txn_current.withdrawal_id`          |
| `status`                 | Статус вывода                             | `withdrawal_txn_current.status`                 |
| `amount`                 | Сумма вывода                              | `withdrawal_txn_current.amount`                 |
| `currency`               | Валюта вывода                             | `withdrawal_txn_current.currency`               |
| `trx_id`                 | Идентификатор транзакции последней сессии | `withdrawal_session.trx_id`                     |
| `provider_id`            | Идентификатор провайдера                  | `withdrawal_txn_current.provider_id`            |
| `terminal_id`            | Идентификатор терминала                   | `withdrawal_txn_current.terminal_id`            |
| `wallet_id`              | Идентификатор кошелька                    | `withdrawal_txn_current.wallet_id`              |
| `exchange_rate_internal` | Внутренний курс конвертации               | `withdrawal_txn_current.exchange_rate_internal` |
| `provider_amount`        | Сумма на стороне провайдера               | `withdrawal_txn_current.provider_amount`        |
| `provider_currency`      | Валюта суммы провайдера                   | `withdrawal_txn_current.provider_currency`      |
| `original_amount`        | Исходная сумма до конвертации             | `withdrawal_txn_current.original_amount`        |
| `original_currency`      | Валюта исходной суммы                     | `withdrawal_txn_current.original_currency`      |
| `converted_amount`       | Сконвертированная сумма                   | `withdrawal_txn_current.converted_amount`       |

## Документация

- [Модель данных и источники полей](docs/docs.md)
- [Требования и архитектурные решения](docs/PLAN.md)
