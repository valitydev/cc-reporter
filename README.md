# CC Reporter

`cc-reporter` асинхронно строит CSV-отчёты по платежам и выводам. Сервис читает доменные события из Kafka,
поддерживает в PostgreSQL актуальное состояние транзакций и формирует отчёт по согласованному снимку данных.

Жизненный цикл задания:

```text
pending -> processing -> created -> expired
   |           |  |
   |           |  +-> failed / timed_out
   |           +----> pending (retry)
   +----------------> canceled
```

Готовый файл хранится во внешнем файловом хранилище и выдаётся по временной подписанной ссылке.

## CSV

Формат одинаков для всех отчётов:

| Параметр | Значение |
|---|---|
| Кодировка | UTF-8 |
| Разделитель | `,` |
| Конец строки | CRLF |
| Экранирование | RFC 4180 |
| `null` | Пустое поле |
| Дата | `yyyy-MM-dd` |
| Время | `HH:mm:ss` |
| Timezone | `CreateReportRequest.timezone`, по умолчанию UTC |
| Денежные значения | Decimal по экспоненте соответствующей валюты |
| `exchange_rate_internal` | Decimal без экспоненциальной записи |

`finalized_date` и `finalized_time` соответствуют текущему терминальному статусу. Если более новое событие
корректирует терминальный статус, время финализации также обновляется.

### Payments

```csv
created_date,created_time,finalized_date,finalized_time,invoice_id,payment_id,status,amount,currency,trx_id,provider_id,terminal_id,shop_id,exchange_rate_internal,provider_amount,provider_currency,original_amount,original_currency,converted_amount,converted_currency
2026-08-20,10:15:00,2026-08-20,10:15:04,invoice-1,payment-1,captured,1000.00,RUB,trx-1,12,34,shop-1,1.0000000000,1000.00,RUB,1000.00,RUB,1000.00,RUB
```

| CSV-поле | Источник |
|---|---|
| `created_date`, `created_time` | `payment_txn_current.created_at` |
| `finalized_date`, `finalized_time` | `payment_txn_current.finalized_at` |
| `invoice_id`, `payment_id`, `status` | `payment_txn_current` |
| `amount`, `currency` | `payment_txn_current` |
| `trx_id` | `payment_txn_current.trx_id` |
| `provider_id`, `terminal_id`, `shop_id` | `payment_txn_current` |
| `exchange_rate_internal` | `payment_txn_current.exchange_rate_internal` |
| `provider_amount`, `provider_currency` | `payment_txn_current` |
| `original_amount`, `original_currency`, `converted_amount`, `converted_currency` | `payment_txn_current` |

### Withdrawals

```csv
created_date,created_time,finalized_date,finalized_time,withdrawal_id,status,amount,currency,trx_id,provider_id,terminal_id,wallet_id,exchange_rate_internal,provider_amount,provider_currency,original_amount,original_currency,converted_amount,converted_currency
2026-08-20,11:20:00,2026-08-20,11:20:05,withdrawal-1,succeeded,5000.00,RUB,trx-2,12,34,wallet-1,83.3333333333,5000.00,RUB,60.00,USD,5000.00,RUB
```

| CSV-поле | Источник |
|---|---|
| `created_date`, `created_time` | `withdrawal_txn_current.created_at` |
| `finalized_date`, `finalized_time` | `withdrawal_txn_current.finalized_at` |
| `withdrawal_id`, `status` | `withdrawal_txn_current` |
| `amount`, `currency` | `withdrawal_txn_current`; обновляются при `body_changed` |
| `trx_id` | последняя `withdrawal_session` |
| `provider_id`, `terminal_id`, `wallet_id` | `withdrawal_txn_current` |
| `exchange_rate_internal` | `withdrawal_txn_current.exchange_rate_internal` |
| `provider_amount`, `provider_currency` | `withdrawal_txn_current` |
| `original_amount`, `original_currency`, `converted_amount`, `converted_currency` | `withdrawal_txn_current` |

## Документация

- [Модель данных, ingestion и жизненный цикл](docs/docs.md)
- [Требования и архитектурные решения](docs/PLAN.md)
