# CSV Report Format (CC Reporter, final candidate)

Документ описывает, как будет выглядеть итоговый CSV-файл для выгрузки. Для каждого типа отчета используется один фиксированный формат без дополнительных профилей.

Здесь зафиксированы:

1. состав колонок в файле;
2. понятный смысл этих колонок;
3. технический источник значений для технарей.

## Общие правила

1. Encoding: UTF-8.
2. Escaping: RFC4180.
3. Decimal separator: `.`.
4. Timestamps форматируются в timezone отчета (`CreateReportRequest.timezone`, по умолчанию `UTC`).
5. `created_*` и `finalized_*` всегда разделены на отдельные колонки `date` и `time`.
6. `finalized_*` пустые для non-terminal статусов.
7. `null` сериализуется как пустая CSV-ячейка.
8. Порядок колонок фиксирован и меняется только новой версией контракта.
9. Понятные человеку названия (`shop_name`, `wallet_name`, `provider_name`, `terminal_name`) могут использоваться для поиска внутри системы, но в текущем CSV-контракте не считаются окончательно зафиксированными.

## Payments CSV (`payments_csv`)

### Порядок колонок в файле

1. `created_date`
2. `created_time`
3. `finalized_date`
4. `finalized_time`
5. `invoice_id`
6. `payment_id`
7. `status`
8. `amount`
9. `currency`
10. `trx_id`
11. `provider_id`
12. `terminal_id`
13. `shop_id`
14. `exchange_rate_internal`
15. `provider_amount`
16. `provider_currency`
17. `original_amount`
18. `original_currency`
19. `converted_amount`

### Что означают ключевые колонки

1. `amount`:
   Основная сумма платежа в валюте самой операции.
2. `currency`:
   Валюта основной суммы платежа.
3. `trx_id`:
   Идентификатор транзакции на стороне внешнего провайдера или платежного канала.
4. `exchange_rate_internal`:
   Внутренний курс конвертации, который использовала наша система при пересчете суммы между валютами. Это значение показывает, по какому курсу система рассчитала итоговую сумму при валютной операции.
5. `provider_amount`:
   Сумма, которая фактически была передана провайдеру для обработки платежа.
6. `provider_currency`:
   Валюта суммы, которая была передана провайдеру.
7. `original_amount`:
   Исходная сумма до конвертации, если операция была валютной.
8. `original_currency`:
   Валюта исходной суммы до конвертации.
9. `converted_amount`:
   Сумма после конвертации в валюте `currency`.

### Технический источник значения (для технарей)

| CSV | CCR source |
|---|---|
| `created_date` / `created_time` | `payment_txn_current.created_at` |
| `finalized_date` / `finalized_time` | `payment_txn_current.finalized_at` |
| `invoice_id` | `payment_txn_current.invoice_id` |
| `payment_id` | `payment_txn_current.payment_id` |
| `status` | `payment_txn_current.status` |
| `amount` | `payment_txn_current.amount` |
| `currency` | `payment_txn_current.currency` |
| `trx_id` | `payment_txn_current.trx_id` |
| `provider_id` | `payment_txn_current.provider_id` |
| `terminal_id` | `payment_txn_current.terminal_id` |
| `shop_id` | `payment_txn_current.shop_id` |
| `exchange_rate_internal` | `payment_txn_current.exchange_rate_internal` |
| `provider_amount` | `payment_txn_current.provider_amount` |
| `provider_currency` | `payment_txn_current.provider_currency` |
| `original_amount` | `payment_txn_current.original_amount` |
| `original_currency` | `payment_txn_current.original_currency` |
| `converted_amount` | `payment_txn_current.converted_amount` |

## Withdrawals CSV (`withdrawals_csv`)

### Порядок колонок в файле

1. `created_date`
2. `created_time`
3. `finalized_date`
4. `finalized_time`
5. `withdrawal_id`
6. `status`
7. `amount`
8. `currency`
9. `trx_id`
10. `provider_id`
11. `terminal_id`
12. `wallet_id`
13. `exchange_rate_internal`
14. `provider_amount`
15. `provider_currency`
16. `original_amount`
17. `original_currency`
18. `converted_amount`

### Что означают ключевые колонки

1. `amount`:
   Основная сумма выплаты в валюте самой выплаты.
2. `currency`:
   Валюта основной суммы выплаты.
3. `trx_id`:
   Идентификатор операции на стороне внешнего провайдера или канала выплаты.
4. `exchange_rate_internal`:
   Внутренний курс конвертации, который использовала наша система, если сумма выплаты пересчитывалась между валютами.
5. `provider_amount`:
   Сумма, которая фактически была передана провайдеру для выполнения выплаты.
6. `provider_currency`:
   Валюта суммы, которая была передана провайдеру.
7. `original_amount`:
   Исходная сумма до конвертации, если выплата была валютной.
8. `original_currency`:
   Валюта исходной суммы до конвертации.
9. `converted_amount`:
   Сумма после конвертации в валюте `currency`.

### Технический источник значения (для backend/QA)

| CSV | CCR source |
|---|---|
| `created_date` / `created_time` | `withdrawal_txn_current.created_at` |
| `finalized_date` / `finalized_time` | `withdrawal_txn_current.finalized_at` |
| `withdrawal_id` | `withdrawal_txn_current.withdrawal_id` |
| `status` | `withdrawal_txn_current.status` |
| `amount` | `withdrawal_txn_current.amount` |
| `currency` | `withdrawal_txn_current.currency` |
| `trx_id` | `withdrawal_txn_current.trx_id` |
| `provider_id` | `withdrawal_txn_current.provider_id` |
| `terminal_id` | `withdrawal_txn_current.terminal_id` |
| `wallet_id` | `withdrawal_txn_current.wallet_id` |
| `exchange_rate_internal` | `withdrawal_txn_current.exchange_rate_internal` |
| `provider_amount` | `withdrawal_txn_current.provider_amount` |
| `provider_currency` | `withdrawal_txn_current.provider_currency` |
| `original_amount` | `withdrawal_txn_current.original_amount` |
| `original_currency` | `withdrawal_txn_current.original_currency` |
| `converted_amount` | `withdrawal_txn_current.converted_amount` |

## Как показываются денежные значения

1. `amount` и `converted_amount` форматируются по exponent валюты из `currency`.
2. `original_amount` форматируется по exponent валюты из `original_currency`.
3. `provider_amount` форматируется по exponent валюты из `provider_currency`; если `provider_currency` пустая, используется exponent из `currency`.
4. `exchange_rate_internal` показывается как обычное десятичное число, без инженерной записи через степень. Например: `1.25`, а не `1.25E0`.

## Как заполняются поля в нестандартных случаях

1. `trx_id` заполняется только если исходная система действительно передает идентификатор операции; иначе поле может остаться пустым.
2. FX/conversion поля допускают `null`, если для канала или конкретной транзакции конвертации не было.
3. `provider_currency` должно быть заполнено, если `provider_amount` указана в валюте, отличной от `currency`; если исходная система не передает валюту провайдера отдельно, поле может остаться пустым.
4. `finalized_date` / `finalized_time` заполняются только для конечных статусов операции.
5. `finalized_at` фиксируется как момент первого перехода в конечный статус и после этого не должен изменяться последующими уточняющими обновлениями.
