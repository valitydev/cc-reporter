# Payment Report Amount Implementation

## Goal

Implement in `cc-reporter` a separate derived payment amount that matches the semantics of
`AS amount` in [`src/test/resources/payment.sql`]( /Users/karleowne/dev/cc-reporter/src/test/resources/payment.sql ).

This value should not be confused with:

- `original_amount` / `original_currency`
- current experimental FX fields
- `provider_amount` / `provider_currency`

It is a distinct concept:

- current report-facing payment amount
- expressed in the current payment currency
- with `cash_changed` priority
- with adjustment merchant deltas applied when relevant

## Source Of Truth Formula

`payment.sql` computes:

```sql
COALESCE(
  pcc.new_amount,
  (p.amount + COALESCE(cs.amount, 0))
)
```

Currency is chosen as:

```sql
COALESCE(pcc.new_currency_code, p.currency_code)
```

So the intended semantics are:

1. If there is a current payment cash change, use its `newCash` amount/currency.
2. Otherwise, use the original payment amount/currency from payment start.
3. Additionally, apply the sum of captured payment adjustment merchant deltas to the original amount.

Important:

- `cash_changed` has higher priority than base payment amount.
- captured adjustment deltas modify the base amount branch.
- this is not derived from payment status `captured.cost`.
- this is not provider amount.

## Recommended New Field

Add a separate field pair to `payment_txn_current`, for example:

- `report_amount`
- `report_currency`

Do not overload:

- `original_amount`
- `amount`
- `provider_amount`

until production behavior is observed and semantics are confirmed.

## Event Sources Needed

### 1. Base payment amount

Source event:

- `invoice_payment_started`

Source fields:

- `payment.cost.amount`
- `payment.cost.currency.symbolic_code`

Current `cc-reporter` status:

- already ingested by `PaymentEventProjector`

What to save:

- base/original payment amount
- base/original payment currency

These can be reused from existing started-event handling.

### 2. Cash override

Source event:

- `invoice_payment_cash_changed`

Source fields:

- `new_cash.amount`
- `new_cash.currency.symbolic_code`

Current `cc-reporter` status:

- already ingested by `PaymentEventProjector`

What to save:

- latest cash-changed amount
- latest cash-changed currency
- enough ordering info to know which cash change is current for the payment

Since current-state is per payment and ordered by `domain_event_id`, the latest seen cash change can be kept directly
inside the current-state row.

### 3. Payment adjustment merchant delta

Source event family:

- `invoice_payment_adjustment_change`

Concrete event types needed:

- `invoice_payment_adjustment_created`
- `invoice_payment_adjustment_status_changed`

Source fields needed from `invoice_payment_adjustment_created.adjustment`:

- `id`
- `status`
- `created_at`
- `new_cash_flow`
- `old_cash_flow_inverse`
- optionally scenario/state metadata for debugging

Source fields needed from `invoice_payment_adjustment_status_changed`:

- updated adjustment status
- captured / cancelled timestamps if present

## How Daway Computes Adjustment Delta

`daway` stores payment adjustments and their cash flows, then uses only captured adjustments in `payment.sql`.

Relevant references:

- [`InvoicePaymentAdjustmentCreatedHandler.java`]( /Users/karleowne/dev/refs/daway/src/main/java/dev/vality/daway/handler/event/stock/impl/invoicing/adjustment/InvoicePaymentAdjustmentCreatedHandler.java )
- [`InvoicePaymentAdjustmentStatusChangedHandler.java`]( /Users/karleowne/dev/refs/daway/src/main/java/dev/vality/daway/handler/event/stock/impl/invoicing/adjustment/InvoicePaymentAdjustmentStatusChangedHandler.java )
- [`AdjustmentUtils.java`]( /Users/karleowne/dev/refs/daway/src/main/java/dev/vality/daway/util/AdjustmentUtils.java )

Merchant amount delta is effectively:

- sum of `provider -> merchant` postings in `new_cash_flow`
- minus sum of `merchant -> provider` postings in `old_cash_flow_inverse`

In `daway`, `payment.sql` rebuilds this by scanning `dw.cash_flow` rows tied to captured adjustments.

## Recommended cc-reporter Design

### Option 1: Full explicit adjustment storage

Recommended if correctness matters and the value should match `daway` truth closely.

Add a dedicated payment adjustment current/storage model:

- `payment_adjustment_current` or similar

Minimum fields to store:

- `invoice_id`
- `payment_id`
- `adjustment_id`
- `domain_event_id`
- `domain_event_created_at`
- `status`
- `captured_at`
- `cancelled_at`
- `merchant_amount_delta`

Potentially also:

- `provider_amount_delta`
- `system_amount_delta`
- raw cash-flow-derived debug fields

How to populate:

- on `invoice_payment_adjustment_created`
  - compute `merchant_amount_delta` from `new_cash_flow` and `old_cash_flow_inverse`
  - store adjustment row
- on `invoice_payment_adjustment_status_changed`
  - update status/current timestamps

How to aggregate into payment current-state:

- when recomputing `report_amount`, sum all adjustment rows for the payment where status is `captured`
- `report_amount = cash_override if present else base_amount + captured_adjustment_delta_sum`
- `report_currency = cash_override currency if present else base currency`

### Option 2: Aggregate delta directly into payment current-state

Possible, but less robust.

Store in `payment_txn_current`:

- `captured_adjustment_amount_delta`

Update rules:

- on adjustment created: do nothing until status is known
- on status changed to `captured`: add merchant delta
- on transition away from captured: subtract merchant delta

Why this is weaker:

- harder to replay safely
- harder to debug
- more complex when statuses change multiple times
- no local history of individual adjustments

Recommendation:

- prefer explicit adjustment storage, not only an aggregated running total

## Event Priority Rules In cc-reporter

For the new report amount field pair:

1. `invoice_payment_started`
   - defines base amount/currency

2. `invoice_payment_cash_changed`
   - overrides report amount/currency directly

3. `invoice_payment_adjustment_*`
   - contributes merchant delta to the base branch
   - only adjustments with final/current status `captured` affect the report amount

4. `invoice_payment_status_changed`
   - does not define the report amount formula directly
   - may still be stored for other reporting fields

5. `invoice_payment_cash_flow_changed`
   - used for fee/provider-fee logic
   - should not be used as the primary source of this report amount

## Concrete Data To Save

### In `payment_txn_current`

Needed eventually:

- `report_amount`
- `report_currency`
- base amount/currency already effectively available from started event
- optional direct cash override marker/fields if not recomputed inline

### In adjustment storage

Needed:

- `invoice_id`
- `payment_id`
- `adjustment_id`
- `status`
- `merchant_amount_delta`
- event ordering fields

Nice to have:

- raw `new_cash_flow` / `old_cash_flow_inverse` JSON for debugging
- provider/system deltas for future analytics

## Implementation Sequence

1. Add new current-state fields:

- `report_amount`
- `report_currency`

2. Populate them initially from existing events only:

- base from `invoice_payment_started`
- override from `invoice_payment_cash_changed`

3. Add payment adjustment ingestion:

- parse `invoice_payment_adjustment_created`
- parse `invoice_payment_adjustment_status_changed`

4. Add adjustment storage table and DAO.

5. Compute and persist `merchant_amount_delta` per adjustment.

6. Fold captured adjustment deltas into `payment_txn_current.report_amount`.

7. Add scenario tests:

- payment with no cash change and no adjustment
- payment with cash change only
- payment with captured adjustment only
- payment with both cash change and adjustment
- replay / reordering behavior

## Current Gap

`cc-reporter` currently does not ingest payment adjustment events at all.

That means:

- full parity with `payment.sql AS amount` is not possible yet
- only the partial version can be implemented now:
  - base started cost
  - cash-changed override

The adjustment part requires a new ingestion/data-storage pipeline.

• Findings

2. High: payment amount сейчас маппится не по truth SQL-модели. В truth query payment.sql amount считается как
   COALESCE(payment_cash_change.new_amount, payment.amount + adjustment_delta), а текущий проектор пишет amount из
   invoice_payment_cash_flow_changed. Это другая семантика: cash flow в truth SQL идёт в fee/provider_fee, а не в основной amount. Значит
   даже если projection консистентна сама с собой, она расходится с declared SQL truth для payments. См. payment.sql,
   PaymentEventProjector.java.

5. Medium: payment adjustments уже есть в observed events, но полностью игнорируются ingestion-проекцией. Если считать payment.sql
   источником истины, это значит, что payment report amount/status drift против truth query уже заложен в design. Это не “мелкий TODO”, а
   настоящий semantic gap. См. 2El3kaBqBU0.txt, payment.sql.

