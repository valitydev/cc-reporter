# kafka-to-db-ingestion

## Scope
- Implement Kafka listeners, projectors, and DAOs that populate `ccr.payment_txn_current` and `ccr.withdrawal_txn_current`.

## Open work
- Map payment current-state from `started`, `route_changed`, `cash_changed`, `status_changed`, `session_transaction_bound`, and `cash_flow_changed` style events.
- Map withdrawal current-state from `created`, `route_changed`, `status_changed`, withdrawal-session `transaction_bound`, quote data, and transfer cashflow events.

## Verified constraints
- `payments.trx_id` should come from `SessionTransactionBound.trx.id`; any repair path is secondary and off the Kafka hot path.
- `withdrawals.trx_id` should come from `TransactionBoundChange.trx_info.id`.
- Ingestion must remain idempotent, monotonic by `domain_event_id`, and independent from Hellgate on the hot path.

## Next step
- Implement listener configuration and projector/DAO skeletons first, then codify event-to-column mapping with tests before filling every field.

## Done when
- Both current-state tables are populated by idempotent monotonic upserts, `finalized_at` first-write-wins is covered by tests, and ingestion does not depend on Hellgate on the hot path.
