# kafka-to-db-ingestion

## Scope
- Implement Kafka listeners, projectors, and DAOs that populate `ccr.payment_txn_current` and `ccr.withdrawal_txn_current`.
- Keep auxiliary ingestion-only state allowed when needed to preserve current-table correctness; at the moment this includes
  `ccr.withdrawal_session_binding_current` for resolving withdrawal-session transaction-bound updates back to
  `withdrawal_id`.

## Open work
- None inside this track. Provider / wallet human-readable names remain unset because no local hot-path source was introduced
  for them, and the current schema / report flow does not require inventing a fallback.

## Verified constraints
- `payments.trx_id` should come from `SessionTransactionBound.trx.id`; any repair path is secondary and off the Kafka hot path.
- `withdrawals.trx_id` should come from `TransactionBoundChange.trx_info.id`.
- Ingestion remains independent from Hellgate on the hot path.
- `payments` / `withdrawals` main current-state updates are monotonic by `domain_event_id`.
- `withdrawal-session` transaction-bound updates are applied as an idempotent patch by `withdrawal_id`, because session and
  withdrawal topics do not share a comparable event-id sequence.

## Next step
- No further work in this track.

## Done when
- Both current-state tables are populated by idempotent monotonic upserts, `finalized_at` first-write-wins is covered by tests,
  ingestion does not depend on Hellgate on the hot path, the mapped fields are backed by
  projector-level serialized-event coverage plus end-to-end report lifecycle coverage from ingestion to presigned URL,
  and Kafka transport wiring is verified with a producer-to-listener integration test against an embedded broker.
