# replayable-current-state-projections

## Scope

- Confirm the official rebuild contract for CCR current-state projections when projector logic or derived fields change.
- Keep the track bounded to projection rebuild strategy; do not reopen report architecture or Kafka ingestion shape.

## Decision

- CCR current-state tables are materialized current projections, not immutable history.
- The official and sufficient rebuild mode is:
  clear projection tables, then reread Kafka topics from the beginning.
- CCR does not support or require reapplying equal events over an already-populated projection database as part of the
  official
  rebuild story.
- Reports continue to be built from CCR current-state tables after that rebuild.

## Primary truth

- `src/main/java/dev/vality/ccreporter/dao/PaymentTxnCurrentDao.java`
- `src/main/java/dev/vality/ccreporter/dao/WithdrawalTxnCurrentDao.java`
- `src/main/java/dev/vality/ccreporter/dao/WithdrawalSessionDao.java`
- `src/main/java/dev/vality/ccreporter/ingestion/payment/PaymentEventProjector.java`
- `src/main/java/dev/vality/ccreporter/ingestion/withdrawal/WithdrawalEventProjector.java`
- `src/main/java/dev/vality/ccreporter/ingestion/withdrawal/WithdrawalSessionEventProjector.java`
- `src/main/resources/db/migration/V1__init.sql`

## Why this is correct

- Current-state rows are derived read-model data, so rebuilding them from the original Kafka history is the correct
  recovery path.
- Existing monotonic upserts already protect against older events moving state backward during normal ingestion.
- Starting from empty projection tables gives deterministic final state without introducing more complex equal-event
  overwrite rules.
- This is enough to backfill newly added derived fields after schema or projector changes.

## Explicit non-goals

- Do not implement equal-event reapplication over already-filled projection tables.
- Do not weaken out-of-order protection in order to make duplicate events mutate existing rows.
- Do not move report building away from CCR current-state tables.

## Operational contract

- If projection schema or projector logic changes in a way that affects stored current-state fields:
  apply migrations,
  clear `ccr.payment_txn_current`, `ccr.withdrawal_txn_current`, and `ccr.withdrawal_session`,
  reread the relevant Kafka topics from the beginning,
  then build reports from the rebuilt tables.

## Status

- Done as a design decision.
- No additional runtime DAO work is required for this track under the chosen contract.
