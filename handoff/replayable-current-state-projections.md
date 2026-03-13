# replayable-current-state-projections

## Scope
- Rework current-state ingestion semantics so Kafka event replays can deterministically rebuild the same CCR projection state,
  including newly added derived fields, instead of freezing prior rows behind duplicate-event no-op behavior.
- Keep this track bounded to replay / overwrite semantics for projection tables and their DAO / projector contracts.
- Apply the design symmetrically to payments and withdrawals where the behavior is not payment-specific.

## Thin continuity layer
- Restore surface for this track only:
  `EXECUTION_INPUT.md` -> `INVARIANTS.md` -> `PROJECT_STATE.md` -> `TEMP_PAYMENT_CURRENT_NOTES.md` ->
  `TEMP_PAYMENT_REPORT_AMOUNT_IMPLEMENTATION.md` -> this file.
- Primary truth files for decisions:
  `src/main/java/dev/vality/ccreporter/dao/PaymentCurrentDao.java`,
  `src/main/java/dev/vality/ccreporter/dao/WithdrawalCurrentDao.java`,
  `src/main/java/dev/vality/ccreporter/dao/WithdrawalSessionBindingDao.java`,
  `src/main/java/dev/vality/ccreporter/ingestion/PaymentEventProjector.java`,
  `src/main/java/dev/vality/ccreporter/ingestion/WithdrawalEventProjector.java`,
  `src/main/resources/db/migration/V1__init.sql`,
  `src/test/java/dev/vality/ccreporter/integration/IngestionSerializedEventsIntegrationTest.java`,
  `src/test/java/dev/vality/ccreporter/integration/IngestionToReportLifecycleIntegrationTest.java`.
- If continuity becomes uncertain after pause/resume/compaction, re-read the files above before changing replay semantics.

## Current state
- `payment_txn_current` and `withdrawal_txn_current` are currently updated with monotonic guards on `domain_event_id`.
- Equal or older event ids mostly become no-op updates, which is safe for duplicate delivery and out-of-order protection.
- That same design blocks projection evolution: if a new derived field is introduced later, replaying the same Kafka history over
  already-populated rows does not backfill the new field.
- `withdrawal_session_binding_current` already behaves differently: it uses `ON CONFLICT .. DO UPDATE .. WHERE older`,
  which is closer to a deterministic reapply pattern.

## Why this track exists
- CCR current-state tables are materialized projections, not immutable history tables.
- The service needs a clear rebuild story when projector logic evolves and old Kafka history is replayed from offset zero.
- The desired behavior is deterministic end state under replay, not permanent preservation of the very first projected shape.

## Design target
- Replay the same Kafka history against:
  - an empty projection database, or
  - an already-populated projection database in controlled rebuild mode,
  and reach the same final current-state rows.
- Preserve the non-regression rules:
  - older events must not move state backward;
  - `finalized_at` must remain first-terminal-write-wins;
  - current-state rows must stay deterministic under duplicate delivery.
- Separate "duplicate tolerance" from "projection rebuildability" instead of relying on no-op duplicates as the only safety story.

## Candidate implementation paths
- Preferred operational path:
  explicit rebuild mode = truncate projection tables + replay from Kafka.
- Alternative runtime path:
  allow deterministic reapplication on equal `domain_event_id` for the same business key, so newly derived fields can be filled
  during replay without requiring a truncate first.
- If the alternative path is chosen, reapplication must stay field-safe:
  `finalized_at` remains first-write-wins,
  patch fields stay monotonic,
  and replay of the same event must be deterministic rather than accidental last-writer-wins.

## Open work
- Decide and document the official rebuild contract:
  `truncate + replay` only, or `reapply on equal event id` as a supported mode.
- If `reapply on equal event id` is chosen, update DAO predicates and patch helpers for both payments and withdrawals.
- Review fields with asymmetric write rules such as `finalized_at`, `trx_id`, and search columns under equal-event reapplication.
- Add explicit replay/backfill integration coverage proving that newly added derived fields are backfilled by the chosen rebuild mode.
- Add an explicit payment replay-from-zero test:
  ingest the payment history,
  rebuild from zero / empty projection state,
  and prove that the final `payment_txn_current` row matches the same end state as before the rebuild.
- Decide whether `withdrawal_session_binding_current` needs any convergence changes so all ingestion-side projections share the same
  rebuild story.

## Constraints
- Do not reopen the decision to build reports from CCR current-state tables.
- Do not put Hellgate or other external synchronous reads on the Kafka hot path.
- Do not lose out-of-order protection while making replay semantics more rebuild-friendly.
- Keep the track cross-cutting: withdrawals must receive the same replayability treatment where the logic is generic.

## Next step
- Pick the official rebuild mode and write it down in `INVARIANTS.md` / `EXECUTION_INPUT.md` once the design is chosen.
- Then implement the minimal DAO changes and replay-focused tests before widening any field-mapping work.

## Done when
- CCR has a documented rebuild strategy for current-state projections.
- Replaying Kafka history after projector evolution can deterministically populate newly added derived fields.
- Test coverage proves that replay from zero returns payment projection data to the same final state.
- Payments and withdrawals both pass replay-oriented integration coverage that proves stable final state under the chosen mode.
