# plan-convergence-hardening

## Scope
- Close the remaining gaps between the implemented service and the canonical plan in `PLAN.md`, without reopening the core
  architecture that is already in place.
- Keep this track bounded to cross-track hardening items that affect report correctness at scale, CSV contract convergence, or
  explicit proof of planned runtime semantics.

## Current state
- The three original delivery tracks are functionally implemented: Kafka ingestion populates current-state tables, the Thrift API
  supports create/get/list/cancel/presigned flows, and the report lifecycle supports claim/process/retry/timeout/expire/cancel.
- Integration coverage already includes end-to-end ingestion -> create report -> worker build -> file metadata -> presigned URL,
  Kafka producer-to-listener wiring, and multi-worker concurrency on report claiming/processing.
- The remaining drift against `brainstorm.md:498+` and `PLAN.md` is concentrated in a small set of hardening items, not in the
  main architecture.

## Open work
- None for the bounded track scope.

## Verified constraints
- Do not reintroduce Hellgate or any other external synchronous reads on the Kafka ingestion hot path.
- Do not remove `payments` FX fields from the contract and do not silently degrade them to permanent nullable-only behavior.
- Do not reopen the decision to build reports from CCR current-state tables.
- Preserve the already working status machine, API contract, and monotonic upsert semantics while hardening the remaining gaps.

## Next step
- Keep this track in maintenance mode only. Reopen it only if real data disproves the current `trx_id` assumptions or if the CSV
  contract changes again.

## Done when
- Done at the current baseline.

## Progress update
- `ReportCsvService` now renders inside `READ ONLY REPEATABLE READ` directly into a staged temp file while streaming JDBC rows
  through the writer instead of materializing the full result set and CSV payload in memory.
- `ReportLifecycleService` now uploads from the staged file, publishes file metadata from the staged artifact, and removes the
  temporary file after processing.
- CSV output now matches the accepted contract in `docs/CSV_REPORT_FORMAT.md`: split `created_*` / `finalized_*` date-time
  columns, fixed column order, and exponent-aware formatting for amount fields.
- Kafka transport hardening now has explicit negative-path proof: a synthetic first-attempt listener failure is retried and the
  batch is committed only after a successful reprocessing pass.
- The `payments.trx_id` fallback item is closed by decision: current primary truth shows `SessionTransactionBound` is sufficient,
  so no separate reconciler is implemented unless production streams show terminal rows that still miss `trx_id`.
- Scoped verification passed with Java 25 via
  `mvn -q -Dtest=ReportExecutionIntegrationTest,ReportQueryFilteringIntegrationTest,IngestionToReportLifecycleIntegrationTest,`
  `KafkaListenerIntegrationTest,KafkaListenerRetryIntegrationTest,ReportLifecycleWorkerIntegrationTest test`.
