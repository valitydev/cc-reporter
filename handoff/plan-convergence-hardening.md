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
- Replace the current in-memory report build path with the plan-aligned large-report path:
  cursor-driven DB reads inside `READ ONLY REPEATABLE READ`, temp-file or equivalent staged output, and publication only after the
  final artifact is complete.
- Align the generated CSV contract with the business plan:
  separate date/time columns if that contract still stands, reconcile `finalized_time` naming expectations, and render monetary
  values with currency-exponent awareness instead of plain numeric serialization.
- Decide whether the planned fallback for missing `payments.trx_id` must now be implemented:
  only as a separate repair/reconciliation path for observed terminal gaps, never on the Kafka hot path.
- Add explicit negative-path Kafka batch proof if required:
  demonstrate that listener error handling preserves the intended batch ack/retry semantics, not only the happy path.

## Verified constraints
- Do not reintroduce Hellgate or any other external synchronous reads on the Kafka ingestion hot path.
- Do not remove `payments` FX fields from the contract and do not silently degrade them to permanent nullable-only behavior.
- Do not reopen the decision to build reports from CCR current-state tables.
- Preserve the already working status machine, API contract, and monotonic upsert semantics while hardening the remaining gaps.

## Next step
- Start with the large-report build path in `ReportCsvService` / worker execution, because it is the biggest remaining divergence
  from both `PLAN.md` and the raw brainstorm implementation plan.

## Done when
- Report generation no longer materializes the full query result set and full CSV payload in memory.
- CSV output and formatting converge with the accepted contract for date/time and amount representation.
- Any implemented `trx_id` fallback remains isolated to a separate repair path and is covered by tests.
- Kafka listener hardening coverage proves the intended batch ack/retry behavior if this semantics is required beyond the current
  happy-path transport test.
