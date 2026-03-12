# csv-cursor-hardening

## Scope
- Close the last thin drift against `brainstorm.md:498+` and `PLAN.md`: make the CSV generation path explicitly cursor/fetch-size
  aware for long-running scans, not only staged-file streaming.
- Keep this track tightly bounded to report-read execution in `ReportCsvService` and its verification.

## Thin continuity layer
- Restore surface for this track only:
  `EXECUTION_INPUT.md` -> `INVARIANTS.md` -> `PROJECT_STATE.md` -> this file.
- Primary truth files for decisions:
  `src/main/java/dev/vality/ccreporter/report/ReportCsvService.java`,
  `src/main/java/dev/vality/ccreporter/report/ReportLifecycleService.java`,
  `src/test/java/dev/vality/ccreporter/integration/ReportExecutionIntegrationTest.java`,
  `src/test/java/dev/vality/ccreporter/integration/IngestionToReportLifecycleIntegrationTest.java`,
  `src/test/java/dev/vality/ccreporter/integration/ReportQueryFilteringIntegrationTest.java`,
  `PLAN.md`, `docs/CSV_REPORT_FORMAT.md`.
- If continuity becomes uncertain after pause/resume/compaction, re-read the files above before changing scope or implementation.

## Current state
- CSV generation already runs inside `READ ONLY REPEATABLE READ`, streams rows directly into a staged temp file, hashes the final
  artifact, uploads only the completed file, and publishes metadata after successful upload.
- CSV contract convergence is already done: split date/time columns, fixed column order, and exponent-aware amount formatting are
  implemented and covered by integration tests.
- Kafka ack/retry negative-path proof is already done and is out of scope for this track.
- `ReportCsvService` now executes report scans through an explicit forward-only, read-only prepared statement with fetch size `1000`
  after named-parameter expansion, so the cursor/fetch-size behavior is declared in code rather than left to driver defaults.
- Verification now includes a focused `ReportCsvServiceTest` that asserts the cursor-oriented JDBC path and preserves CSV output,
  alongside the existing report execution/filtering integration scenarios.

## Remaining drift
- None within this track's bounded scope.

## Constraints
- Do not reopen the CSV column contract, payments FX placeholder policy, ingestion mappings, API behavior, or lifecycle status
  machine.
- Do not reintroduce in-memory CSV assembly.
- Do not add external reads on the ingestion or report-generation hot paths.

## Next step
- None. Reopen only if a regression shows the cursor-oriented path is bypassed or incompatible with real PostgreSQL scans.

## Done when
- Done: the CSV read path makes cursor/fetch-size behavior explicit in code rather than relying on default driver behavior.
- Done: verification covers that the cursor-oriented path still preserves existing CSV content and lifecycle semantics.
- Done: no previously closed convergence item was reopened.
