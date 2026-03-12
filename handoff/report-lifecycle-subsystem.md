# report-lifecycle-subsystem

## Scope
- Implement async report execution: claim, process, retry, timeout, cancel visibility, expiration, and atomic file publication.

## Current state
- API-visible lifecycle state is already wired through `report_job` / `report_file`: `pending`, `processing`, `created`, `failed`, `canceled`, `timed_out`, `expired`, plus `started_at`, `data_snapshot_fixed_at`, `finished_at`, `expires_at`, and file metadata reads.
- `cancelReport` is implemented and remains terminal/idempotent for `pending|processing`.
- Worker-side lifecycle is implemented end-to-end: claim next due `pending` job with `FOR UPDATE SKIP LOCKED`, build CSV from CCR current-state tables inside `READ ONLY REPEATABLE READ`, upload to `file-storage`, publish `report_file`, mark `created`, reschedule retry, mark `failed`, mark `timed_out`, and expire `created` reports.
- Scenario-oriented integration coverage exercises successful payments/withdrawals execution, retry-to-failed behavior, first-terminal-wins behavior for `finished_at`, timeout handling, expiration, and API-visible lifecycle reads.

## Open work
- None for the bounded track scope.

## Verified constraints
- Worker status transitions must stay consistent with `report_job` / `report_file` semantics used by the Thrift API.
- Upload must complete before file metadata is published.
- CSV build reads only CCR current-state tables.
- `finished_at` must remain first-write-wins across terminal transitions.

## Next step
- Keep this track in maintenance mode while `kafka-to-db-ingestion` catches up with real data; lifecycle/report execution path itself is implemented for the current CCR schema.

## Done when
- Done: end-to-end report execution succeeds through real worker code with correct status transitions, retry/timeout behavior is covered by tests, and broken temporary artifacts are not published via CCR metadata.
