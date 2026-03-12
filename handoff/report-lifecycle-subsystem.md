# report-lifecycle-subsystem

## Scope
- Implement async report execution: claim, process, retry, timeout, cancel visibility, expiration, and atomic file publication.

## Open work
- Implement lifecycle `pending -> processing -> created|failed|timed_out|canceled`, with later expiration handling.
- Implement CSV build inside `READ ONLY REPEATABLE READ` with fixed `data_snapshot_fixed_at`, streamed output, and atomic file publication.

## Verified constraints
- Worker status transitions must stay consistent with `report_job` / `report_file` semantics used by the Thrift API.
- Upload must complete before file metadata is published.
- CSV build reads only CCR current-state tables.

## Next step
- Implement atomic claim/transition operations and worker skeleton first, then add CSV build/upload flow and timeout/retry handlers.

## Done when
- End-to-end report execution succeeds with correct status transitions, retry/timeout behavior is covered by tests, and broken temporary artifacts are never published.
