# INVARIANTS.md

- Reports are built from CCR current-state PostgreSQL tables, not via query-time calls to `magista` or `fistful-magista`.
- Kafka ingestion is batch-based, manual-ack, and must commit offsets only after successful DB transaction commit.
- Current-state updates are monotonic by business key plus `domain_event_id`; older or duplicate events must not move state backward.
- `finalized_at` is fixed on the first terminal status and must not be overwritten later.
- Do not put Hellgate reads on the ingestion hot path.
