# INVARIANTS.md

- Reports are built from CCR current-state PostgreSQL tables, not via query-time calls to `magista` or `fistful-magista`.
- Kafka ingestion is batch-based, manual-ack, and must commit offsets only after successful DB transaction commit.
- Current-state updates are monotonic by business key plus `domain_event_id`; older or duplicate events must not move state backward.
- `finalized_at` reflects the latest terminal state seen for the entity under the current projection semantics.
- Do not put Hellgate reads on the ingestion hot path.
- Projection rebuild contract is operational and explicit:
  clear current-state projection tables and reread Kafka topics from the beginning.
- CCR does not support equal-event reapplication over already-populated current-state tables as an official rebuild
  mode.
- If DAO methods move from `NamedParameterJdbcTemplate` to `jOOQ`, preserve the existing SQL semantics, PostgreSQL-specific
  behavior, transaction boundaries, and domain DTO boundaries.
