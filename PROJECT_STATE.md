# PROJECT_STATE.md

## Active tracks
- `plan-convergence-hardening`

## Completed tracks
- `kafka-to-db-ingestion`
- `thrift-api-implementation`
- `report-lifecycle-subsystem`

## Temporary cross-track exception
- `payments` FX handling remains a temporary exception: keep the contract, allow mock+`TODO`, and do not silently degrade it to permanent `null` behavior.

## Shared residual gaps
- Current core flows are implemented and green in tests, but the service is not yet fully converged with the canonical plan on
  large-report build mechanics: CSV generation still materializes query rows and the output file in memory instead of using a
  cursor-driven streaming path with temporary-file publication discipline.
- CSV contract convergence is incomplete: the current renderer still emits timestamp columns as `created_at` / `finalized_at`
  instead of splitting date/time as required by the original business plan, and amount formatting is not yet exponent-aware.
- A separate repair/reconciliation path for terminal `payments` rows that still miss `trx_id` is still only a planned fallback,
  not an implemented subsystem.
- Kafka transport happy-path wiring is covered, but explicit failure-path proof for batch ack/retry semantics remains a follow-up
  hardening item.

## Cross-track dependencies
- `kafka-to-db-ingestion` defines the read-model columns used by CSV generation and report filters.
- `kafka-to-db-ingestion` must keep `payment_txn_current` / `withdrawal_txn_current` compatible with the completed report execution and Thrift tracks.
- `plan-convergence-hardening` depends on the completed ingestion, Thrift, and lifecycle tracks without reopening their core
  architecture decisions.
