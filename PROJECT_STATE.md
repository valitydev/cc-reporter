# PROJECT_STATE.md

## Active tracks
- `audit-observability-hardening`

## Completed tracks
- `kafka-to-db-ingestion`
- `thrift-api-implementation`
- `report-lifecycle-subsystem`
- `plan-convergence-hardening`
- `csv-cursor-hardening`

## Temporary cross-track exception
- `payments` FX handling remains a temporary exception: keep the contract, allow mock+`TODO`, and do not silently degrade it to permanent `null` behavior.

## Shared residual gaps
- `report_audit_event` already exists in the main DDL, but no runtime path writes audit records yet. The intended trust boundary is
  `Wachter`-normalized identity/tracing headers, not local JWT parsing inside `cc-reporter`.
- `shop_name`, `wallet_name`, `provider_name`, and `terminal_name` remain only partially converged current-state fields: the schema,
  DAO merge logic, and search normalization support them, but ingestion still lacks confirmed event-native sources for reliably
  populating those display-name columns and currently leaves explicit `TODO` markers at the projector gap points.
- `payments` `trx_id` remains event-first from `SessionTransactionBound`; a separate repair/reconciliation worker is not required by
  the current primary truth and should only be revived if real ingestion streams demonstrate terminal rows that still miss `trx_id`.

## Cross-track dependencies
- `kafka-to-db-ingestion` defines the read-model columns used by CSV generation and report filters.
- `kafka-to-db-ingestion` must keep `payment_txn_current` / `withdrawal_txn_current` compatible with the completed report execution and Thrift tracks.
- `plan-convergence-hardening` closed on top of the completed ingestion, Thrift, and lifecycle tracks without reopening their core
  architecture decisions.
- `csv-cursor-hardening` closed as a narrow follow-up on the completed report execution path without reopening the CSV contract, API
  semantics, ingestion mappings, or lifecycle state machine.
- `audit-observability-hardening` is a bounded follow-up on API/report metadata handling and must not reopen the CSV contract,
  ingestion mappings, or report lifecycle state machine beyond adding explicit audit writes for already-supported user actions.
