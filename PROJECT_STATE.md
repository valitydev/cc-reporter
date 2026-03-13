# PROJECT_STATE.md

## Active tracks
- None.

## Completed tracks
- `kafka-to-db-ingestion`
- `thrift-api-implementation`
- `report-lifecycle-subsystem`
- `plan-convergence-hardening`
- `csv-cursor-hardening`
- `audit-observability-hardening`
- `jooq-dsl-dao-transition`

## Temporary cross-track exception
- `payments` FX handling remains a temporary exception: keep the contract, allow mock+`TODO`, and do not silently degrade it to permanent `null` behavior.

## Shared residual gaps
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
- `audit-observability-hardening` closed with runtime writes to `report_audit_event` for report create/cancel/presigned-url actions,
  consuming trusted forwarded identity/tracing headers without adding local JWT parsing.
- `jooq-dsl-dao-transition` must preserve the completed ingestion, Thrift, lifecycle, CSV, and audit tracks while changing only the
  persistence API used inside DAO implementations.

## Active track snapshot
- No active track is open right now.

## Latest completed track snapshot
- `jooq-dsl-dao-transition` is complete. Runtime DAO code now uses a Spring-managed `DSLContext` plus generated `jOOQ` schema
  models in `WithdrawalSessionBindingDao`, `ReportAuditDao`, `ReportLifecycleDao`, `PaymentCurrentDao`,
  `WithdrawalCurrentDao`, and `ReportDao`.
- The transition stayed SQL-first and PostgreSQL-specific: current DTO/service boundaries were preserved, and the service was not
  widened into `Hibernate` / JPA.
- Full regression verification passed on Java 25; the residual JaCoCo missing-report message remains non-fatal when Maven exits `0`.
