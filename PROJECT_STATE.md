# PROJECT_STATE.md

## Active tracks
- `replayable-current-state-projections`
- `dominant-name-materialization`
- `projector-builder-refactor`

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
- Current-state replay semantics are still optimized for duplicate tolerance rather than projection rebuildability; newly added
  derived fields may require a stronger replay/backfill contract than the present no-op-on-duplicate baseline.

## Cross-track dependencies
- `kafka-to-db-ingestion` defines the read-model columns used by CSV generation and report filters.
- `kafka-to-db-ingestion` must keep `payment_txn_current` / `withdrawal_txn_current` compatible with the completed report execution and Thrift tracks.
- `replayable-current-state-projections` builds on top of the completed ingestion track without reopening the decision to keep
  PostgreSQL current-state tables as the reporting truth.
- `dominant-name-materialization` depends on the completed ingestion/report tracks and supplies a local enrichment source for
  display-name columns still left unresolved by event-native projectors.
- `projector-builder-refactor` depends on the completed ingestion track and should preserve both the completed report path and the
  active replay/name-enrichment follow-up tracks while improving projector readability.
- `plan-convergence-hardening` closed on top of the completed ingestion, Thrift, and lifecycle tracks without reopening their core
  architecture decisions.
- `csv-cursor-hardening` closed as a narrow follow-up on the completed report execution path without reopening the CSV contract, API
  semantics, ingestion mappings, or lifecycle state machine.
- `audit-observability-hardening` closed with runtime writes to `report_audit_event` for report create/cancel/presigned-url actions,
  consuming trusted forwarded identity/tracing headers without adding local JWT parsing.
- `jooq-dsl-dao-transition` must preserve the completed ingestion, Thrift, lifecycle, CSV, and audit tracks while changing only the
  persistence API used inside DAO implementations.

## Active track snapshot
- `replayable-current-state-projections`
  is a follow-up design/implementation track for deterministic current-state rebuilds under Kafka replay and projector evolution.
- `dominant-name-materialization`
  is a follow-up design/implementation track for CCR-owned local lookup tables that restore `shop_name` / `provider_name` /
  `terminal_name` and similar display-name fields without foreign DB joins.
- `projector-builder-refactor`
  is a follow-up design/implementation track for Lombok/builder-based projector assembly and decomposition of large current-state
  projectors, starting with payments and extending generic improvements to withdrawals.

## Latest completed track snapshot
- `jooq-dsl-dao-transition` is complete. Runtime DAO code now uses a Spring-managed `DSLContext` plus generated `jOOQ` schema
  models in `WithdrawalSessionBindingDao`, `ReportAuditDao`, `ReportLifecycleDao`, `PaymentCurrentDao`,
  `WithdrawalCurrentDao`, and `ReportDao`.
- The transition stayed SQL-first and PostgreSQL-specific: current DTO/service boundaries were preserved, and the service was not
  widened into `Hibernate` / JPA.
- Full regression verification passed on Java 25; the residual JaCoCo missing-report message remains non-fatal when Maven exits `0`.
