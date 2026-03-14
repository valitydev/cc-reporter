# PROJECT_STATE.md

## Active tracks
- `replayable-current-state-projections`

## Completed tracks
- `kafka-to-db-ingestion`
- `thrift-api-implementation`
- `report-lifecycle-subsystem`
- `plan-convergence-hardening`
- `csv-cursor-hardening`
- `audit-observability-hardening`
- `jooq-dsl-dao-transition`
- `dominant-name-materialization`
- `projector-builder-refactor`

## Temporary cross-track exception
- `payments` FX handling remains a temporary exception: keep the contract, allow mock+`TODO`, and do not silently degrade it to permanent `null` behavior.

## Shared residual gaps
- Display-name enrichment is now CCR-owned through local dominant lookups. Current-state payment/withdrawal rows keep stable ids plus
  transaction-native search only; dominant-backed name and name-search data live exclusively in CCR lookup tables.
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

## Latest completed track snapshot
- `projector-builder-refactor` is complete. Payment, withdrawal, and withdrawal-session current-state projector assembly now uses
  local fluent builders plus small event-branch helpers instead of the old monolithic positional/update-method style, without
  widening dependencies to Lombok or changing ingestion semantics.
- `dominant-name-materialization` is complete. CCR now owns local `shop` / `provider` / `terminal` / `wallet` lookup tables,
  resolves report name filters through local joins, and ingests dominant `HistoricalCommit` batches into version-aware,
  tombstone-aware lookup state without depending on `daway` DB at runtime.
- `jooq-dsl-dao-transition` is complete. Runtime DAO code now uses a Spring-managed `DSLContext` plus generated `jOOQ` schema
  models in `WithdrawalSessionBindingDao`, `ReportAuditDao`, `ReportLifecycleDao`, `PaymentCurrentDao`,
  `WithdrawalCurrentDao`, and `ReportDao`.
- The transition stayed SQL-first and PostgreSQL-specific: current DTO/service boundaries were preserved, and the service was not
  widened into `Hibernate` / JPA.
- Full regression verification passed on Java 25; the residual JaCoCo missing-report message remains non-fatal when Maven exits `0`.
