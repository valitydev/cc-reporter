# EXECUTION_INPUT.md

## Task goal
- Implement `cc-reporter` as an independent Java service that:
- ingests payment and withdrawal events from Kafka into PostgreSQL current-state tables;
- exposes a Thrift API for async report creation, listing, cancel, and presigned download;
- builds CSV reports asynchronously from its own read model and stores files via `file-storage`/S3-compatible storage.

## Chosen approach
- Use three durable delivery workstreams: `kafka-to-db-ingestion`, `thrift-api-implementation`, `report-lifecycle-subsystem`.
- Keep a separate modernization track `jooq-dsl-dao-transition` for DAO-layer persistence API changes without reopening the service
  contract or report architecture.
- Keep three new follow-up tracks active:
  `dominant-name-materialization`,
  `projector-builder-refactor`.
- Build current-state projections in PostgreSQL, not on-demand reads from `magista` / `fistful-magista`.
- Kafka consumption must be batch-based, manual-ack, and commit offsets only after successful DB transaction commit.
- Use DB-level idempotency with monotonic `domain_event_id` updates on business keys.
- Official projection recovery mode is operational:
  clear CCR current-state tables and reread Kafka topics from the beginning.
- Do not implement equal-event reapplication over already-populated projection tables unless that decision is explicitly
  reopened.
- Do not copy old `reporter` 1:1; reuse only ideas, not legacy structure or Hellgate-on-hot-path behavior.
- `payments.trx_id` source: `TransactionInfo.id` from `SessionTransactionBound`; fallback, if needed, is a separate repair/reconciliation worker using full invoice reads, not ingestion path.
- `withdrawals.trx_id` source: `TransactionBoundChange.trx_info.id`.
- `withdrawals` FX fields should be derived from quote/cashflow-related events.
- `payments` FX block stays in contract and must not be downgraded to permanent `null`; first implementation may use mock values with explicit `TODO` markers until real source is confirmed.

## Done criteria
- Kafka ingestion for payments and withdrawals writes into `ccr.payment_txn_current` and `ccr.withdrawal_txn_current` with idempotent monotonic upsert semantics.
- Thrift API supports create/get/list/cancel/presigned-url for reports against `ccr.report_job` and `ccr.report_file`.
- Async lifecycle supports `pending -> processing -> created|failed|timed_out|canceled`, plus expiration flow.
- CSV generation reads only CCR current-state tables inside `READ ONLY REPEATABLE READ`, fixes `data_snapshot_fixed_at`, streams output, uploads final file, and publishes metadata only after successful upload.
- `payments` FX placeholder behavior is implemented as mock+`TODO`, not silent `null` collapse.

## Acceptance criteria
- Re-reading Kafka messages does not create duplicates and does not move entity state backward.
- `finalized_at` reflects the latest terminal status recorded by the projection.
- `CreateReport` is idempotent by `(created_by, idempotency_key)`.
- `GetReport` / `GetReports` return report state from CCR DB without external query-time joins to legacy reporting services.
- `GeneratePresignedUrl` uses configured TTL cap.
- Cancel is idempotent for allowed states.
- A successful report produces exactly one final file record and downloadable object.
- Failed generation does not leave published broken artifacts.
- Payments CSV keeps FX columns present in schema/contract even if populated by temporary mock values.

## Validation
Core checks:
- Projector tests for payment and withdrawal event-to-row mapping.
- DB tests for upsert monotonicity on `domain_event_id`.
- Tests for `trx_id` extraction from transaction-bound events.
- Tests for current `finalized_at` projection semantics.
- API tests for create/get/list/cancel idempotency.
- Worker test for `data_snapshot_fixed_at` capture and atomic file publication.

Exit checks:
- End-to-end flow: ingest events -> create report -> worker builds CSV -> file stored -> metadata readable -> presigned URL generated.
- Retry/timeout behavior verified for report lifecycle.
- CSV output includes required columns for payments and withdrawals.
- Payments FX placeholder behavior is visibly marked in code as temporary `TODO`.

Last known baseline:
- Root `cc-reporter` service is implemented across Kafka ingestion, Thrift API, report lifecycle, CSV execution,
  audit writes, and the bounded `jOOQ` DAO migration track.
- Reference modules are present in the same repo: `reporter`, `magista`, `fistful-reporter`, `fistful-magista`, `daway`.

## Constraints / non-goals
- Do not build `cc-reporter` as `reporterv2` or as a wrapper over old reporter legacy.
- Do not copy old `reporter` implementation 1:1.
- Do not put Hellgate reads on the Kafka hot path.
- Do not reopen the decision to use current-state tables as the primary reporting model.
- Do not remove payments FX columns from the contract.
- Do not replace payments FX placeholder behavior with permanent nullable-only behavior.
- Do not depend on browser-side CSV assembly or query-time `magista` / `fistful-magista` pagination.
- Internal auth is out of scope; `Wachter` remains responsible.
- If the DAO layer is modernized, preserve the current SQL-first posture and PostgreSQL semantics; do not turn `cc-reporter`
  into an ORM-first service.
- If projection rebuild semantics are reopened, keep the scope bounded to operational rebuildability; do not reopen the
  decision to
  materialize current state in CCR.

## Known risks
- Exact authoritative source for full `payments` FX block is still unresolved.
- `provider_currency` is not fully confirmed as a business-final field.
- Some provider-related enrichment may arrive later than initial entity creation events.
- Long `REPEATABLE READ` report builds can increase MVCC pressure.
- `payments.trx_id` may still require repair flow for edge cases where transaction-bound events are missing in observed ingestion streams.
- Rebuild-friendly replay semantics can accidentally weaken out-of-order protection if equal-event reapplication is introduced
  without explicit safeguards.
- Introducing local dominant enrichment adds a new CCR-owned projection pipeline that must stay consistent with report joins.
- Builder/subclass refactors over generated `jOOQ` POJOs can become harder to maintain if inheritance boundaries are not kept
  projector-local.

## Expected workstreams
- likely multi-track
- `kafka-to-db-ingestion`
- `thrift-api-implementation`
- `report-lifecycle-subsystem`
- `jooq-dsl-dao-transition`
- `dominant-name-materialization`
- `projector-builder-refactor`

## Project-specific facts
- language / stack: Java, Spring Boot, Spring Kafka, JDBC, jOOQ code generation, Flyway, PostgreSQL, Thrift, Woody,
  S3-compatible storage via `file-storage`
- package manager: Maven
- dev command: unknown
- build command: `env JAVA_HOME=/Users/karleowne/Library/Java/JavaVirtualMachines/temurin-25.0.2/Contents/Home PATH=$JAVA_HOME/bin:$PATH mvn -q -DskipTests validate`
- test command: `env JAVA_HOME=/Users/karleowne/Library/Java/JavaVirtualMachines/temurin-25.0.2/Contents/Home PATH=$JAVA_HOME/bin:$PATH mvn -q test`
- lint / typecheck command: unknown
- notable subsystems / protocols / infra: Kafka batch listeners, PostgreSQL current-state read model, `cc-reporter-proto` Thrift API, `damsel`, `fistful-proto`, `file-storage`, presigned URL flow, reference services `reporter` / `magista` / `fistful-*` / `daway`

## Missing or unclear inputs
- Exact confirmed source for `payments` FX fields beyond temporary mock placeholder behavior.
- Exact final mapping for `provider_amount/provider_currency` in withdrawals if cashflow interpretation differs from current assumptions.
- Exact retry policy values and worker concurrency sizing for production behavior.
