# jooq-dsl-dao-transition

## Scope
- Complete a bounded DAO-layer transition from `NamedParameterJdbcTemplate` to `jOOQ DSL` using generated schema models.
- Keep the track bounded to persistence API changes inside DAO implementations.
- Do not reopen the service contract, Kafka ingestion architecture, report lifecycle semantics, or move the service to
  `Hibernate` / JPA.

## Thin continuity layer
- Restore surface for this track only:
  `EXECUTION_INPUT.md` -> `INVARIANTS.md` -> `PROJECT_STATE.md` -> this file.
- Primary truth files for decisions:
  `pom.xml`,
  `src/main/java/dev/vality/ccreporter/config/JooqConfig.java`,
  `src/main/java/dev/vality/ccreporter/dao/ReportDao.java`,
  `src/main/java/dev/vality/ccreporter/dao/ReportLifecycleDao.java`,
  `src/main/java/dev/vality/ccreporter/dao/PaymentCurrentDao.java`,
  `src/main/java/dev/vality/ccreporter/dao/WithdrawalCurrentDao.java`,
  `src/main/java/dev/vality/ccreporter/dao/WithdrawalSessionBindingDao.java`,
  `src/main/java/dev/vality/ccreporter/dao/ReportAuditDao.java`,
  `src/main/resources/db/migration/V1__init.sql`,
  `src/main/resources/db/migration/V2__withdrawal_session_binding.sql`.
- If continuity becomes uncertain after pause/resume/compaction, re-read the files above before changing scope or implementation.

## Current state
- Runtime wiring now exposes a Spring-managed `DSLContext` backed by the existing `DataSource` via
  `TransactionAwareDataSourceProxy`, so DAO migrations stay inside current transaction boundaries.
- `WithdrawalSessionBindingDao`, `ReportAuditDao`, `ReportLifecycleDao`, `PaymentCurrentDao`,
  `WithdrawalCurrentDao`, and `ReportDao` now use `jOOQ` generated table models plus `DSLContext`.
- The DAO layer is SQL-first and PostgreSQL-specific: it relies on `ON CONFLICT`, `FOR UPDATE SKIP LOCKED`, enum casts,
  `jsonb`, and explicit partial-update semantics with `COALESCE`.
- Regression fixes discovered during migration are already folded into primary truth:
  `ReportLifecycleDao` writes `Instant` values through `Timestamp` bindings to preserve UTC semantics,
  and `ReportDao` translates duplicate-key inserts back to Spring `DuplicateKeyException` while reading timestamps
  through explicit `Timestamp` projections to avoid timezone drift.

## Why this track exists
- The current `Spring JDBC` approach keeps SQL explicit, but schema drift remains easy to miss when migrations evolve.
- `jOOQ` is a better fit than `Hibernate` for this codebase because the service is already SQL-first and not entity-lifecycle-first.
- The intended value is compile-time coupling to the generated schema model and a gradual move from string SQL assembly to typed DSL,
  not an ORM rewrite.

## Transition shape
- Keep existing domain DTOs and records such as `StoredReport`, `StoredFileData`, `ClaimedReportJob`,
  `PaymentCurrentUpdate`, and `WithdrawalCurrentUpdate`.
- Introduce `DSLContext` as the DAO-facing persistence API.
- Do not require a pure-DSL rewrite of every complex query in the first pass; for especially dense PostgreSQL queries, a temporary
  hybrid inside `jOOQ` is allowed if it preserves readability and semantics.
- Do not mix `JdbcTemplate` and `jOOQ` inside the same DAO method.

## Migration order taken
- Infrastructure first:
  `JooqConfig` wires `DSLContext` to the existing Spring-managed transactional `DataSource`.
- Simple DAO conversions:
  `WithdrawalSessionBindingDao`, `ReportAuditDao`.
- Lifecycle DAO:
  `ReportLifecycleDao`.
- Current-state upsert DAOs:
  `PaymentCurrentDao`, `WithdrawalCurrentDao`.
- Complex read/write DAO last:
  `ReportDao`.

## Constraints
- Preserve current SQL semantics and PostgreSQL behavior during migration.
- Preserve existing service-layer DTO boundaries; do not promote generated `jOOQ` POJOs or `UpdatableRecord` types into business
  code by default.
- Do not turn this track into a `Hibernate` / JPA adoption track.
- Do not reopen completed ingestion, Thrift, lifecycle, CSV, or audit decisions while migrating DAO internals.

## Next step
- No further track-local implementation work is planned.
- If a future change touches DAO persistence, use the migrated `DSLContext` DAOs as the baseline and preserve the verified
  PostgreSQL semantics already covered by the full integration suite.

## Done when
- DAO implementations use `DSLContext` instead of `NamedParameterJdbcTemplate` where the transition is complete.
- Generated `jOOQ` schema models are used by runtime persistence code, not only by build-time code generation.
- Existing SQL semantics, transaction behavior, and public service behavior remain unchanged.

## Exit status
- Track completion criteria are satisfied for the bounded DAO migration scope.
- Full regression verification passed with Java 25 via `mvn -q test`.
- The remaining `Unable to find jacoco report file in ./target/site/jacoco/index.html` message is a known non-fatal
  warning when the Maven exit code is zero.
