# jooq-dsl-dao-transition

## Scope
- Plan and stage a DAO-layer transition from `NamedParameterJdbcTemplate` to `jOOQ DSL` using generated schema models.
- Keep the track bounded to persistence API changes inside DAO implementations.
- Do not reopen the service contract, Kafka ingestion architecture, report lifecycle semantics, or move the service to
  `Hibernate` / JPA.

## Thin continuity layer
- Restore surface for this track only:
  `EXECUTION_INPUT.md` -> `INVARIANTS.md` -> `PROJECT_STATE.md` -> this file.
- Primary truth files for decisions:
  `pom.xml`,
  `src/main/java/dev/vality/ccreporter/dao/ReportDao.java`,
  `src/main/java/dev/vality/ccreporter/dao/ReportLifecycleDao.java`,
  `src/main/java/dev/vality/ccreporter/dao/PaymentCurrentDao.java`,
  `src/main/java/dev/vality/ccreporter/dao/WithdrawalCurrentDao.java`,
  `src/main/java/dev/vality/ccreporter/dao/WithdrawalSessionBindingDao.java`,
  `src/main/java/dev/vality/ccreporter/dao/ReportAuditDao.java`,
  `src/main/resources/db/migration/V1__init.sql`.
- If continuity becomes uncertain after pause/resume/compaction, re-read the files above before changing scope or implementation.

## Current state
- Runtime DAO code is fully `NamedParameterJdbcTemplate`-based.
- Runtime code does not currently use `org.jooq.*`, generated `dev.vality.ccreporter.domain.*`, JPA, or `Hibernate`.
- `jOOQ` is present only as build-time code generation and does not currently shape runtime persistence code.
- The DAO layer is SQL-first and PostgreSQL-specific: it relies on `ON CONFLICT`, `FOR UPDATE SKIP LOCKED`, enum casts,
  `jsonb`, `PGobject`, and explicit partial-update semantics with `COALESCE`.

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

## Planned migration order
- Infrastructure first:
  add `DSLContext` configuration wired to the existing Spring `DataSource` and transaction boundaries.
- Simple DAO conversions first:
  `WithdrawalSessionBindingDao`, `ReportAuditDao`.
- Lifecycle DAO next:
  `ReportLifecycleDao`.
- Current-state upsert DAO after that:
  `PaymentCurrentDao`, `WithdrawalCurrentDao`.
- Most complex read DAO last:
  `ReportDao`.

## Constraints
- Preserve current SQL semantics and PostgreSQL behavior during migration.
- Preserve existing service-layer DTO boundaries; do not promote generated `jOOQ` POJOs or `UpdatableRecord` types into business
  code by default.
- Do not turn this track into a `Hibernate` / JPA adoption track.
- Do not reopen completed ingestion, Thrift, lifecycle, CSV, or audit decisions while migrating DAO internals.

## Next step
- Decide whether the first implementation step is schema-model adoption only or immediate DAO migration to `DSLContext`.
- If the decision is to start migration now, use `WithdrawalSessionBindingDao` as the style-setting first DAO.

## Done when
- DAO implementations use `DSLContext` instead of `NamedParameterJdbcTemplate` where the transition is complete.
- Generated `jOOQ` schema models are used by runtime persistence code, not only by build-time code generation.
- Existing SQL semantics, transaction behavior, and public service behavior remain unchanged.
