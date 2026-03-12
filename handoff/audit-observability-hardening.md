# audit-observability-hardening

## Scope
- Close the audit gap around report user actions without widening into generic analytics or storage-side download telemetry.
- Keep this track bounded to request metadata extraction, `report_audit_event` writes, and verification for already-supported API
  actions in `cc-reporter`.

## Thin continuity layer
- Restore surface for this track only:
  `EXECUTION_INPUT.md` -> `INVARIANTS.md` -> `PROJECT_STATE.md` -> this file.
- Primary truth files for decisions:
  `src/main/resources/db/migration/V1__init.sql`,
  `refs/cc-reporter-proto/proto/ccreports.thrift`,
  `src/main/java/dev/vality/ccreporter/report/ReportManagementService.java`,
  `src/main/java/dev/vality/ccreporter/security/CurrentPrincipalResolver.java`,
  `refs/wachter/src/main/java/dev/vality/wachter/service/WachterService.java`,
  `refs/wachter/README.md`,
  `refs/wachter/wachter_context.md`,
  `refs/woody-http-bridge/src/main/java/dev/vality/woody/http/bridge/tracing/TraceContextHeadersNormalizer.java`,
  `refs/woody-http-bridge/src/main/java/dev/vality/woody/http/bridge/tracing/TraceHeadersConstants.java`.
- If continuity becomes uncertain after pause/resume/compaction, re-read the files above before changing scope or implementation.

## Current state
- The main DDL already defines `ccr.report_audit_event`, but the runtime code has no DAO/service path that writes into it.
- `cc-reporter` currently resolves only `X-User-Id` for business ownership via `CurrentPrincipalResolver`.
- `Wachter` and `woody-http-bridge` already normalize and forward richer identity/tracing headers downstream:
  `woody.meta.user-identity.id`,
  `woody.meta.user-identity.username`,
  `woody.meta.user-identity.email`,
  `woody.meta.user-identity.realm`,
  `woody.meta.user-identity.X-Request-ID`,
  `woody.meta.user-identity.X-Request-Deadline`,
  `traceparent`,
  `tracestate`.
- Because files are downloaded via presigned URL from storage, `cc-reporter` can truthfully audit URL generation or download intent,
  but not the final object fetch itself.

## Remaining drift
- `report_audit_event` is a primary-truth model that is currently provisioned but unused.
- There is no dedicated request-metadata resolver for enriched audit identity/tracing context.
- There are no tests proving audit writes for `CreateReport`, `CancelReport`, or `GeneratePresignedUrl`.

## Constraints
- Do not add local JWT parsing to `cc-reporter`; trust `Wachter` as the auth boundary and consume forwarded headers only.
- Do not claim to audit final file downloads unless the request still passes through `cc-reporter`; for the current presigned URL path,
  audit only `presigned_url_generated` / download intent.
- Do not reopen CSV schema, Kafka ingestion architecture, or report lifecycle semantics outside the minimum audit hooks needed for
  already-supported user actions.

## Next step
- Add a bounded audit subsystem:
  request metadata resolver -> `ReportAuditDao` -> writes on successful `CreateReport`, `CancelReport`, and
  `GeneratePresignedUrl`, with payload fields that preserve actor, request identity, and trace context.

## Done when
- `report_audit_event` is written by runtime code for the chosen report actions, not just defined in SQL.
- Audit payload captures the trusted forwarded identity/tracing fields needed for incident review.
- Tests prove that audit records are written with the expected event type, actor, and key metadata.
- No broader analytics/checkpoint subsystem is reintroduced.
