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
- `ReportManagementService` now resolves bounded request audit metadata and writes `report_audit_event` rows for successful
  `CreateReport`, `CancelReport`, and `GeneratePresignedUrl` calls through `ReportAuditDao`.
- `cc-reporter` still resolves `X-User-Id` for business ownership via `CurrentPrincipalResolver`, and now also reads trusted
  forwarded Woody identity/request headers plus `traceparent` / `tracestate` for audit payloads.
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
- `ReportAuditIntegrationTest` now covers audit writes for create/cancel/presigned-url, and the targeted Java 25 Maven validation
  passes for the audit + related API/lifecycle scenarios.

## Remaining drift
- No open drift remains inside this track.

## Constraints
- Do not add local JWT parsing to `cc-reporter`; trust `Wachter` as the auth boundary and consume forwarded headers only.
- Do not claim to audit final file downloads unless the request still passes through `cc-reporter`; for the current presigned URL path,
  audit only `presigned_url_generated` / download intent.
- Do not reopen CSV schema, Kafka ingestion architecture, or report lifecycle semantics outside the minimum audit hooks needed for
  already-supported user actions.

## Next step
- None. This track is complete unless broader reviewer feedback asks for different audit payload fields.

## Done when
- `report_audit_event` is written by runtime code for the chosen report actions, not just defined in SQL.
- Audit payload captures the trusted forwarded identity/tracing fields needed for incident review.
- Tests prove that audit records are written with the expected event type, actor, and key metadata.
- No broader analytics/checkpoint subsystem is reintroduced.
