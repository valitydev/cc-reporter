# thrift-api-implementation

## Scope
- Implement the CCR Thrift boundary and persistence-facing service layer for report creation, listing, cancel, and presigned downloads.

## Open work
- Align caller identity extraction with the real Wachter/gateway propagation contract if it differs from the current explicit header-based bridge.
- Replace the current test stubbed `file-storage` download path with end-to-end integration once the environment for external thrift clients is available in this service.

## Verified constraints
- API contract now comes from Maven artifact `dev.vality:cc-reporter-proto:1.3-e7841ec`, not local `refs/cc-reporter-proto` generation.
- The Thrift boundary should validate, map errors, and delegate; report building does not belong in the handler layer.
- `file-storage` / presigned URL approach remains unchanged.
- `CreateReport` is implemented as idempotent by `(created_by, idempotency_key)` against `ccr.report_job`.
- `GetReport`, `GetReports`, `CancelReport`, and `GeneratePresignedUrl` are scoped by caller identity and read only CCR storage.

## Next step
- Keep this track focused on contract hardening and external integration polish against the pinned Maven contract; main code path for CRUD/list/cancel/presigned is implemented and covered by tests.

## Done when
- `CreateReport`, `GetReport`, `GetReports`, `CancelReport`, and `GeneratePresignedUrl` are implemented against CCR storage and covered by API-level tests.
