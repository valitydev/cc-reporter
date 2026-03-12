# thrift-api-implementation

## Scope
- Implement the CCR Thrift boundary and persistence-facing service layer for report creation, listing, cancel, and presigned downloads.

## Open work
- None for the bounded track scope.

## Verified constraints
- API contract now comes from Maven artifact `dev.vality:cc-reporter-proto:1.3-e7841ec`, not local `refs/cc-reporter-proto` generation.
- The Thrift boundary should validate, map errors, and delegate; report building does not belong in the handler layer.
- `file-storage` / presigned URL approach remains unchanged.
- `CreateReport` is implemented as idempotent by `(created_by, idempotency_key)` against `ccr.report_job`.
- `GetReport`, `GetReports`, `CancelReport`, and `GeneratePresignedUrl` are scoped by caller identity and read only CCR storage.

## Next step
- Keep this track in maintenance mode; CRUD/list/cancel/presigned path is implemented against CCR storage and already covered by API-level tests.

## Done when
- Done: `CreateReport`, `GetReport`, `GetReports`, `CancelReport`, and `GeneratePresignedUrl` are implemented against CCR storage and covered by API-level tests.
