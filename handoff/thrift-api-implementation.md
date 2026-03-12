# thrift-api-implementation

## Scope
- Implement the CCR Thrift boundary and persistence-facing service layer for report creation, listing, cancel, and presigned downloads.

## Open work
- Implement request validation, persistence access, state reads, cancel, and presigned download handling.
- `CreateReport` must be idempotent by `(created_by, idempotency_key)`.

## Verified constraints
- API contract lives in `cc-reporter-proto/proto/ccreports.thrift`.
- The Thrift boundary should validate, map errors, and delegate; report building does not belong in the handler layer.
- `file-storage` / presigned URL approach remains unchanged.

## Next step
- Implement DAO and service methods around `report_job` / `report_file`, then wire the Thrift handler with request validation and pagination token handling.

## Done when
- `CreateReport`, `GetReport`, `GetReports`, `CancelReport`, and `GeneratePresignedUrl` are implemented against CCR storage and covered by API-level tests.
