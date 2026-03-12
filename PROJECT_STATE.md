# PROJECT_STATE.md

## Active tracks
- `kafka-to-db-ingestion`
- `thrift-api-implementation`
- `report-lifecycle-subsystem`

## Temporary cross-track exception
- `payments` FX handling remains a temporary exception: keep the contract, allow mock+`TODO`, and do not silently degrade it to permanent `null` behavior.

## Cross-track dependencies
- `kafka-to-db-ingestion` defines the read-model columns used by CSV generation and report filters.
- `thrift-api-implementation` and `report-lifecycle-subsystem` must use the same `report_job` / `report_file` status model.
- `report-lifecycle-subsystem` depends on stable read-model schema and storage integration.
