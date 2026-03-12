# PROJECT_STATE.md

## Active tracks
- `kafka-to-db-ingestion`

## Completed tracks
- `thrift-api-implementation`
- `report-lifecycle-subsystem`

## Temporary cross-track exception
- `payments` FX handling remains a temporary exception: keep the contract, allow mock+`TODO`, and do not silently degrade it to permanent `null` behavior.

## Cross-track dependencies
- `kafka-to-db-ingestion` defines the read-model columns used by CSV generation and report filters.
- `kafka-to-db-ingestion` must keep `payment_txn_current` / `withdrawal_txn_current` compatible with the completed report execution and Thrift tracks.
