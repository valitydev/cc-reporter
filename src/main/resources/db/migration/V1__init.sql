CREATE SCHEMA IF NOT EXISTS ccr;

-- Needed for case-insensitive partial search by name/id fragments.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TYPE ccr.report_status AS ENUM (
  'pending',
  'processing',
  'created',
  'failed',
  'canceled',
  'timed_out',
  'expired'
);

CREATE TYPE ccr.report_type AS ENUM (
  'payments',
  'withdrawals'
);

CREATE TYPE ccr.file_type AS ENUM (
  'csv'
);

CREATE TABLE ccr.report_job (
  id BIGSERIAL PRIMARY KEY,

  report_type ccr.report_type NOT NULL,
  file_type ccr.file_type NOT NULL,
  query_json JSONB NOT NULL,
  query_hash VARCHAR(64) NOT NULL,

  requested_time_from TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  requested_time_to TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  timezone VARCHAR NOT NULL DEFAULT 'UTC',

  status ccr.report_status NOT NULL DEFAULT 'pending',
  created_by VARCHAR NOT NULL,
  idempotency_key VARCHAR,

  rows_count BIGINT,
  attempt INT NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMP WITHOUT TIME ZONE,
  data_snapshot_fixed_at TIMESTAMP WITHOUT TIME ZONE,

  error_code VARCHAR,
  error_message VARCHAR,

  created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
  updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
  started_at TIMESTAMP WITHOUT TIME ZONE,
  finished_at TIMESTAMP WITHOUT TIME ZONE,
  expires_at TIMESTAMP WITHOUT TIME ZONE,

  CONSTRAINT report_job_attempt_chk CHECK (attempt >= 0),
  CONSTRAINT report_job_time_range_chk CHECK (requested_time_from < requested_time_to)
);

CREATE TABLE ccr.report_file (
  id BIGSERIAL PRIMARY KEY,
  report_id BIGINT NOT NULL REFERENCES ccr.report_job(id) ON DELETE CASCADE,

  file_id VARCHAR NOT NULL UNIQUE,
  file_type ccr.file_type NOT NULL,
  bucket VARCHAR NOT NULL,
  object_key VARCHAR NOT NULL,
  filename VARCHAR NOT NULL,
  content_type VARCHAR NOT NULL DEFAULT 'text/csv',

  size_bytes BIGINT,
  md5 VARCHAR NOT NULL,
  sha256 VARCHAR NOT NULL,

  created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),

  CONSTRAINT report_file_report_id_uniq UNIQUE (report_id),
  CONSTRAINT report_file_storage_uniq UNIQUE (bucket, object_key)
);

CREATE TABLE ccr.report_audit_event (
  id BIGSERIAL PRIMARY KEY,
  report_id BIGINT NOT NULL REFERENCES ccr.report_job(id) ON DELETE CASCADE,
  event_type VARCHAR NOT NULL,
  actor VARCHAR,
  payload_json JSONB,
  created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
);

CREATE TABLE ccr.payment_txn_current (
  id BIGSERIAL PRIMARY KEY,

  invoice_id VARCHAR NOT NULL,
  payment_id VARCHAR NOT NULL,

  -- Domain event order comes from MachineEvent.eventId.
  domain_event_id BIGINT NOT NULL,
  domain_event_created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,

  party_id VARCHAR NOT NULL,
  shop_id VARCHAR,
  shop_name VARCHAR,

  created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  finalized_at TIMESTAMP WITHOUT TIME ZONE,
  status VARCHAR NOT NULL,

  provider_id VARCHAR,
  provider_name VARCHAR,
  terminal_id VARCHAR,
  terminal_name VARCHAR,

  amount BIGINT NOT NULL,
  fee BIGINT,
  currency VARCHAR NOT NULL,

  trx_id VARCHAR,
  external_id VARCHAR,
  rrn VARCHAR,
  approval_code VARCHAR,
  payment_tool_type VARCHAR,
  error_summary VARCHAR,

  original_amount BIGINT,
  original_currency VARCHAR,
  converted_amount BIGINT,
  exchange_rate_internal NUMERIC(20, 10),
  provider_amount BIGINT,
  provider_currency VARCHAR,

  shop_search VARCHAR,
  provider_search VARCHAR,
  terminal_search VARCHAR,
  trx_search VARCHAR,

  updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),

  CONSTRAINT payment_txn_current_event_chk CHECK (domain_event_id > 0),
  CONSTRAINT payment_txn_current_uniq UNIQUE (invoice_id, payment_id)
);

CREATE TABLE ccr.withdrawal_txn_current (
  id BIGSERIAL PRIMARY KEY,

  withdrawal_id VARCHAR NOT NULL,

  -- Domain event order comes from MachineEvent.eventId.
  domain_event_id BIGINT NOT NULL,
  domain_event_created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,

  party_id VARCHAR NOT NULL,
  wallet_id VARCHAR,
  wallet_name VARCHAR,
  destination_id VARCHAR,

  created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  finalized_at TIMESTAMP WITHOUT TIME ZONE,
  status VARCHAR NOT NULL,

  provider_id VARCHAR,
  provider_name VARCHAR,
  terminal_id VARCHAR,
  terminal_name VARCHAR,

  amount BIGINT NOT NULL,
  fee BIGINT,
  currency VARCHAR NOT NULL,

  trx_id VARCHAR,
  external_id VARCHAR,
  error_code VARCHAR,
  error_reason VARCHAR,
  error_sub_failure VARCHAR,

  original_amount BIGINT,
  original_currency VARCHAR,
  converted_amount BIGINT,
  exchange_rate_internal NUMERIC(20, 10),
  provider_amount BIGINT,
  provider_currency VARCHAR,

  wallet_search VARCHAR,
  provider_search VARCHAR,
  terminal_search VARCHAR,
  trx_search VARCHAR,

  updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),

  CONSTRAINT withdrawal_txn_current_event_chk CHECK (domain_event_id > 0),
  CONSTRAINT withdrawal_txn_current_uniq UNIQUE (withdrawal_id)
);

CREATE INDEX report_job_created_by_created_at_idx
  ON ccr.report_job (created_by, created_at DESC, id DESC);

CREATE INDEX report_job_pending_idx
  ON ccr.report_job (next_attempt_at, created_at, id)
  WHERE status = 'pending';

CREATE INDEX report_job_processing_updated_at_idx
  ON ccr.report_job (updated_at, id)
  WHERE status = 'processing';

CREATE INDEX report_job_time_range_idx
  ON ccr.report_job (requested_time_from, requested_time_to, id DESC);

CREATE INDEX report_job_report_type_file_type_created_at_idx
  ON ccr.report_job (report_type, file_type, created_at DESC, id DESC);

CREATE INDEX report_job_expires_at_idx
  ON ccr.report_job (expires_at)
  WHERE expires_at IS NOT NULL;

CREATE UNIQUE INDEX report_job_idempotency_key_uniq
  ON ccr.report_job (created_by, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

CREATE INDEX report_audit_event_report_id_idx
  ON ccr.report_audit_event (report_id, created_at DESC, id DESC);

CREATE INDEX payment_txn_created_at_idx
  ON ccr.payment_txn_current (created_at DESC, id DESC);

CREATE INDEX payment_txn_finalized_at_idx
  ON ccr.payment_txn_current (finalized_at DESC, id DESC)
  WHERE finalized_at IS NOT NULL;

CREATE INDEX payment_txn_filters_idx
  ON ccr.payment_txn_current (
    party_id,
    shop_id,
    provider_id,
    terminal_id,
    status,
    currency,
    created_at DESC,
    id DESC
  );

CREATE INDEX payment_txn_trx_idx
  ON ccr.payment_txn_current (trx_id);

CREATE INDEX payment_txn_shop_trgm_idx
  ON ccr.payment_txn_current USING gin (shop_search gin_trgm_ops);

CREATE INDEX payment_txn_provider_trgm_idx
  ON ccr.payment_txn_current USING gin (provider_search gin_trgm_ops);

CREATE INDEX payment_txn_terminal_trgm_idx
  ON ccr.payment_txn_current USING gin (terminal_search gin_trgm_ops);

CREATE INDEX payment_txn_trx_trgm_idx
  ON ccr.payment_txn_current USING gin (trx_search gin_trgm_ops);

CREATE INDEX withdrawal_txn_created_at_idx
  ON ccr.withdrawal_txn_current (created_at DESC, id DESC);

CREATE INDEX withdrawal_txn_finalized_at_idx
  ON ccr.withdrawal_txn_current (finalized_at DESC, id DESC)
  WHERE finalized_at IS NOT NULL;

CREATE INDEX withdrawal_txn_filters_idx
  ON ccr.withdrawal_txn_current (
    party_id,
    wallet_id,
    provider_id,
    terminal_id,
    status,
    currency,
    created_at DESC,
    id DESC
  );

CREATE INDEX withdrawal_txn_trx_idx
  ON ccr.withdrawal_txn_current (trx_id);

CREATE INDEX withdrawal_txn_wallet_trgm_idx
  ON ccr.withdrawal_txn_current USING gin (wallet_search gin_trgm_ops);

CREATE INDEX withdrawal_txn_provider_trgm_idx
  ON ccr.withdrawal_txn_current USING gin (provider_search gin_trgm_ops);

CREATE INDEX withdrawal_txn_terminal_trgm_idx
  ON ccr.withdrawal_txn_current USING gin (terminal_search gin_trgm_ops);

CREATE INDEX withdrawal_txn_trx_trgm_idx
  ON ccr.withdrawal_txn_current USING gin (trx_search gin_trgm_ops);

CREATE TABLE ccr.withdrawal_session_binding_current (
  id BIGSERIAL PRIMARY KEY,
  session_id VARCHAR NOT NULL,
  withdrawal_id VARCHAR NOT NULL,
  domain_event_id BIGINT NOT NULL,
  domain_event_created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),

  CONSTRAINT withdrawal_session_binding_current_event_chk CHECK (domain_event_id > 0),
  CONSTRAINT withdrawal_session_binding_current_session_uniq UNIQUE (session_id)
);

CREATE INDEX withdrawal_session_binding_current_withdrawal_idx
  ON ccr.withdrawal_session_binding_current (withdrawal_id);

-- Ingest contract (implemented in application code):
-- payments:
--   INSERT ... ON CONFLICT (invoice_id, payment_id) DO UPDATE
--   ... WHERE ccr.payment_txn_current.domain_event_id < EXCLUDED.domain_event_id;
--
-- withdrawals:
--   INSERT ... ON CONFLICT (withdrawal_id) DO UPDATE
--   ... WHERE ccr.withdrawal_txn_current.domain_event_id < EXCLUDED.domain_event_id;
