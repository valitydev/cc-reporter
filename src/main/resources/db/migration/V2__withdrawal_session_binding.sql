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
