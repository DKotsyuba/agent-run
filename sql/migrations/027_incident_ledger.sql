-- Bounded, content-free incident phases survive ordinary agent retirement.
-- execution_id is a validated public execution ID, deliberately without a
-- cascading foreign key. phase distinguishes immutable lifecycle observations.
-- occurred_at is their finite Unix timestamp; details_json accepts only the
-- reviewed diagnostic projection, bounded to four KiB of UTF-8 bytes.
CREATE TABLE incident_ledger (
  execution_id TEXT NOT NULL CHECK (execution_id LIKE 'ag-%' AND length(execution_id) <= 64),
  phase TEXT NOT NULL CHECK (phase IN ('terminal','execution_failure','cleanup','delivery_waiting_binding','delivery_pending','delivery_sending','delivery_delivered','delivery_retry_wait','delivery_failed','delivery_cancelled','delivery_expired')),
  occurred_at REAL NOT NULL CHECK (occurred_at >= 0),
  details_json TEXT NOT NULL CHECK (json_valid(details_json) AND json_type(details_json)='object' AND length(CAST(details_json AS BLOB)) <= 4096),
  PRIMARY KEY (execution_id, phase)
);
CREATE INDEX idx_incident_ledger_time ON incident_ledger(occurred_at, execution_id, phase);
CREATE TRIGGER incident_ledger_immutable BEFORE UPDATE ON incident_ledger
BEGIN SELECT RAISE(ABORT,'incident ledger observations are immutable'); END;
