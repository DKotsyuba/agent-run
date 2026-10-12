-- Single resident writer: durable operation receipts committed atomically with
-- each mutation, durable per-producer order watermarks, the writer authority
-- epoch and launch capability hashes. Results are canonical ASCII JSON whose
-- byte length (not character count) is bounded.
CREATE TABLE operation_receipts (
  commit_seq INTEGER PRIMARY KEY AUTOINCREMENT,
  operation_id TEXT NOT NULL UNIQUE CHECK (length(operation_id) = 66),
  digest TEXT NOT NULL CHECK (length(digest) = 64),
  op_kind TEXT NOT NULL,
  producer_key TEXT NOT NULL,
  ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
  authority_kind TEXT NOT NULL,
  authority_fingerprint TEXT NOT NULL CHECK (length(authority_fingerprint) = 64),
  agent_id TEXT REFERENCES agents(id) ON DELETE CASCADE,
  outcome TEXT NOT NULL CHECK (outcome IN ('committed','rejected')),
  result_json TEXT NOT NULL CHECK (length(CAST(result_json AS BLOB)) <= 65536
    AND result_json NOT GLOB '*[^ -~]*'),
  writer_epoch INTEGER NOT NULL,
  committed_at REAL NOT NULL
);
CREATE INDEX operation_receipts_unscoped ON operation_receipts(committed_at) WHERE agent_id IS NULL;
CREATE INDEX operation_receipts_agent ON operation_receipts(agent_id) WHERE agent_id IS NOT NULL;
CREATE TABLE writer_producers (
  producer_key TEXT PRIMARY KEY,
  last_ordinal INTEGER NOT NULL CHECK (last_ordinal >= 0),
  last_boot_tag TEXT NOT NULL CHECK (length(last_boot_tag) = 16),
  last_issued_ns INTEGER NOT NULL CHECK (last_issued_ns >= 0),
  execution_id TEXT,
  updated_at REAL NOT NULL
);
CREATE TABLE writer_authority (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  epoch INTEGER NOT NULL CHECK (epoch >= 1),
  holder_pid INTEGER NOT NULL,
  holder_birth REAL,
  holder_token TEXT,
  acquired_at REAL NOT NULL,
  probe_at REAL
);
ALTER TABLE agents ADD COLUMN supervisor_capability_sha256 TEXT
  CHECK (supervisor_capability_sha256 IS NULL OR length(supervisor_capability_sha256) = 64);
ALTER TABLE managed_service_generations ADD COLUMN exec_capability_sha256 TEXT
  CHECK (exec_capability_sha256 IS NULL OR length(exec_capability_sha256) = 64);
-- The incident ledger gains the projection_proof phase: the only phase whose
-- row may be refreshed in place, and only on its own (execution_id, phase)
-- key. Every other phase stays immutable. SQLite bakes a CHECK constraint into
-- the table's CREATE text, so the table is rebuilt with its rows, index and
-- trigger restored exactly to the canonical schema.
ALTER TABLE incident_ledger RENAME TO incident_ledger_v29;
CREATE TABLE incident_ledger (
  execution_id TEXT NOT NULL CHECK (execution_id LIKE 'ag-%' AND length(execution_id) <= 64),
  phase TEXT NOT NULL CHECK (phase IN ('terminal','execution_failure','cleanup','delivery_waiting_binding','delivery_pending','delivery_sending','delivery_delivered','delivery_retry_wait','delivery_failed','delivery_cancelled','delivery_expired','projection_proof')),
  occurred_at REAL NOT NULL CHECK (occurred_at >= 0),
  details_json TEXT NOT NULL CHECK (json_valid(details_json) AND json_type(details_json)='object' AND length(CAST(details_json AS BLOB)) <= 4096),
  PRIMARY KEY (execution_id, phase)
);
INSERT INTO incident_ledger (execution_id, phase, occurred_at, details_json)
SELECT execution_id, phase, occurred_at, details_json FROM incident_ledger_v29;
DROP TABLE incident_ledger_v29;
CREATE INDEX idx_incident_ledger_time ON incident_ledger(occurred_at, execution_id, phase);
CREATE TRIGGER incident_ledger_immutable BEFORE UPDATE ON incident_ledger
WHEN OLD.phase <> 'projection_proof' OR NEW.phase <> 'projection_proof'
  OR NEW.execution_id IS NOT OLD.execution_id
BEGIN SELECT RAISE(ABORT,'incident ledger observations are immutable'); END;
-- Incident source lookups (first failure, latest cleanup) seek one
-- execution's events of one kind instead of walking its whole journal.
CREATE INDEX idx_events_agent_kind_seq ON events(agent_id, kind, seq);
