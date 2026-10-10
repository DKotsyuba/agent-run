-- Attempt-owned idle state and immutable finish intent; no lifetime clock.
CREATE TABLE worker_lifecycle (
  attempt_id TEXT PRIMARY KEY REFERENCES worker_capabilities(attempt_id) ON DELETE CASCADE,
  phase TEXT NOT NULL CHECK (phase IN ('running','idle','closing')),
  turn_count INTEGER NOT NULL DEFAULT 1 CHECK (turn_count >= 1),
  transition_at REAL NOT NULL,
  idle_seconds REAL NOT NULL DEFAULT 0 CHECK (idle_seconds >= 0),
  finish_json TEXT,
  finish_sha256 TEXT CHECK (finish_sha256 IS NULL OR length(finish_sha256)=64),
  accepted_at REAL,
  receipt_observed INTEGER NOT NULL DEFAULT 0 CHECK (receipt_observed IN (0,1)),
  CHECK ((finish_json IS NULL AND finish_sha256 IS NULL AND accepted_at IS NULL AND phase!='closing')
      OR (finish_json IS NOT NULL AND finish_sha256 IS NOT NULL AND accepted_at IS NOT NULL AND phase='closing'))
);
CREATE TRIGGER worker_finish_immutable BEFORE UPDATE ON worker_lifecycle
WHEN NEW.attempt_id IS NOT OLD.attempt_id OR
 (OLD.finish_json IS NOT NULL AND (NEW.finish_json IS NOT OLD.finish_json
  OR NEW.finish_sha256 IS NOT OLD.finish_sha256 OR NEW.accepted_at IS NOT OLD.accepted_at))
BEGIN SELECT RAISE(ABORT,'worker finish intent is immutable'); END;

-- Native completions reuse the durable command queue. Only hashes of native
-- job identities are retained; identical receipts cannot enqueue another turn.
CREATE TABLE worker_native_wakes (
  attempt_id TEXT NOT NULL REFERENCES worker_lifecycle(attempt_id) ON DELETE CASCADE,
  receipt_sha256 TEXT NOT NULL CHECK(length(receipt_sha256)=64),
  command_id INTEGER NOT NULL REFERENCES commands(id) ON DELETE CASCADE,
  created_at REAL NOT NULL,
  PRIMARY KEY(attempt_id,receipt_sha256)
);
