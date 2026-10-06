-- Active independent-worker enrollment without changing execution ownership.
ALTER TABLE agents ADD COLUMN pool_membership_ever INTEGER DEFAULT 0 CHECK(pool_membership_ever IN (0,1));
-- Existing unrecorded history is unknown; never infer independence from missing GC rows.
UPDATE agents SET pool_membership_ever=NULL;
UPDATE agents SET pool_membership_ever=1 WHERE id IN (SELECT agent_id FROM pool_members);
ALTER TABLE worker_capabilities ADD COLUMN pool_catalog_version INTEGER;
ALTER TABLE worker_capabilities ADD COLUMN pool_catalog_digest TEXT
  CHECK (pool_catalog_digest IS NULL OR length(pool_catalog_digest)=64);
ALTER TABLE worker_capabilities ADD COLUMN pool_catalog_observed_at REAL;

CREATE TABLE pool_enrollments (
  agent_id TEXT PRIMARY KEY REFERENCES pool_members(agent_id) ON DELETE CASCADE,
  run_id TEXT NOT NULL REFERENCES agents(id),
  attempt_id TEXT NOT NULL REFERENCES attempts(id),
  challenge TEXT NOT NULL UNIQUE CHECK(length(challenge) BETWEEN 1 AND 128),
  deadline REAL NOT NULL,
  state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','joined','needs_action')),
  created_at REAL NOT NULL,
  ack_seq INTEGER REFERENCES pool_entries(seq),
  ack_at REAL,
  failure_reason TEXT,
  attention_delivery_id TEXT REFERENCES deliveries(id) ON DELETE SET NULL,
  attention_issued INTEGER NOT NULL DEFAULT 0 CHECK(attention_issued IN (0,1)),
  CHECK (state<>'joined' OR (ack_seq IS NOT NULL AND ack_at IS NOT NULL))
);
CREATE INDEX idx_pool_enrollments_pending ON pool_enrollments(state,deadline);
CREATE TRIGGER pool_enrollment_identity_immutable BEFORE UPDATE ON pool_enrollments
WHEN NEW.agent_id IS NOT OLD.agent_id OR NEW.run_id IS NOT OLD.run_id
  OR NEW.attempt_id IS NOT OLD.attempt_id OR NEW.challenge IS NOT OLD.challenge
  OR NEW.deadline IS NOT OLD.deadline OR NEW.created_at IS NOT OLD.created_at
  OR (OLD.state='joined' AND NEW.state<>'joined')
  OR (OLD.attention_issued=1 AND NEW.attention_issued<>1)
BEGIN SELECT RAISE(ABORT,'pool enrollment identity is immutable'); END;

CREATE TRIGGER pool_members_remember_history AFTER INSERT ON pool_members
BEGIN UPDATE agents SET pool_membership_ever=1 WHERE id=NEW.agent_id; END;
CREATE TRIGGER agents_pool_history_immutable BEFORE UPDATE OF pool_membership_ever ON agents
WHEN OLD.pool_membership_ever=1 AND NEW.pool_membership_ever IS NOT 1
BEGIN SELECT RAISE(ABORT,'pool membership history is immutable'); END;
