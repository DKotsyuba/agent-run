-- Cooperative pools: one goal shared by a small roster of ordinary executions.
-- Membership history is retained: a replaced member keeps its row and points at
-- its successor through replaced_by (checked at commit, so one transaction can
-- retire the old row and insert its successor), and only one row per slot is current.
-- Entries are an append-only log whose author is stamped at send time; they
-- carry no authority and are never rewritten. Pool rows reference agents and
-- deliveries without cascades, so a purge deletes pool rows before agents.
CREATE TABLE pools (
  id TEXT PRIMARY KEY CHECK (id LIKE 'pool-%' AND length(id) BETWEEN 6 AND 64),
  request_namespace TEXT NOT NULL CHECK (length(request_namespace) BETWEEN 1 AND 512),
  request_id TEXT NOT NULL CHECK (length(request_id) BETWEEN 1 AND 512),
  request_sha256 TEXT NOT NULL CHECK (length(request_sha256) = 64),
  orchestrator_session_id TEXT REFERENCES orchestrator_sessions(id),
  goal TEXT NOT NULL CHECK (length(CAST(goal AS BLOB)) BETWEEN 1 AND 8192),
  acceptance_json TEXT NOT NULL
    CHECK (json_valid(acceptance_json) AND json_type(acceptance_json) = 'array'
      AND length(CAST(acceptance_json AS BLOB)) <= 32768),
  state TEXT NOT NULL CHECK (state IN ('open','completed')),
  roster_revision INTEGER NOT NULL CHECK (roster_revision >= 1),
  completed_at REAL,
  completion_delivery_id TEXT REFERENCES deliveries(id),
  created_at REAL NOT NULL,
  UNIQUE (request_namespace, request_id),
  CHECK ((state = 'completed') = (completed_at IS NOT NULL)),
  CHECK (completion_delivery_id IS NULL OR state = 'completed')
);
CREATE INDEX idx_pools_completion_delivery ON pools(completion_delivery_id)
  WHERE completion_delivery_id IS NOT NULL;

CREATE TABLE pool_members (
  agent_id TEXT PRIMARY KEY REFERENCES agents(id),
  pool_id TEXT NOT NULL REFERENCES pools(id),
  slot INTEGER NOT NULL CHECK (slot BETWEEN 1 AND 5),
  name TEXT NOT NULL CHECK (length(name) BETWEEN 1 AND 256),
  role TEXT NOT NULL CHECK (length(CAST(role AS BLOB)) BETWEEN 1 AND 256),
  personal_task TEXT NOT NULL,
  joined_roster_revision INTEGER NOT NULL CHECK (joined_roster_revision >= 1),
  replaced_by TEXT,
  UNIQUE (pool_id, agent_id),
  FOREIGN KEY (pool_id, replaced_by) REFERENCES pool_members(pool_id, agent_id)
    DEFERRABLE INITIALLY DEFERRED,
  CHECK (replaced_by IS NULL OR replaced_by <> agent_id)
);
CREATE UNIQUE INDEX idx_pool_members_current_slot ON pool_members(pool_id, slot)
  WHERE replaced_by IS NULL;
CREATE UNIQUE INDEX idx_pool_members_current_name ON pool_members(pool_id, name)
  WHERE replaced_by IS NULL;
CREATE TRIGGER pool_members_identity_immutable
BEFORE UPDATE ON pool_members
WHEN NEW.agent_id IS NOT OLD.agent_id OR NEW.pool_id IS NOT OLD.pool_id
  OR NEW.slot IS NOT OLD.slot OR NEW.name IS NOT OLD.name OR NEW.role IS NOT OLD.role
  OR NEW.personal_task IS NOT OLD.personal_task
  OR NEW.joined_roster_revision IS NOT OLD.joined_roster_revision
  OR (OLD.replaced_by IS NOT NULL AND NEW.replaced_by IS NOT OLD.replaced_by)
BEGIN
  SELECT RAISE(ABORT, 'pool member identity is immutable');
END;

CREATE TABLE pool_entries (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  pool_id TEXT NOT NULL REFERENCES pools(id),
  author_kind TEXT NOT NULL CHECK (author_kind IN ('member','operator','broker')),
  author_agent_id TEXT,
  author_name TEXT,
  author_role TEXT,
  direction TEXT NOT NULL CHECK (direction IN ('team','orchestrator_copy')),
  kind TEXT NOT NULL CHECK (kind IN ('message','report','proposal','vote','revoke','roster')),
  severity TEXT CHECK (severity IN ('notice','risk','question','blocker')),
  proposal_seq INTEGER,
  roster_revision INTEGER NOT NULL CHECK (roster_revision >= 1),
  decision TEXT CHECK (decision IN ('ready','block')),
  snapshot TEXT CHECK (length(CAST(snapshot AS BLOB)) <= 65536),
  checks_json TEXT CHECK (json_valid(checks_json) AND length(CAST(checks_json AS BLOB)) <= 16384),
  body TEXT NOT NULL CHECK (length(CAST(body AS BLOB)) BETWEEN 1 AND 8192),
  delivery_id TEXT UNIQUE REFERENCES deliveries(id),
  sender_run_id TEXT REFERENCES agents(id),
  sender_attempt_id TEXT REFERENCES attempts(id),
  idem_scope TEXT NOT NULL CHECK (length(idem_scope) BETWEEN 1 AND 64),
  request_id TEXT NOT NULL CHECK (length(request_id) BETWEEN 1 AND 128),
  created_at REAL NOT NULL,
  UNIQUE (pool_id, seq),
  UNIQUE (pool_id, idem_scope, request_id),
  FOREIGN KEY (pool_id, author_agent_id) REFERENCES pool_members(pool_id, agent_id),
  FOREIGN KEY (pool_id, proposal_seq) REFERENCES pool_entries(pool_id, seq),
  CHECK ((author_kind = 'member'
      AND author_agent_id IS NOT NULL AND author_name IS NOT NULL AND author_role IS NOT NULL
      AND sender_run_id IS NOT NULL AND sender_attempt_id IS NOT NULL)
    OR (author_kind <> 'member'
      AND author_agent_id IS NULL AND author_name IS NULL AND author_role IS NULL
      AND sender_run_id IS NULL AND sender_attempt_id IS NULL)),
  CHECK (author_kind <> 'operator' OR (kind = 'message' AND direction = 'team')),
  CHECK (author_kind <> 'broker' OR (kind = 'roster' AND direction = 'team')),
  CHECK (kind <> 'roster' OR author_kind = 'broker'),
  CHECK (direction = 'team' OR kind = 'report'),
  CHECK ((kind = 'report') = (severity IS NOT NULL)),
  CHECK (delivery_id IS NULL OR kind = 'report'),
  CHECK ((kind = 'proposal') = (snapshot IS NOT NULL)),
  CHECK ((kind IN ('vote','revoke')) = (proposal_seq IS NOT NULL)),
  CHECK ((kind = 'vote') = (decision IS NOT NULL)),
  CHECK (kind IN ('vote','proposal') OR checks_json IS NULL)
);
CREATE INDEX idx_pool_entries_pool_seq ON pool_entries(pool_id, seq);
CREATE INDEX idx_pool_entries_sender_run ON pool_entries(sender_run_id)
  WHERE sender_run_id IS NOT NULL;
CREATE TRIGGER pool_entries_immutable
BEFORE UPDATE ON pool_entries
BEGIN
  SELECT RAISE(ABORT, 'pool entries are immutable');
END;
