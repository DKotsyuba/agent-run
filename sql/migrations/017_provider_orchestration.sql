-- Provider orchestration: account registry, quota capacity identity, and
-- per-attempt selection/process facts. Purely additive: every new column is
-- NULLable, so historical rows and current 'running' attempt inserts stay
-- valid until consumers transition to the new contracts.

CREATE TABLE provider_accounts (
  account_id TEXT PRIMARY KEY,
  auth_family TEXT NOT NULL,
  secret_ref TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('enabled', 'disabled')),
  created_at REAL NOT NULL,
  updated_at REAL NOT NULL
);

CREATE TABLE quota_capacity_revisions (
  quota_key TEXT PRIMARY KEY,
  capacity_revision INTEGER NOT NULL CHECK (capacity_revision >= 0),
  updated_at REAL NOT NULL
);

ALTER TABLE agents ADD COLUMN selection_intent TEXT
  CHECK (selection_intent IN ('auto', 'pinned'));
ALTER TABLE agents ADD COLUMN requested_account_id TEXT;

ALTER TABLE attempts ADD COLUMN selected_account_id TEXT;
ALTER TABLE attempts ADD COLUMN phase TEXT;
ALTER TABLE attempts ADD COLUMN process_identity TEXT;
ALTER TABLE attempts ADD COLUMN process_birth_time REAL;
ALTER TABLE attempts ADD COLUMN cleanup_proof_json TEXT;
ALTER TABLE attempts ADD COLUMN session_facts_json TEXT;
ALTER TABLE attempts ADD COLUMN ownership_active INTEGER NOT NULL DEFAULT 0
  CHECK (ownership_active IN (0, 1));

CREATE INDEX idx_attempts_selected_account
  ON attempts(selected_account_id) WHERE selected_account_id IS NOT NULL;

-- At most one attempt owns the agent's execution slot at any time. The
-- ownership flag spans the whole orchestrated lifecycle: an attempt holds
-- ownership from claim (prepared/starting) through running, account switch,
-- and cleanup-pending, and releases it (set to 0) only after verified process
-- teardown and cleanup proof. Legacy rows default to 0, so historical stores
-- with several open 'running' attempts and current release inserts that do
-- not claim ownership migrate and keep working unchanged.
CREATE UNIQUE INDEX idx_attempts_one_active
  ON attempts(agent_id) WHERE ownership_active = 1;
