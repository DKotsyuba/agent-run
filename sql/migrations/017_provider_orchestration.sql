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
  updated_at REAL NOT NULL,
  UNIQUE (auth_family, secret_ref)
);

-- Historical capacity rows keep their original runtime/target identity and
-- receive NULLs. New provider observations name the registered global account
-- and its exact physical lane instead of overloading target.
ALTER TABLE capacity_samples ADD COLUMN account_id TEXT
  REFERENCES provider_accounts(account_id);
ALTER TABLE capacity_samples ADD COLUMN quota_key TEXT
  CHECK (
    (account_id IS NULL AND quota_key IS NULL)
    OR (account_id IS NOT NULL AND quota_key IS NOT NULL
      AND substr(quota_key, 1, length(account_id) + 2) = account_id || '::'
      AND length(quota_key) > length(account_id) + 2)
  );
CREATE INDEX idx_capacity_samples_account_key
  ON capacity_samples(account_id, quota_key) WHERE account_id IS NOT NULL;

-- Durable exhaustion survives bounded capacity_samples retention. Source is
-- the stable collector identity; collector_revision is non-key metadata.
CREATE TABLE quota_exhaustion (
  account_id TEXT NOT NULL REFERENCES provider_accounts(account_id),
  quota_key TEXT NOT NULL,
  source TEXT NOT NULL CHECK (length(trim(source)) > 0),
  window_id TEXT NOT NULL CHECK (length(trim(window_id)) > 0),
  observed_at REAL NOT NULL,
  reset_at REAL,
  collector_revision TEXT,
  PRIMARY KEY (account_id, quota_key, source, window_id),
  CHECK (
    substr(quota_key, 1, length(account_id) + 2) = account_id || '::'
    AND length(quota_key) > length(account_id) + 2
  )
);

-- One committed revision for the entire scored snapshot. Each relevant quota
-- mutation advances this singleton in the same transaction as its facts.
CREATE TABLE quota_capacity_revision (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  revision INTEGER NOT NULL CHECK (revision >= 0),
  updated_at REAL NOT NULL
);
INSERT INTO quota_capacity_revision VALUES (1, 0, 0.0);

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

-- One attempt can reserve several physical pools. Keys are inserted with the
-- selected account in the admission transaction and remain as durable facts
-- after ownership releases; active counts join ownership_active.
CREATE TABLE attempt_quota_keys (
  attempt_id TEXT NOT NULL REFERENCES attempts(id),
  quota_key TEXT NOT NULL,
  PRIMARY KEY (attempt_id, quota_key)
);
CREATE INDEX idx_attempt_quota_keys_key ON attempt_quota_keys(quota_key);
CREATE TRIGGER attempt_quota_keys_account_guard
BEFORE INSERT ON attempt_quota_keys
WHEN NOT EXISTS (
  SELECT 1 FROM attempts
  WHERE id = NEW.attempt_id AND selected_account_id IS NOT NULL
    AND substr(NEW.quota_key, 1, length(selected_account_id) + 2) = selected_account_id || '::'
    AND length(NEW.quota_key) > length(selected_account_id) + 2
)
BEGIN
  SELECT RAISE(ABORT, 'quota key must belong to selected account');
END;
-- A legacy NULL selection may be bound once; a selected attempt cannot move
-- its durable reservations to another account.
CREATE TRIGGER attempts_selected_account_immutable
BEFORE UPDATE OF selected_account_id ON attempts
WHEN OLD.selected_account_id IS NOT NULL
  AND NEW.selected_account_id IS NOT OLD.selected_account_id
BEGIN
  SELECT RAISE(ABORT, 'selected account is immutable');
END;
CREATE TRIGGER attempt_quota_keys_immutable
BEFORE UPDATE ON attempt_quota_keys
WHEN NEW.attempt_id IS NOT OLD.attempt_id OR NEW.quota_key IS NOT OLD.quota_key
BEGIN
  SELECT RAISE(ABORT, 'attempt quota keys are immutable');
END;

-- At most one attempt owns the agent's execution slot at any time. The
-- ownership flag spans the whole orchestrated lifecycle: an attempt holds
-- ownership from claim (prepared/starting) through running, account switch,
-- and cleanup-pending. Consumers may release it only after verified process
-- teardown and cleanup proof; this index enforces uniqueness only. Legacy
-- rows default to 0, so historical stores with several open 'running'
-- attempts and current release inserts that do
-- not claim ownership migrate and keep working unchanged.
CREATE UNIQUE INDEX idx_attempts_one_active
  ON attempts(agent_id) WHERE ownership_active = 1;
