PRAGMA auto_vacuum = INCREMENTAL;

CREATE TABLE orchestrator_sessions (
  id TEXT PRIMARY KEY,
  transport TEXT NOT NULL,
  external_session_id TEXT NOT NULL,
  external_turn_id TEXT,
  created_at REAL NOT NULL,
  last_seen_at REAL NOT NULL,
  UNIQUE (transport, external_session_id)
);

CREATE TABLE agents (
  id TEXT PRIMARY KEY,
  request_id TEXT,
  orchestrator_session_id TEXT REFERENCES orchestrator_sessions(id),
  runtime TEXT NOT NULL,
  model TEXT NOT NULL,
  profile TEXT NOT NULL,
  task TEXT NOT NULL,
  task_summary TEXT NOT NULL,
  workdir TEXT NOT NULL,
  request_json TEXT NOT NULL,
  status TEXT NOT NULL CHECK (
    status IN (
      'created', 'starting', 'running', 'cancelling', 'succeeded', 'failed',
      'timed_out', 'cancelled', 'lost'
    )
  ),
  created_at REAL NOT NULL,
  started_at REAL,
  finished_at REAL,
  timeout_seconds REAL NOT NULL,
  supervisor_pid INTEGER,
  supervisor_identity TEXT,
  process_group_id INTEGER,
  heartbeat_at REAL,
  runtime_session_id TEXT,
  config_revision TEXT NOT NULL,
  exit_code INTEGER,
  failure_kind TEXT,
  failure_text TEXT,
  warned INTEGER NOT NULL DEFAULT 0 CHECK (warned IN (0, 1)),
  silent_seconds REAL,
  answer_path TEXT,
  answer_bytes INTEGER,
  answer_sha256 TEXT,
  startup_owner_pid_identity TEXT,
  startup_deadline_at REAL,
  parent_agent_id TEXT REFERENCES agents(id),
  root_agent_id TEXT NOT NULL DEFAULT '',
  sequence INTEGER NOT NULL DEFAULT 1 CHECK (sequence >= 1),
  resume_of_runtime_session_id TEXT,
  identity_json TEXT,
  supervisor_birth_time REAL,
  startup_owner_birth_time REAL,
  selection_intent TEXT CHECK (selection_intent IN ('auto', 'pinned')),
  requested_account_id TEXT,
  UNIQUE (orchestrator_session_id, request_id)
);
CREATE UNIQUE INDEX agents_parent_agent_id_unique
  ON agents(parent_agent_id) WHERE parent_agent_id IS NOT NULL;
CREATE INDEX idx_agents_request_id
  ON agents(request_id) WHERE request_id IS NOT NULL;

CREATE TABLE attempts (
  id TEXT PRIMARY KEY,
  agent_id TEXT NOT NULL REFERENCES agents(id),
  number INTEGER NOT NULL,
  state TEXT NOT NULL,
  adapter_state_json TEXT NOT NULL,
  created_at REAL NOT NULL,
  finished_at REAL,
  selected_account_id TEXT,
  phase TEXT,
  process_identity TEXT,
  process_birth_time REAL,
  cleanup_proof_json TEXT,
  session_facts_json TEXT,
  ownership_active INTEGER NOT NULL DEFAULT 0 CHECK (ownership_active IN (0, 1)),
  UNIQUE (agent_id, number)
);

CREATE TABLE events (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  agent_id TEXT NOT NULL REFERENCES agents(id),
  attempt_id TEXT REFERENCES attempts(id),
  at REAL NOT NULL,
  kind TEXT NOT NULL,
  from_status TEXT,
  to_status TEXT,
  data_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE messages (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  agent_id TEXT NOT NULL REFERENCES agents(id),
  attempt_id TEXT REFERENCES attempts(id),
  at REAL NOT NULL,
  role TEXT NOT NULL,
  name TEXT,
  content TEXT NOT NULL,
  raw_ref TEXT
);

CREATE TABLE commands (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  agent_id TEXT NOT NULL REFERENCES agents(id),
  kind TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('pending', 'claimed', 'completed')),
  created_at REAL NOT NULL,
  claimed_at REAL,
  completed_at REAL,
  result_json TEXT
);

CREATE TABLE deliveries (
  id TEXT PRIMARY KEY,
  agent_id TEXT NOT NULL REFERENCES agents(id),
  orchestrator_session_id TEXT REFERENCES orchestrator_sessions(id),
  terminal_event_seq INTEGER REFERENCES events(seq),
  state TEXT NOT NULL CHECK (
    state IN (
      'waiting_binding', 'pending', 'sending', 'delivered', 'retry_wait',
      'failed', 'cancelled', 'expired'
    )
  ),
  attempts INTEGER NOT NULL DEFAULT 0,
  lease_owner TEXT,
  lease_until REAL,
  next_attempt_at REAL,
  remote_message_id TEXT,
  last_error TEXT,
  ambiguous_result INTEGER NOT NULL DEFAULT 0
    CHECK (ambiguous_result IN (0, 1)),
  UNIQUE (agent_id, terminal_event_seq)
);

CREATE TABLE delivery_attempt_evidence (
  delivery_id TEXT NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
  attempt INTEGER NOT NULL CHECK (attempt > 0),
  recorded_at REAL NOT NULL,
  evidence_json TEXT NOT NULL
    CHECK (length(CAST(evidence_json AS BLOB)) <= 16384),
  PRIMARY KEY (delivery_id, attempt)
);

CREATE TABLE capacity_samples (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  runtime TEXT NOT NULL,
  lane TEXT NOT NULL,
  window TEXT NOT NULL,
  target TEXT,
  source TEXT NOT NULL,
  remaining_percent REAL,
  reset_at REAL,
  observed_at REAL,
  valid_until REAL,
  payload_json TEXT NOT NULL
, account_id TEXT
  REFERENCES provider_accounts(account_id), quota_key TEXT
  CHECK (
    (account_id IS NULL AND quota_key IS NULL)
    OR (account_id IS NOT NULL AND quota_key IS NOT NULL
      AND substr(quota_key, 1, length(account_id) + 2) = account_id || '::'
      AND length(quota_key) > length(account_id) + 2)
  ));

CREATE TABLE context_receipts (
  orchestrator_session_id TEXT PRIMARY KEY REFERENCES orchestrator_sessions(id),
  context_key TEXT NOT NULL,
  injected_at REAL NOT NULL
);

CREATE TABLE reconciliation_cursors (
  name TEXT PRIMARY KEY,
  created_at REAL NOT NULL,
  agent_id TEXT NOT NULL
);

CREATE TABLE provider_accounts (
  account_id TEXT PRIMARY KEY,
  auth_family TEXT NOT NULL,
  secret_ref TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('enabled', 'disabled')),
  created_at REAL NOT NULL,
  updated_at REAL NOT NULL,
  UNIQUE (auth_family, secret_ref)
);

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

CREATE TABLE quota_capacity_revision (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  revision INTEGER NOT NULL CHECK (revision >= 0),
  updated_at REAL NOT NULL
);
INSERT INTO quota_capacity_revision VALUES (1, 0, 0.0);

CREATE TABLE attempt_quota_keys (
  attempt_id TEXT NOT NULL REFERENCES attempts(id),
  quota_key TEXT NOT NULL,
  PRIMARY KEY (attempt_id, quota_key)
);
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

CREATE TABLE workflow_runs (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  script_sha TEXT NOT NULL,
  status TEXT NOT NULL CHECK (
    status IN (
      'created', 'running', 'succeeded', 'failed', 'cancelled', 'lost'
    )
  ),
  owner_pid_identity TEXT,
  created_at REAL NOT NULL,
  finished_at REAL,
  plan_json TEXT,
  result_json TEXT,
  orchestrator_session_id TEXT REFERENCES orchestrator_sessions(id)
, owner_birth_time REAL);

CREATE TABLE workflow_deliveries (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES workflow_runs(id),
  orchestrator_session_id TEXT NOT NULL REFERENCES orchestrator_sessions(id),
  state TEXT NOT NULL CHECK (
    state IN ('pending', 'sending', 'delivered', 'retry_wait', 'failed', 'cancelled')
  ),
  attempts INTEGER NOT NULL DEFAULT 0,
  lease_owner TEXT,
  lease_until REAL,
  next_attempt_at REAL,
  remote_message_id TEXT,
  last_error TEXT,
  ambiguous_result INTEGER NOT NULL DEFAULT 0 CHECK (ambiguous_result IN (0, 1)),
  attempt_generation INTEGER NOT NULL DEFAULT 1 CHECK (attempt_generation >= 1),
  run_status TEXT NOT NULL CHECK (
    run_status IN ('succeeded', 'failed', 'cancelled', 'lost')
  ),
  result_json TEXT,
  UNIQUE (run_id, attempt_generation)
);

CREATE INDEX workflow_deliveries_due
  ON workflow_deliveries(state, next_attempt_at, lease_until);

CREATE TABLE workflow_steps (
  run_id TEXT NOT NULL REFERENCES workflow_runs(id),
  step_key TEXT NOT NULL,
  spec_json TEXT NOT NULL,
  agent_id TEXT REFERENCES agents(id),
  status TEXT NOT NULL CHECK (
    status IN (
      'pending', 'running', 'succeeded', 'failed', 'skipped', 'cached'
    )
  ),
  result_json TEXT,
  failure_kind TEXT,
  failure_params_json TEXT,
  PRIMARY KEY (run_id, step_key)
);

CREATE TABLE run_stats (
  agent_id TEXT PRIMARY KEY REFERENCES agents(id),
  runtime TEXT NOT NULL,
  model TEXT NOT NULL,
  profile TEXT NOT NULL,
  status TEXT NOT NULL,
  failure_kind TEXT,
  started_at REAL,
  finished_at REAL,
  duration_seconds REAL,
  input_tokens INTEGER,
  output_tokens INTEGER,
  cache_read_tokens INTEGER,
  cache_write_tokens INTEGER,
  reasoning_tokens INTEGER,
  total_tokens INTEGER,
  num_turns INTEGER,
  ttft_ms REAL,
  api_duration_ms REAL,
  cost_usd REAL,
  usage_source TEXT NOT NULL CHECK (
    usage_source IN ('runtime_result', 'token_usage_updated', 'none')
  ),
  recorded_at REAL NOT NULL
);

CREATE INDEX idx_agents_active
  ON agents(status, created_at, id)
  WHERE status IN ('created', 'starting', 'running', 'cancelling');
CREATE INDEX idx_events_agent_seq ON events(agent_id, seq);
CREATE INDEX idx_messages_agent_seq ON messages(agent_id, seq);
CREATE INDEX idx_attempts_selected_account
  ON attempts(selected_account_id) WHERE selected_account_id IS NOT NULL;
CREATE INDEX idx_attempt_quota_keys_key ON attempt_quota_keys(quota_key);
CREATE UNIQUE INDEX idx_attempts_one_active
  ON attempts(agent_id) WHERE ownership_active = 1;
CREATE INDEX idx_commands_due ON commands(agent_id, state, id);
CREATE INDEX idx_deliveries_due
  ON deliveries(state, next_attempt_at, lease_until, id);
CREATE INDEX idx_capacity_lane_window_source_reset
  ON capacity_samples(lane, window, source, reset_at, observed_at);
CREATE INDEX idx_capacity_samples_account_key
  ON capacity_samples(account_id, quota_key) WHERE account_id IS NOT NULL;
CREATE INDEX idx_workflow_runs_active
  ON workflow_runs(status, created_at, id)
  WHERE status IN ('created', 'running');
CREATE INDEX idx_workflow_steps_agent
  ON workflow_steps(agent_id)
  WHERE agent_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS capacity_route_snapshots (
    runtime TEXT NOT NULL,
    scope_id TEXT NOT NULL,
    observed_at REAL NOT NULL,
    valid_until REAL NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (runtime, scope_id),
    CHECK (length(CAST(payload_json AS BLOB)) <= 65536)
);

CREATE TABLE managed_service_generations (
    id TEXT PRIMARY KEY,
    service_id TEXT NOT NULL,
    revision TEXT NOT NULL CHECK(length(revision) = 64),
    definition_json TEXT NOT NULL CHECK(json_valid(definition_json) AND length(CAST(definition_json AS BLOB)) <= 1048576),
    state TEXT NOT NULL CHECK(state IN ('starting','ready','unhealthy','stopping','stopped','unknown')),
    broker_identity_json TEXT NOT NULL CHECK(json_valid(broker_identity_json)),
    process_identity_json TEXT CHECK(process_identity_json IS NULL OR json_valid(process_identity_json)),
    created_at REAL NOT NULL,
    ready_at REAL,
    checked_at REAL,
    idle_since REAL,
    failure_kind TEXT CHECK(failure_kind IS NULL OR length(failure_kind) <= 64),
    cleanup_json TEXT CHECK(cleanup_json IS NULL OR json_valid(cleanup_json))
, ownership TEXT NOT NULL DEFAULT 'managed' CHECK(ownership IN ('managed','external')));
CREATE UNIQUE INDEX idx_managed_service_live
    ON managed_service_generations(service_id) WHERE state != 'stopped';

CREATE TABLE agent_service_gates (
    agent_id TEXT PRIMARY KEY REFERENCES agents(id),
    state TEXT NOT NULL CHECK(state IN ('pending','ready','failed')),
    failure_kind TEXT CHECK(failure_kind IS NULL OR length(failure_kind) <= 64)
);
CREATE INDEX idx_agent_service_gates_pending ON agent_service_gates(agent_id) WHERE state='pending';
CREATE TABLE managed_service_leases (
    generation_id TEXT NOT NULL REFERENCES managed_service_generations(id),
    agent_id TEXT NOT NULL REFERENCES agents(id),
    acquired_at REAL NOT NULL,
    released_at REAL,
    PRIMARY KEY(generation_id, agent_id)
);
CREATE INDEX idx_managed_service_lease_active
    ON managed_service_leases(generation_id, agent_id) WHERE released_at IS NULL;

CREATE TABLE managed_service_probes (
    id TEXT PRIMARY KEY,
    generation_id TEXT NOT NULL REFERENCES managed_service_generations(id),
    created_at REAL NOT NULL
);

CREATE TABLE process_ownership (
    owner_kind TEXT NOT NULL CHECK(owner_kind IN ('attempt','service','probe')),
    owner_id TEXT NOT NULL,
    leader_json TEXT NOT NULL CHECK(json_valid(leader_json)),
    descendants_observed INTEGER NOT NULL CHECK(descendants_observed IN (0,1)),
    updated_at REAL NOT NULL,
    PRIMARY KEY(owner_kind, owner_id)
);
CREATE TABLE process_members (
    owner_kind TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    pid INTEGER NOT NULL CHECK(pid > 1),
    token TEXT NOT NULL CHECK(length(token) > 0),
    identity_json TEXT NOT NULL CHECK(json_valid(identity_json)),
    PRIMARY KEY(pid, token),
    FOREIGN KEY(owner_kind, owner_id) REFERENCES process_ownership(owner_kind, owner_id)
);
CREATE INDEX idx_process_members_owner ON process_members(owner_kind, owner_id);

CREATE INDEX idx_events_attempt ON events(attempt_id) WHERE attempt_id IS NOT NULL;
CREATE INDEX idx_messages_attempt ON messages(attempt_id) WHERE attempt_id IS NOT NULL;
CREATE INDEX idx_deliveries_terminal_event ON deliveries(terminal_event_seq) WHERE terminal_event_seq IS NOT NULL;

PRAGMA user_version = 20;
