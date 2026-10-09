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
  supervisor_pid INTEGER,
  supervisor_identity TEXT,
  process_group_id INTEGER,
  heartbeat_at REAL,
  runtime_session_id TEXT,
  config_revision TEXT NOT NULL,
  exit_code INTEGER,
  failure_kind TEXT,
  failure_text TEXT,
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
  display_name TEXT,
  pool_membership_ever INTEGER DEFAULT 0 CHECK(pool_membership_ever IN (0,1)),
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
  , error INTEGER CHECK (error IN (0,1)),
  error_source TEXT,
  content_complete INTEGER CHECK (content_complete IN (0,1)),
  root_agent_id TEXT NOT NULL DEFAULT '');

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

CREATE TABLE worker_capabilities (
  attempt_id TEXT PRIMARY KEY REFERENCES attempts(id) ON DELETE CASCADE,
  token_sha256 TEXT NOT NULL CHECK (length(token_sha256) = 64),
  created_at REAL NOT NULL
, pool_catalog_version INTEGER, pool_catalog_digest TEXT
  CHECK (pool_catalog_digest IS NULL OR length(pool_catalog_digest)=64), pool_catalog_observed_at REAL);

CREATE TABLE worker_notifications (
  delivery_id TEXT PRIMARY KEY REFERENCES deliveries(id) ON DELETE CASCADE,
  agent_id TEXT NOT NULL REFERENCES agents(id),
  attempt_id TEXT NOT NULL REFERENCES attempts(id),
  request_id TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('notice','risk','question','blocker')),
  message TEXT NOT NULL CHECK (length(CAST(message AS BLOB)) BETWEEN 1 AND 2048),
  created_at REAL NOT NULL,
  UNIQUE (agent_id, request_id)
);
CREATE INDEX idx_worker_notifications_agent_time ON worker_notifications(agent_id, created_at);

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

CREATE TABLE runtime_storage_layouts (
    runtime_home TEXT PRIMARY KEY
        CHECK (length(runtime_home) > 1 AND runtime_home LIKE '/%'),
    index_sha256 TEXT NOT NULL CHECK (length(index_sha256) = 64),
    layout_json TEXT NOT NULL
        CHECK (json_valid(layout_json) AND length(CAST(layout_json AS BLOB)) <= 65536),
    layout_sha256 TEXT NOT NULL CHECK (length(layout_sha256) = 64),
    state TEXT NOT NULL CHECK (state IN ('prepared','committed')),
    operation_token TEXT NOT NULL CHECK (length(operation_token) BETWEEN 16 AND 64),
    owner_agent_id TEXT,
    updated_at REAL NOT NULL
);
CREATE INDEX idx_runtime_storage_layouts_pending
    ON runtime_storage_layouts(updated_at) WHERE state='prepared';

CREATE INDEX idx_events_attempt ON events(attempt_id) WHERE attempt_id IS NOT NULL;
CREATE INDEX idx_messages_attempt ON messages(attempt_id) WHERE attempt_id IS NOT NULL;
CREATE INDEX idx_deliveries_terminal_event ON deliveries(terminal_event_seq) WHERE terminal_event_seq IS NOT NULL;

CREATE INDEX idx_messages_root_seq ON messages(root_agent_id,seq);

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

PRAGMA user_version = 28;

CREATE TABLE pool_enrollments (
  agent_id TEXT PRIMARY KEY REFERENCES pool_members(agent_id) ON DELETE CASCADE,
  run_id TEXT NOT NULL REFERENCES agents(id),
  attempt_id TEXT NOT NULL REFERENCES attempts(id),
  challenge TEXT NOT NULL UNIQUE CHECK(length(challenge) BETWEEN 1 AND 128),
  state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','joined','needs_action')),
  created_at REAL NOT NULL,
  ack_seq INTEGER REFERENCES pool_entries(seq),
  ack_at REAL,
  failure_reason TEXT,
  attention_delivery_id TEXT REFERENCES deliveries(id) ON DELETE SET NULL,
  attention_issued INTEGER NOT NULL DEFAULT 0 CHECK(attention_issued IN (0,1)),
  CHECK (state<>'joined' OR (ack_seq IS NOT NULL AND ack_at IS NOT NULL))
);
CREATE INDEX idx_pool_enrollments_pending ON pool_enrollments(state);
CREATE TRIGGER pool_enrollment_identity_immutable BEFORE UPDATE ON pool_enrollments
WHEN NEW.agent_id IS NOT OLD.agent_id OR NEW.run_id IS NOT OLD.run_id
  OR NEW.attempt_id IS NOT OLD.attempt_id OR NEW.challenge IS NOT OLD.challenge
  OR NEW.created_at IS NOT OLD.created_at
  OR (OLD.state='joined' AND NEW.state<>'joined')
  OR (OLD.attention_issued=1 AND NEW.attention_issued<>1)
BEGIN SELECT RAISE(ABORT,'pool enrollment identity is immutable'); END;

CREATE TRIGGER pool_members_remember_history AFTER INSERT ON pool_members
BEGIN UPDATE agents SET pool_membership_ever=1 WHERE id=NEW.agent_id; END;
CREATE TRIGGER agents_pool_history_immutable BEFORE UPDATE OF pool_membership_ever ON agents
WHEN OLD.pool_membership_ever=1 AND NEW.pool_membership_ever IS NOT 1
BEGIN SELECT RAISE(ABORT,'pool membership history is immutable'); END;
