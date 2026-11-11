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
  agent_id TEXT REFERENCES agents(id),
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

-- Frozen Bridge publication ownership, independent of native delivery proof.
CREATE TABLE bridge_delivery_intents (
  delivery_id TEXT PRIMARY KEY REFERENCES deliveries(id) ON DELETE CASCADE,
  intent_json TEXT CHECK(intent_json IS NULL OR json_valid(intent_json)),
  custody_receipt_json TEXT CHECK(custody_receipt_json IS NULL OR json_valid(custody_receipt_json)),
  custody_state TEXT NOT NULL DEFAULT 'pending'
    CHECK(custody_state IN ('pending','custody_accepted','outcome_unknown'))
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
CREATE INDEX idx_events_agent_kind_seq ON events(agent_id, kind, seq);
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
-- cascading foreign key. phase distinguishes immutable lifecycle observations;
-- only the projection_proof row may be refreshed, on its own key.
-- occurred_at is their finite Unix timestamp; details_json accepts only the
-- reviewed diagnostic projection, bounded to four KiB of UTF-8 bytes.
CREATE TABLE incident_ledger (
  execution_id TEXT NOT NULL CHECK (execution_id LIKE 'ag-%' AND length(execution_id) <= 64),
  phase TEXT NOT NULL CHECK (phase IN ('terminal','execution_failure','cleanup','delivery_waiting_binding','delivery_pending','delivery_sending','delivery_delivered','delivery_retry_wait','delivery_failed','delivery_cancelled','delivery_expired','projection_proof')),
  occurred_at REAL NOT NULL CHECK (occurred_at >= 0),
  details_json TEXT NOT NULL CHECK (json_valid(details_json) AND json_type(details_json)='object' AND length(CAST(details_json AS BLOB)) <= 4096),
  PRIMARY KEY (execution_id, phase)
);
CREATE INDEX idx_incident_ledger_time ON incident_ledger(occurred_at, execution_id, phase);
CREATE TRIGGER incident_ledger_immutable BEFORE UPDATE ON incident_ledger
WHEN OLD.phase <> 'projection_proof' OR NEW.phase <> 'projection_proof'
  OR NEW.execution_id IS NOT OLD.execution_id
BEGIN SELECT RAISE(ABORT,'incident ledger observations are immutable'); END;

PRAGMA user_version = 29;

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
CREATE INDEX operation_receipts_producer ON operation_receipts(producer_key);
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

-- Binding cooldowns are separate from quota and survive history retention.
CREATE TABLE binding_cooldowns (
  provider_id TEXT NOT NULL CHECK (length(trim(provider_id)) > 0),
  account_id TEXT NOT NULL REFERENCES provider_accounts(account_id),
  reason TEXT NOT NULL CHECK (reason IN ('billing_unavailable', 'quota_exhausted')),
  observed_at REAL NOT NULL CHECK (observed_at >= 0),
  blocked_until REAL NOT NULL CHECK (blocked_until > observed_at),
  attempt_id TEXT NOT NULL CHECK (length(trim(attempt_id)) > 0),
  PRIMARY KEY (account_id, provider_id)
);
CREATE INDEX idx_binding_cooldowns_expiry ON binding_cooldowns(blocked_until);

-- Restart history is retained with its agents and any unsent notifications.
CREATE TABLE service_restarts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  request_id TEXT NOT NULL UNIQUE,
  phase TEXT NOT NULL CHECK (phase IN ('preparing','ready','requesting','recovering','complete')),
  started_at REAL NOT NULL,
  boot_id TEXT NOT NULL,
  notice_wait_started_ns INTEGER,
  writer_epoch INTEGER,
  finished_at REAL,
  CHECK ((phase='complete') = (finished_at IS NOT NULL))
);
CREATE UNIQUE INDEX service_restart_one_active ON service_restarts((1)) WHERE finished_at IS NULL;

CREATE TABLE service_restart_agents (
  restart_id INTEGER NOT NULL REFERENCES service_restarts(id) ON DELETE CASCADE,
  agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
  orchestrator_session_id TEXT REFERENCES orchestrator_sessions(id) ON DELETE SET NULL,
  cancelled_before_restart INTEGER NOT NULL DEFAULT 0 CHECK (cancelled_before_restart IN (0,1)),
  first_notice_ns INTEGER,
  second_notice_ns INTEGER,
  force_requested_ns INTEGER,
  resume_requested_ns INTEGER,
  resumed_execution_id TEXT REFERENCES agents(id) ON DELETE SET NULL,
  resume_status TEXT NOT NULL DEFAULT 'pending' CHECK (resume_status IN ('pending','started','failed','unconfirmed','cancelled','finished')),
  resume_error TEXT,
  PRIMARY KEY (restart_id,agent_id)
);
CREATE INDEX service_restart_agents_agent ON service_restart_agents(agent_id);
CREATE INDEX service_restart_agents_child ON service_restart_agents(resumed_execution_id) WHERE resumed_execution_id IS NOT NULL;

CREATE TABLE service_restart_recipients (
  restart_id INTEGER NOT NULL REFERENCES service_restarts(id) ON DELETE CASCADE,
  transport TEXT NOT NULL,
  external_session_id TEXT NOT NULL,
  had_agents INTEGER NOT NULL DEFAULT 0 CHECK (had_agents IN (0,1)),
  tried_start INTEGER NOT NULL DEFAULT 0 CHECK (tried_start IN (0,1)),
  PRIMARY KEY (restart_id,transport,external_session_id)
);

CREATE TABLE service_restart_notices (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  restart_id INTEGER NOT NULL,
  transport TEXT NOT NULL,
  external_session_id TEXT NOT NULL,
  stage TEXT NOT NULL CHECK (stage IN ('before','after')),
  message TEXT NOT NULL,
  delivery_id TEXT NOT NULL UNIQUE REFERENCES deliveries(id) ON DELETE CASCADE,
  UNIQUE (restart_id,transport,external_session_id,stage),
  FOREIGN KEY (restart_id,transport,external_session_id)
    REFERENCES service_restart_recipients(restart_id,transport,external_session_id) ON DELETE CASCADE
);

-- The identity belongs to this database and is minted only when it is created or upgraded.
CREATE TABLE store_identity (
  singleton INTEGER PRIMARY KEY CHECK (singleton=1),
  store_id TEXT NOT NULL CHECK (length(store_id)=38 AND substr(store_id,1,6)='store_' AND substr(store_id,7) NOT GLOB '*[^0-9a-f]*')
);
INSERT INTO store_identity(singleton,store_id) VALUES (1,'store_' || lower(hex(randomblob(16))));
CREATE TRIGGER store_identity_immutable BEFORE UPDATE ON store_identity
BEGIN SELECT RAISE(ABORT,'store identity is immutable'); END;
CREATE TRIGGER store_identity_retained BEFORE DELETE ON store_identity
BEGIN SELECT RAISE(ABORT,'store identity is retained'); END;

-- Batch receipts survive agent history retention; identifiers are not FKs.
CREATE TABLE start_batch_receipts (
  request_id TEXT PRIMARY KEY NOT NULL,
  fingerprint_version INTEGER NOT NULL CHECK (fingerprint_version > 0),
  fingerprint TEXT NOT NULL CHECK (length(fingerprint) = 64 AND fingerprint NOT GLOB '*[^0-9a-f]*'),
  receipt_json TEXT NOT NULL CHECK (json_valid(receipt_json) AND length(CAST(receipt_json AS BLOB)) <= 65536)
);
CREATE TRIGGER start_batch_receipt_immutable BEFORE UPDATE ON start_batch_receipts
BEGIN SELECT RAISE(ABORT,'batch receipt is immutable'); END;
CREATE TRIGGER start_batch_receipt_retained BEFORE DELETE ON start_batch_receipts
BEGIN SELECT RAISE(ABORT,'batch receipt is retained'); END;

-- The root is deliberately not an FK: released tombstones outlive history.
CREATE TABLE history_pins (
  pin_id TEXT PRIMARY KEY NOT NULL CHECK (length(pin_id)=36 AND substr(pin_id,1,4)='pin_' AND substr(pin_id,5) NOT GLOB '*[^0-9a-f]*'),
  root_agent_id TEXT NOT NULL,
  released INTEGER NOT NULL DEFAULT 0 CHECK (released IN (0,1))
);
CREATE INDEX idx_history_pins_root_active ON history_pins(root_agent_id,released);
CREATE TRIGGER history_pin_immutable BEFORE UPDATE ON history_pins
WHEN NEW.pin_id IS NOT OLD.pin_id OR NEW.root_agent_id IS NOT OLD.root_agent_id OR OLD.released!=0 OR NEW.released!=1
BEGIN SELECT RAISE(ABORT,'history pin is immutable except release'); END;
CREATE TRIGGER history_pin_retained BEFORE DELETE ON history_pins
BEGIN SELECT RAISE(ABORT,'history pin tombstone is retained'); END;

-- Permanent receipts and release tombstones; execution roots deliberately are not FKs.
CREATE TABLE lineage_holds (
  hold_id TEXT PRIMARY KEY NOT NULL CHECK (length(hold_id)=37 AND substr(hold_id,1,5)='hold_' AND substr(hold_id,6) NOT GLOB '*[^0-9a-f]*'),
  request_id TEXT NOT NULL UNIQUE,
  version INTEGER NOT NULL CHECK (version=1),
  fingerprint BLOB NOT NULL CHECK (typeof(fingerprint)='blob' AND length(fingerprint)=32),
  verifying_key BLOB NOT NULL CHECK (typeof(verifying_key)='blob' AND length(verifying_key)=32),
  intent_digest BLOB CHECK (intent_digest IS NULL OR (typeof(intent_digest)='blob' AND length(intent_digest)=32)),
  target_count INTEGER NOT NULL CHECK (target_count BETWEEN 1 AND 20),
  released INTEGER NOT NULL DEFAULT 0 CHECK (released IN (0,1))
);
CREATE INDEX idx_lineage_holds_state_id ON lineage_holds(released,hold_id);
CREATE TRIGGER lineage_hold_no_replace BEFORE INSERT ON lineage_holds
WHEN EXISTS (SELECT 1 FROM lineage_holds WHERE rowid=NEW.rowid OR hold_id=NEW.hold_id OR request_id=NEW.request_id)
BEGIN SELECT RAISE(ABORT,'lineage hold receipt cannot be replaced'); END;
CREATE TRIGGER lineage_hold_immutable BEFORE UPDATE ON lineage_holds
WHEN NEW.rowid IS NOT OLD.rowid OR NEW.hold_id IS NOT OLD.hold_id OR NEW.request_id IS NOT OLD.request_id
  OR NEW.version IS NOT OLD.version OR NEW.fingerprint IS NOT OLD.fingerprint
  OR NEW.verifying_key IS NOT OLD.verifying_key OR NEW.intent_digest IS NOT OLD.intent_digest
  OR NEW.target_count IS NOT OLD.target_count OR OLD.released!=0 OR NEW.released!=1
BEGIN SELECT RAISE(ABORT,'lineage hold is immutable except release'); END;
CREATE TRIGGER lineage_hold_retained BEFORE DELETE ON lineage_holds
BEGIN SELECT RAISE(ABORT,'lineage hold tombstone is retained'); END;

CREATE TABLE lineage_hold_targets (
  hold_id TEXT NOT NULL REFERENCES lineage_holds(hold_id),
  ordinal INTEGER NOT NULL CHECK (ordinal BETWEEN 0 AND 19),
  agent_id TEXT NOT NULL,
  sequence INTEGER NOT NULL CHECK (sequence BETWEEN 1 AND 4294967295),
  execution_ref TEXT NOT NULL,
  pin_id TEXT NOT NULL REFERENCES history_pins(pin_id),
  root_agent_id TEXT NOT NULL,
  PRIMARY KEY (hold_id,ordinal),
  UNIQUE (hold_id,root_agent_id)
);
CREATE INDEX idx_lineage_hold_targets_root ON lineage_hold_targets(root_agent_id,hold_id);
CREATE INDEX idx_lineage_hold_targets_pin ON lineage_hold_targets(pin_id,hold_id);
CREATE TRIGGER lineage_hold_target_insert BEFORE INSERT ON lineage_hold_targets
WHEN EXISTS (SELECT 1 FROM lineage_hold_targets WHERE rowid=NEW.rowid OR (hold_id=NEW.hold_id AND (ordinal=NEW.ordinal OR root_agent_id=NEW.root_agent_id)))
  OR NOT EXISTS (SELECT 1 FROM lineage_holds h JOIN history_pins p ON p.pin_id=NEW.pin_id
  WHERE h.hold_id=NEW.hold_id AND h.released=0 AND NEW.ordinal<h.target_count
    AND p.released=0 AND p.root_agent_id=NEW.root_agent_id)
BEGIN SELECT RAISE(ABORT,'invalid lineage hold membership'); END;
CREATE TRIGGER lineage_hold_target_immutable BEFORE UPDATE ON lineage_hold_targets
BEGIN SELECT RAISE(ABORT,'lineage hold membership is immutable'); END;
CREATE TRIGGER lineage_hold_target_retained BEFORE DELETE ON lineage_hold_targets
BEGIN SELECT RAISE(ABORT,'lineage hold membership is retained'); END;

-- A pin remains independently active after release, and one hold cannot release another.
-- REPLACE can delete a conflicting row without DELETE triggers; inspect every unique key,
-- including rowid, before the insertion/update rather than relying on recursive_triggers.
CREATE TRIGGER history_pin_no_replace BEFORE INSERT ON history_pins
WHEN EXISTS (SELECT 1 FROM history_pins WHERE pin_id=NEW.pin_id OR rowid=NEW.rowid)
BEGIN SELECT RAISE(ABORT,'history pin cannot be replaced'); END;
CREATE TRIGGER history_pin_rowid_immutable BEFORE UPDATE ON history_pins
WHEN NEW.rowid IS NOT OLD.rowid
BEGIN SELECT RAISE(ABORT,'history pin rowid is immutable'); END;
CREATE TRIGGER held_history_pin_release BEFORE UPDATE ON history_pins
WHEN EXISTS (SELECT 1 FROM lineage_hold_targets t JOIN lineage_holds h USING(hold_id)
  WHERE t.pin_id=OLD.pin_id AND h.released=0)
BEGIN SELECT RAISE(ABORT,'history pin has an active lineage hold'); END;
CREATE TRIGGER store_identity_no_replace BEFORE INSERT ON store_identity
WHEN EXISTS (SELECT 1 FROM store_identity WHERE singleton=NEW.singleton)
BEGIN SELECT RAISE(ABORT,'store identity cannot be replaced'); END;

-- Every admission path, including legacy insertion, shares the same database guard.
CREATE TRIGGER held_agent_insert BEFORE INSERT ON agents
WHEN EXISTS (SELECT 1 FROM lineage_hold_targets t JOIN lineage_holds h USING(hold_id)
  WHERE h.released=0 AND (t.root_agent_id=COALESCE(NULLIF(NEW.root_agent_id,''),NEW.id)
    OR t.root_agent_id=(SELECT COALESCE(NULLIF(root_agent_id,''),id) FROM agents WHERE id=NEW.parent_agent_id)))
  OR EXISTS (SELECT 1 FROM agents a JOIN lineage_hold_targets t ON t.root_agent_id=COALESCE(NULLIF(a.root_agent_id,''),a.id)
    JOIN lineage_holds h USING(hold_id) WHERE h.released=0 AND (a.rowid=NEW.rowid OR a.id=NEW.id
      OR a.parent_agent_id=NEW.parent_agent_id
      OR (a.orchestrator_session_id=NEW.orchestrator_session_id AND a.request_id=NEW.request_id)))
BEGIN SELECT RAISE(ABORT,'lineage has an active hold'); END;
CREATE TRIGGER held_agent_replace_update BEFORE UPDATE ON agents
WHEN EXISTS (SELECT 1 FROM agents a JOIN lineage_hold_targets t ON t.root_agent_id=COALESCE(NULLIF(a.root_agent_id,''),a.id)
  JOIN lineage_holds h USING(hold_id) WHERE h.released=0 AND a.rowid!=OLD.rowid
    AND (a.rowid=NEW.rowid OR a.id=NEW.id OR a.parent_agent_id=NEW.parent_agent_id
      OR (a.orchestrator_session_id=NEW.orchestrator_session_id AND a.request_id=NEW.request_id)))
BEGIN SELECT RAISE(ABORT,'lineage has an active hold'); END;
CREATE TRIGGER held_agent_proof_update BEFORE UPDATE OF id,root_agent_id,parent_agent_id,sequence,
  identity_json,runtime,model,profile,task,workdir,status,finished_at,exit_code,failure_kind,failure_text,
  answer_path,answer_bytes,answer_sha256 ON agents
WHEN EXISTS (SELECT 1 FROM lineage_hold_targets t JOIN lineage_holds h USING(hold_id)
  WHERE h.released=0 AND (t.root_agent_id=COALESCE(NULLIF(OLD.root_agent_id,''),OLD.id)
    OR t.root_agent_id=COALESCE(NULLIF(NEW.root_agent_id,''),NEW.id)
    OR t.root_agent_id=(SELECT COALESCE(NULLIF(root_agent_id,''),id) FROM agents WHERE id=NEW.parent_agent_id)))
BEGIN SELECT RAISE(ABORT,'lineage has an active hold'); END;
CREATE TRIGGER held_agent_delete BEFORE DELETE ON agents
WHEN EXISTS (SELECT 1 FROM lineage_hold_targets t JOIN lineage_holds h USING(hold_id)
  WHERE h.released=0 AND t.root_agent_id=COALESCE(NULLIF(OLD.root_agent_id,''),OLD.id))
BEGIN SELECT RAISE(ABORT,'lineage has an active hold'); END;

CREATE TRIGGER held_attempt_insert BEFORE INSERT ON attempts
WHEN EXISTS (SELECT 1 FROM agents a JOIN lineage_hold_targets t ON t.root_agent_id=COALESCE(NULLIF(a.root_agent_id,''),a.id)
  JOIN lineage_holds h USING(hold_id) WHERE h.released=0 AND (a.id=NEW.agent_id
    OR a.id IN (SELECT agent_id FROM attempts WHERE id=NEW.id OR rowid=NEW.rowid)))
BEGIN SELECT RAISE(ABORT,'lineage has an active hold'); END;
CREATE TRIGGER held_attempt_update BEFORE UPDATE ON attempts
WHEN EXISTS (SELECT 1 FROM agents a JOIN lineage_hold_targets t ON t.root_agent_id=COALESCE(NULLIF(a.root_agent_id,''),a.id)
  JOIN lineage_holds h USING(hold_id) WHERE h.released=0 AND (a.id IN (OLD.agent_id,NEW.agent_id)
    OR a.id IN (SELECT agent_id FROM attempts WHERE id=NEW.id OR rowid=NEW.rowid)))
BEGIN SELECT RAISE(ABORT,'lineage has an active hold'); END;
CREATE TRIGGER held_attempt_delete BEFORE DELETE ON attempts
WHEN EXISTS (SELECT 1 FROM agents a JOIN lineage_hold_targets t ON t.root_agent_id=COALESCE(NULLIF(a.root_agent_id,''),a.id)
  JOIN lineage_holds h USING(hold_id) WHERE a.id=OLD.agent_id AND h.released=0)
BEGIN SELECT RAISE(ABORT,'lineage has an active hold'); END;

CREATE TRIGGER held_status_event_insert BEFORE INSERT ON events
WHEN NEW.kind='status' AND EXISTS (
  SELECT 1 FROM agents a JOIN lineage_hold_targets t ON t.root_agent_id=COALESCE(NULLIF(a.root_agent_id,''),a.id)
  JOIN lineage_holds h USING(hold_id) WHERE a.id=NEW.agent_id AND h.released=0)
  OR EXISTS (SELECT 1 FROM events e JOIN agents a ON a.id=e.agent_id
    JOIN lineage_hold_targets t ON t.root_agent_id=COALESCE(NULLIF(a.root_agent_id,''),a.id)
    JOIN lineage_holds h USING(hold_id) WHERE e.seq=NEW.seq AND e.kind='status' AND h.released=0)
BEGIN SELECT RAISE(ABORT,'lineage has an active hold'); END;
CREATE TRIGGER held_status_event_update BEFORE UPDATE ON events
WHEN (OLD.kind='status' OR NEW.kind='status') AND EXISTS (
  SELECT 1 FROM agents a JOIN lineage_hold_targets t ON t.root_agent_id=COALESCE(NULLIF(a.root_agent_id,''),a.id)
  JOIN lineage_holds h USING(hold_id) WHERE a.id IN (OLD.agent_id,NEW.agent_id) AND h.released=0)
  OR EXISTS (SELECT 1 FROM events e JOIN agents a ON a.id=e.agent_id
    JOIN lineage_hold_targets t ON t.root_agent_id=COALESCE(NULLIF(a.root_agent_id,''),a.id)
    JOIN lineage_holds h USING(hold_id) WHERE e.seq=NEW.seq AND e.kind='status' AND h.released=0)
BEGIN SELECT RAISE(ABORT,'lineage has an active hold'); END;
CREATE TRIGGER held_status_event_delete BEFORE DELETE ON events
WHEN OLD.kind='status' AND EXISTS (
  SELECT 1 FROM agents a JOIN lineage_hold_targets t ON t.root_agent_id=COALESCE(NULLIF(a.root_agent_id,''),a.id)
  JOIN lineage_holds h USING(hold_id) WHERE a.id=OLD.agent_id AND h.released=0)
BEGIN SELECT RAISE(ABORT,'lineage has an active hold'); END;

CREATE TRIGGER held_restart_agent_insert BEFORE INSERT ON service_restart_agents
WHEN EXISTS (SELECT 1 FROM agents a JOIN lineage_hold_targets t ON t.root_agent_id=COALESCE(NULLIF(a.root_agent_id,''),a.id)
  JOIN lineage_holds h USING(hold_id) WHERE a.id IN (NEW.agent_id,NEW.resumed_execution_id) AND h.released=0)
  OR EXISTS (SELECT 1 FROM service_restart_agents r JOIN agents a ON a.id IN (r.agent_id,r.resumed_execution_id)
    JOIN lineage_hold_targets t ON t.root_agent_id=COALESCE(NULLIF(a.root_agent_id,''),a.id)
    JOIN lineage_holds h USING(hold_id) WHERE h.released=0
      AND (r.rowid=NEW.rowid OR (r.restart_id=NEW.restart_id AND r.agent_id=NEW.agent_id)))
BEGIN SELECT RAISE(ABORT,'lineage has an active hold'); END;
CREATE TRIGGER held_restart_agent_update BEFORE UPDATE ON service_restart_agents
WHEN EXISTS (SELECT 1 FROM agents a JOIN lineage_hold_targets t ON t.root_agent_id=COALESCE(NULLIF(a.root_agent_id,''),a.id)
  JOIN lineage_holds h USING(hold_id) WHERE a.id IN (OLD.agent_id,OLD.resumed_execution_id,NEW.agent_id,NEW.resumed_execution_id) AND h.released=0)
  OR EXISTS (SELECT 1 FROM service_restart_agents r JOIN agents a ON a.id IN (r.agent_id,r.resumed_execution_id)
    JOIN lineage_hold_targets t ON t.root_agent_id=COALESCE(NULLIF(a.root_agent_id,''),a.id)
    JOIN lineage_holds h USING(hold_id) WHERE h.released=0
      AND (r.rowid=NEW.rowid OR (r.restart_id=NEW.restart_id AND r.agent_id=NEW.agent_id)))
BEGIN SELECT RAISE(ABORT,'lineage has an active hold'); END;
CREATE TRIGGER held_restart_agent_delete BEFORE DELETE ON service_restart_agents
WHEN EXISTS (SELECT 1 FROM agents a JOIN lineage_hold_targets t ON t.root_agent_id=COALESCE(NULLIF(a.root_agent_id,''),a.id)
  JOIN lineage_holds h USING(hold_id) WHERE a.id IN (OLD.agent_id,OLD.resumed_execution_id) AND h.released=0)
BEGIN SELECT RAISE(ABORT,'lineage has an active hold'); END;

PRAGMA user_version = 42;

-- Launch correlation is never inferred for historical attempts. A spawning
-- row without a bound incarnation is unresolved until explicitly reconciled.
CREATE TABLE attempt_launches (
  spawn_key TEXT PRIMARY KEY NOT NULL,
  execution_id TEXT NOT NULL,
  sequence INTEGER NOT NULL CHECK (sequence > 0),
  attempt_id TEXT NOT NULL UNIQUE,
  launch_id TEXT NOT NULL UNIQUE CHECK (length(launch_id)=36),
  state TEXT NOT NULL CHECK (state IN ('prepared','spawning','bound','failed','unknown')),
  boot_id TEXT,
  pid_namespace_inode INTEGER,
  uid INTEGER,
  pid INTEGER,
  start_ticks INTEGER,
  CHECK ((state='bound' AND boot_id IS NOT NULL AND pid_namespace_inode > 0
          AND uid >= 0 AND pid > 1 AND start_ticks > 0)
      OR (state!='bound' AND boot_id IS NULL AND pid_namespace_inode IS NULL
          AND uid IS NULL AND pid IS NULL AND start_ticks IS NULL))
);

CREATE TRIGGER attempt_launch_identity_immutable BEFORE UPDATE ON attempt_launches
WHEN NEW.spawn_key IS NOT OLD.spawn_key OR NEW.execution_id IS NOT OLD.execution_id
  OR NEW.sequence IS NOT OLD.sequence OR NEW.attempt_id IS NOT OLD.attempt_id
  OR NEW.launch_id IS NOT OLD.launch_id OR OLD.state IN ('bound','failed')
  OR NOT ((OLD.state='prepared' AND NEW.state IN ('spawning','failed'))
       OR (OLD.state='spawning' AND NEW.state IN ('bound','failed','unknown'))
       OR (OLD.state='unknown' AND NEW.state IN ('bound','failed')))
BEGIN SELECT RAISE(ABORT, 'invalid attempt launch transition'); END;

-- Permanent v1 admission and abort records. Old attempts gain no authority.
-- Installed only by the owning supervisor after managed-role native checks.
CREATE TABLE attempt_role_qualifications (
  launch_id TEXT PRIMARY KEY NOT NULL,
  attempt_id TEXT NOT NULL UNIQUE,
  harness TEXT NOT NULL CHECK(harness IN ('codex','claude-code')),
  role_digest TEXT NOT NULL CHECK(length(role_digest)=64 AND role_digest NOT GLOB '*[^0-9a-f]*'),
  config_digest TEXT NOT NULL CHECK(length(config_digest)=64 AND config_digest NOT GLOB '*[^0-9a-f]*'),
  inventory_digest TEXT NOT NULL CHECK(length(inventory_digest)=64 AND inventory_digest NOT GLOB '*[^0-9a-f]*'),
  boot_id TEXT NOT NULL,
  pid_namespace_inode INTEGER NOT NULL CHECK(pid_namespace_inode>0),
  uid INTEGER NOT NULL CHECK(uid>=0),
  pid INTEGER NOT NULL CHECK(pid>1),
  start_ticks INTEGER NOT NULL CHECK(start_ticks>0)
);
CREATE TRIGGER attempt_role_qualification_immutable BEFORE UPDATE ON attempt_role_qualifications
BEGIN SELECT RAISE(ABORT, 'attempt qualification is immutable'); END;

-- Created before any Bridge lookup so namespace retirement sees in-flight work.
CREATE TABLE attempt_pending_checks (
  intent_id TEXT PRIMARY KEY NOT NULL,
  security_digest TEXT NOT NULL CHECK(length(security_digest)=64 AND security_digest NOT GLOB '*[^0-9a-f]*'),
  request_fingerprint TEXT NOT NULL CHECK(length(request_fingerprint)=64 AND request_fingerprint NOT GLOB '*[^0-9a-f]*'),
  pool_route_json TEXT NOT NULL CHECK(json_valid(pool_route_json)),
  bridge_route_json TEXT NOT NULL CHECK(json_valid(bridge_route_json)),
  producer TEXT NOT NULL,
  issuer_epoch TEXT NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('pending','resolved'))
);

CREATE TRIGGER attempt_pending_checks_immutable BEFORE UPDATE ON attempt_pending_checks
WHEN NEW.intent_id IS NOT OLD.intent_id OR NEW.security_digest IS NOT OLD.security_digest
  OR NEW.request_fingerprint IS NOT OLD.request_fingerprint OR NEW.pool_route_json IS NOT OLD.pool_route_json
  OR NEW.bridge_route_json IS NOT OLD.bridge_route_json OR NEW.producer IS NOT OLD.producer
  OR NEW.issuer_epoch IS NOT OLD.issuer_epoch OR OLD.state!='pending' OR NEW.state!='resolved'
BEGIN SELECT RAISE(ABORT, 'invalid attempt lookup transition'); END;

CREATE TABLE attempt_admissions (
  intent_id TEXT PRIMARY KEY NOT NULL,
  security_digest TEXT NOT NULL CHECK(length(security_digest)=64 AND security_digest NOT GLOB '*[^0-9a-f]*'),
  pool_route_json TEXT NOT NULL CHECK(json_valid(pool_route_json)),
  request_fingerprint TEXT CHECK(request_fingerprint IS NULL OR (length(request_fingerprint)=64 AND request_fingerprint NOT GLOB '*[^0-9a-f]*')),
  admission_id TEXT UNIQUE,
  admission_json TEXT CHECK(admission_json IS NULL OR json_valid(admission_json)),
  abort_request_digest TEXT CHECK(abort_request_digest IS NULL OR (length(abort_request_digest)=64 AND abort_request_digest NOT GLOB '*[^0-9a-f]*')),
  state TEXT NOT NULL CHECK(state IN ('admitted','aborted_before_admit','settled_committed','settled_aborted')),
  outcome_digest TEXT CHECK(outcome_digest IS NULL OR (length(outcome_digest)=64 AND outcome_digest NOT GLOB '*[^0-9a-f]*')),
  CHECK ((state='aborted_before_admit' AND admission_id IS NULL AND admission_json IS NULL AND request_fingerprint IS NULL AND outcome_digest IS NULL)
      OR (state='admitted' AND admission_id IS NOT NULL AND admission_json IS NOT NULL AND request_fingerprint IS NOT NULL AND outcome_digest IS NULL AND abort_request_digest IS NULL)
      OR (state IN ('settled_committed','settled_aborted') AND admission_id IS NOT NULL AND admission_json IS NOT NULL AND request_fingerprint IS NOT NULL AND outcome_digest IS NOT NULL AND abort_request_digest IS NULL))
);

CREATE TRIGGER attempt_admissions_immutable BEFORE UPDATE ON attempt_admissions
WHEN NEW.intent_id IS NOT OLD.intent_id OR NEW.security_digest IS NOT OLD.security_digest
  OR NEW.pool_route_json IS NOT OLD.pool_route_json OR NEW.request_fingerprint IS NOT OLD.request_fingerprint
  OR NEW.admission_id IS NOT OLD.admission_id OR NEW.admission_json IS NOT OLD.admission_json
  OR NEW.abort_request_digest IS NOT OLD.abort_request_digest
  OR OLD.state!='admitted' OR NEW.state NOT IN ('settled_committed','settled_aborted')
BEGIN SELECT RAISE(ABORT, 'invalid attempt admission transition'); END;

-- Stored request/receipt associations are immutable and cannot silently renew.
CREATE TABLE attempt_pending_current (
  request_id TEXT NOT NULL,
  pool_route_json TEXT NOT NULL CHECK(json_valid(pool_route_json)),
  bridge_route_json TEXT NOT NULL CHECK(json_valid(bridge_route_json)),
  producer TEXT NOT NULL,
  fingerprint TEXT NOT NULL CHECK(length(fingerprint)=64 AND fingerprint NOT GLOB '*[^0-9a-f]*'),
  state TEXT NOT NULL CHECK(state IN ('pending','resolved')),
  PRIMARY KEY(pool_route_json,request_id)
);
CREATE TRIGGER attempt_pending_current_immutable BEFORE UPDATE ON attempt_pending_current
WHEN NEW.request_id IS NOT OLD.request_id OR NEW.pool_route_json IS NOT OLD.pool_route_json
  OR NEW.bridge_route_json IS NOT OLD.bridge_route_json OR NEW.producer IS NOT OLD.producer
  OR NEW.fingerprint IS NOT OLD.fingerprint OR OLD.state!='pending' OR NEW.state!='resolved'
BEGIN SELECT RAISE(ABORT, 'invalid attempt current lookup transition'); END;

CREATE TABLE attempt_current_receipts (
  request_id TEXT NOT NULL,
  pool_route_json TEXT NOT NULL CHECK(json_valid(pool_route_json)),
  fingerprint TEXT NOT NULL CHECK(length(fingerprint)=64 AND fingerprint NOT GLOB '*[^0-9a-f]*'),
  receipt_id TEXT NOT NULL UNIQUE,
  receipt_json TEXT NOT NULL CHECK(json_valid(receipt_json)),
  PRIMARY KEY(pool_route_json,request_id)
);

-- An intent is persisted before retirement I/O and remains a permanent fence.
CREATE TABLE attempt_namespace_retirements (
  retirement_key TEXT PRIMARY KEY NOT NULL,
  request_id TEXT NOT NULL UNIQUE,
  pool_seal_request_id TEXT NOT NULL UNIQUE,
  bridge_route_json TEXT NOT NULL CHECK(json_valid(bridge_route_json)),
  pool_route_json TEXT NOT NULL CHECK(json_valid(pool_route_json)),
  producer TEXT NOT NULL,
  issuer_epoch TEXT NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('prepared','fenced','invoking','retired')),
  pool_seal_id TEXT,
  retired_count INTEGER CHECK(retired_count IS NULL OR retired_count>=0),
  UNIQUE(bridge_route_json,producer,issuer_epoch),
  CHECK ((state='retired' AND retired_count IS NOT NULL AND pool_seal_id IS NOT NULL)
      OR (state='invoking' AND retired_count IS NULL AND pool_seal_id IS NOT NULL)
      OR (state='fenced' AND retired_count IS NULL)
      OR (state='prepared' AND retired_count IS NULL AND pool_seal_id IS NULL))
);

CREATE TRIGGER attempt_retirements_immutable BEFORE UPDATE ON attempt_namespace_retirements
WHEN NEW.retirement_key IS NOT OLD.retirement_key OR NEW.bridge_route_json IS NOT OLD.bridge_route_json
  OR NEW.request_id IS NOT OLD.request_id OR NEW.pool_seal_request_id IS NOT OLD.pool_seal_request_id
  OR NEW.pool_route_json IS NOT OLD.pool_route_json OR NEW.producer IS NOT OLD.producer
  OR NEW.issuer_epoch IS NOT OLD.issuer_epoch
  OR (OLD.pool_seal_id IS NOT NULL AND NEW.pool_seal_id IS NOT OLD.pool_seal_id)
  OR NOT ((OLD.state='prepared' AND NEW.state='fenced')
       OR (OLD.state='fenced' AND NEW.state='fenced' AND OLD.pool_seal_id IS NULL AND NEW.pool_seal_id IS NOT NULL)
       OR (OLD.state='fenced' AND NEW.state='invoking') OR (OLD.state='invoking' AND NEW.state='retired'))
BEGIN SELECT RAISE(ABORT, 'invalid namespace retirement transition'); END;

-- One immutable effect across dispatch IDs and process restarts. Text is never stored.
CREATE TABLE owner_effects (
  producer_scope TEXT NOT NULL,
  effect_id TEXT NOT NULL,
  effect_digest TEXT NOT NULL CHECK(length(effect_digest)=64 AND effect_digest NOT GLOB '*[^0-9a-f]*'),
  run_route_json TEXT NOT NULL CHECK(json_valid(run_route_json)),
  bridge_route_json TEXT NOT NULL CHECK(json_valid(bridge_route_json)),
  outbox_json TEXT CHECK(outbox_json IS NULL OR json_valid(outbox_json)),
  dispatch_token TEXT,
  state TEXT NOT NULL CHECK(state IN ('reserved','invoking','known_unsent','unknown','accepted','aborted')),
  protocol TEXT CHECK(protocol IS NULL OR protocol IN ('codex_turn','claude_input')),
  native_id TEXT,
  reason TEXT,
  CHECK ((state='accepted' AND outbox_json IS NOT NULL AND dispatch_token IS NOT NULL AND protocol IS NOT NULL AND native_id IS NOT NULL AND reason IS NULL)
      OR (state IN ('reserved','invoking') AND outbox_json IS NOT NULL AND dispatch_token IS NOT NULL AND protocol IS NULL AND native_id IS NULL AND reason IS NULL)
      OR (state IN ('known_unsent','unknown') AND outbox_json IS NOT NULL AND dispatch_token IS NOT NULL AND protocol IS NULL AND native_id IS NULL AND reason IS NOT NULL)
      OR (state='aborted' AND protocol IS NULL AND native_id IS NULL AND reason IS NULL)),
  PRIMARY KEY(producer_scope,effect_id)
);

CREATE TRIGGER owner_effects_immutable BEFORE UPDATE ON owner_effects
WHEN NEW.producer_scope IS NOT OLD.producer_scope OR NEW.effect_id IS NOT OLD.effect_id
  OR NEW.effect_digest IS NOT OLD.effect_digest OR NEW.outbox_json IS NOT OLD.outbox_json
  OR NEW.run_route_json IS NOT OLD.run_route_json OR NEW.bridge_route_json IS NOT OLD.bridge_route_json
  OR (NOT (OLD.state='known_unsent' AND NEW.state='reserved') AND NEW.dispatch_token IS NOT OLD.dispatch_token)
  OR (OLD.state='known_unsent' AND NEW.state='reserved' AND (NEW.dispatch_token IS NULL OR NEW.dispatch_token IS OLD.dispatch_token))
  OR NOT ((OLD.state='reserved' AND NEW.state IN ('invoking','known_unsent','aborted'))
       OR (OLD.state='invoking' AND NEW.state IN ('accepted','known_unsent','unknown'))
       OR (OLD.state='unknown' AND NEW.state='accepted')
       OR (OLD.state='known_unsent' AND NEW.state IN ('reserved','aborted')))
BEGIN SELECT RAISE(ABORT, 'invalid owner effect transition'); END;

-- New currentness lookups carry their nonsecret launch key for active-capacity
-- accounting. Historical rows remain intact and are not promoted to authority.
ALTER TABLE attempt_pending_current ADD COLUMN launch_id TEXT;
DROP TRIGGER attempt_pending_current_immutable;
CREATE TRIGGER attempt_pending_current_immutable BEFORE UPDATE ON attempt_pending_current
WHEN NEW.request_id IS NOT OLD.request_id OR NEW.pool_route_json IS NOT OLD.pool_route_json
  OR NEW.bridge_route_json IS NOT OLD.bridge_route_json OR NEW.producer IS NOT OLD.producer
  OR NEW.fingerprint IS NOT OLD.fingerprint OR NEW.launch_id IS NOT OLD.launch_id
  OR OLD.state!='pending' OR NEW.state!='resolved'
BEGIN SELECT RAISE(ABORT, 'invalid attempt current lookup transition'); END;

-- Permanent exact-key fences and historical provenance never disappear via
-- retention. Future compaction requires a separately reviewed namespace format.
CREATE TRIGGER attempt_launches_no_delete BEFORE DELETE ON attempt_launches BEGIN SELECT RAISE(ABORT, 'attempt launch is permanent'); END;
CREATE TRIGGER attempt_role_qualifications_no_delete BEFORE DELETE ON attempt_role_qualifications BEGIN SELECT RAISE(ABORT, 'attempt qualification is permanent'); END;
CREATE TRIGGER attempt_pending_checks_no_delete BEFORE DELETE ON attempt_pending_checks BEGIN SELECT RAISE(ABORT, 'attempt admission key is permanent'); END;
CREATE TRIGGER attempt_admissions_no_delete BEFORE DELETE ON attempt_admissions BEGIN SELECT RAISE(ABORT, 'attempt admission is permanent'); END;
CREATE TRIGGER attempt_pending_current_no_delete BEFORE DELETE ON attempt_pending_current BEGIN SELECT RAISE(ABORT, 'attempt current key is permanent'); END;
CREATE TRIGGER attempt_current_receipts_no_delete BEFORE DELETE ON attempt_current_receipts BEGIN SELECT RAISE(ABORT, 'attempt current receipt is permanent'); END;
CREATE TRIGGER attempt_namespace_retirements_no_delete BEFORE DELETE ON attempt_namespace_retirements BEGIN SELECT RAISE(ABORT, 'attempt namespace fence is permanent'); END;
CREATE TRIGGER owner_effects_no_delete BEFORE DELETE ON owner_effects BEGIN SELECT RAISE(ABORT, 'owner effect is permanent'); END;

-- Permanent Pool-resident enrollment keys. An abort is a rejecting tombstone;
-- an admission and all of its member ownership rows commit in one transaction.
-- Only a prepared key retains the start request body; the terminal transition
-- clears it while the fingerprint, pin and receipt or tombstone stay permanent.
CREATE TABLE managed_enrollments (
  pool_route_json TEXT NOT NULL CHECK(json_valid(pool_route_json)),
  pool_store_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  fingerprint_version INTEGER NOT NULL CHECK(fingerprint_version=1),
  fingerprint TEXT NOT NULL CHECK(length(fingerprint)=64 AND fingerprint NOT GLOB '*[^0-9a-f]*'),
  state TEXT NOT NULL CHECK(state IN ('prepared','admitted','aborted')),
  request_json TEXT CHECK(request_json IS NULL OR json_valid(request_json)),
  grant_json TEXT CHECK(grant_json IS NULL OR json_valid(grant_json)),
  receipt_id TEXT UNIQUE,
  receipt_json TEXT CHECK(receipt_json IS NULL OR json_valid(receipt_json)),
  CHECK ((state='admitted' AND request_json IS NULL AND grant_json IS NOT NULL AND receipt_id IS NOT NULL AND receipt_json IS NOT NULL)
      OR (state='prepared' AND request_json IS NOT NULL AND grant_json IS NOT NULL AND receipt_id IS NULL AND receipt_json IS NULL)
      OR (state='aborted' AND request_json IS NULL AND receipt_id IS NULL AND receipt_json IS NULL)),
  PRIMARY KEY(pool_route_json,pool_store_id,request_id),
  UNIQUE(pool_store_id,request_id)
);
CREATE INDEX managed_enrollments_prepared ON managed_enrollments(state) WHERE state='prepared';
CREATE TRIGGER managed_enrollments_immutable BEFORE UPDATE ON managed_enrollments
WHEN NEW.pool_route_json IS NOT OLD.pool_route_json OR NEW.pool_store_id IS NOT OLD.pool_store_id
  OR NEW.request_id IS NOT OLD.request_id OR NEW.fingerprint_version IS NOT OLD.fingerprint_version
  OR NEW.fingerprint IS NOT OLD.fingerprint OR NEW.request_json IS NOT NULL OR NEW.grant_json IS NOT OLD.grant_json
  OR OLD.state!='prepared' OR NEW.state NOT IN ('admitted','aborted')
BEGIN SELECT RAISE(ABORT, 'invalid managed enrollment transition'); END;
CREATE TRIGGER managed_enrollments_no_delete BEFORE DELETE ON managed_enrollments
BEGIN SELECT RAISE(ABORT, 'managed enrollment is permanent'); END;

-- Independent external ownership survives retention, release and Pool removal.
CREATE TABLE managed_enrollment_members (
  lineage_root_id TEXT PRIMARY KEY NOT NULL,
  pool_route_json TEXT NOT NULL,
  pool_store_id TEXT NOT NULL,
  pool_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  member_id TEXT NOT NULL,
  member_index INTEGER NOT NULL CHECK(member_index>=0 AND member_index<20),
  UNIQUE(pool_store_id,pool_id,member_id),
  UNIQUE(pool_route_json,pool_store_id,request_id,member_index),
  FOREIGN KEY(pool_route_json,pool_store_id,request_id)
    REFERENCES managed_enrollments(pool_route_json,pool_store_id,request_id)
);
CREATE TRIGGER managed_enrollment_members_no_update BEFORE UPDATE ON managed_enrollment_members
BEGIN SELECT RAISE(ABORT, 'managed ownership is immutable'); END;
CREATE TRIGGER managed_enrollment_members_no_delete BEFORE DELETE ON managed_enrollment_members
BEGIN SELECT RAISE(ABORT, 'managed ownership is permanent'); END;
