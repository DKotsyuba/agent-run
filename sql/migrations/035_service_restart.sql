-- Old maintenance history has no automatic conversion. Refuse to discard it.
CREATE TEMP TABLE migration_035_maintenance_guard (id INTEGER);
CREATE TEMP TRIGGER migration_035_maintenance_guard_refuse
BEFORE INSERT ON migration_035_maintenance_guard
WHEN EXISTS (SELECT 1 FROM maintenance_fences)
  OR EXISTS (SELECT 1 FROM maintenance_cohort)
  OR EXISTS (SELECT 1 FROM maintenance_recipients)
  OR EXISTS (SELECT 1 FROM maintenance_checkpoints)
  OR EXISTS (SELECT 1 FROM maintenance_parks)
  OR EXISTS (SELECT 1 FROM maintenance_resumes)
BEGIN
  SELECT RAISE(ABORT, 'maintenance history exists; preserve the database and contact the owner before upgrading to v35');
END;
INSERT INTO migration_035_maintenance_guard VALUES (1);
DROP TABLE migration_035_maintenance_guard;

DROP TABLE maintenance_resumes;
DROP TABLE maintenance_parks;
DROP TABLE maintenance_checkpoints;
DROP TABLE maintenance_recipients;
DROP TABLE maintenance_cohort;
DROP TABLE maintenance_fences;

-- Service notices have no agent; keep all existing delivery rows and references.
CREATE TABLE deliveries_v35 (
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
INSERT INTO deliveries_v35 SELECT * FROM deliveries;
DROP TABLE deliveries;
ALTER TABLE deliveries_v35 RENAME TO deliveries;
CREATE INDEX idx_deliveries_due ON deliveries(state, next_attempt_at, lease_until, id);
CREATE INDEX idx_deliveries_terminal_event ON deliveries(terminal_event_seq) WHERE terminal_event_seq IS NOT NULL;

-- Persist the current restart, its captured agents and affected chat sessions.
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
