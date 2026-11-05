-- Admission-only maintenance boundary. No lifecycle or process state changes.
CREATE TABLE maintenance_fences (
  epoch INTEGER PRIMARY KEY AUTOINCREMENT,
  request_id TEXT NOT NULL UNIQUE CHECK(length(request_id) BETWEEN 1 AND 256),
  created_at REAL NOT NULL CHECK(created_at>=0),
  ended_at REAL CHECK(ended_at IS NULL OR ended_at>=created_at),
  member_count INTEGER NOT NULL CHECK(member_count>=0),
  phase TEXT NOT NULL DEFAULT 'preparing' CHECK(phase IN ('preparing','ready','stopping','recovering','deferring','complete')),
  origin_writer_epoch INTEGER CHECK(origin_writer_epoch IS NULL OR origin_writer_epoch>0),
  ready_writer_epoch INTEGER CHECK(ready_writer_epoch IS NULL OR ready_writer_epoch>0),
  outcome TEXT CHECK(outcome IS NULL OR outcome IN ('completed','deferred')),
  CHECK((phase='complete')=(ended_at IS NOT NULL)),
  CHECK((phase='complete')=(outcome IS NOT NULL)),
  CHECK(phase NOT IN ('ready','stopping','recovering') OR ready_writer_epoch IS NOT NULL)
);
CREATE UNIQUE INDEX maintenance_one_open_fence
  ON maintenance_fences((1)) WHERE ended_at IS NULL;
CREATE TABLE maintenance_cohort (
  epoch INTEGER NOT NULL REFERENCES maintenance_fences(epoch),
  agent_id TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('created','starting','running','cancelling')),
  sequence INTEGER NOT NULL CHECK(sequence>=1),
  orchestrator_session_id TEXT,
  attempts_json TEXT NOT NULL CHECK(json_valid(attempts_json)),
  PRIMARY KEY(epoch,agent_id)
);
CREATE INDEX maintenance_cohort_agent ON maintenance_cohort(agent_id,epoch);
-- Bound recipients and their original session routing are captured once.
CREATE TABLE maintenance_recipients (
  epoch INTEGER NOT NULL,
  orchestrator_session_id TEXT NOT NULL,
  transport TEXT NOT NULL,
  external_session_id TEXT NOT NULL,
  anchor_agent_id TEXT NOT NULL,
  before_delivery_id TEXT NOT NULL UNIQUE,
  after_delivery_id TEXT UNIQUE,
  PRIMARY KEY(epoch,orchestrator_session_id),
  FOREIGN KEY(epoch,anchor_agent_id) REFERENCES maintenance_cohort(epoch,agent_id)
);
CREATE INDEX maintenance_recipients_anchor ON maintenance_recipients(anchor_agent_id,epoch);
CREATE TRIGGER maintenance_recipient_immutable BEFORE UPDATE ON maintenance_recipients
WHEN NEW.epoch IS NOT OLD.epoch OR NEW.orchestrator_session_id IS NOT OLD.orchestrator_session_id
  OR NEW.transport IS NOT OLD.transport OR NEW.external_session_id IS NOT OLD.external_session_id
  OR NEW.anchor_agent_id IS NOT OLD.anchor_agent_id OR NEW.before_delivery_id IS NOT OLD.before_delivery_id
  OR (OLD.after_delivery_id IS NOT NULL AND NEW.after_delivery_id IS NOT OLD.after_delivery_id)
BEGIN SELECT RAISE(ABORT,'maintenance recipient is immutable'); END;
CREATE TRIGGER maintenance_fence_identity_immutable BEFORE UPDATE ON maintenance_fences
WHEN NEW.epoch IS NOT OLD.epoch OR NEW.request_id IS NOT OLD.request_id
  OR NEW.created_at IS NOT OLD.created_at OR NEW.member_count IS NOT OLD.member_count
  OR NEW.origin_writer_epoch IS NOT OLD.origin_writer_epoch
  OR (OLD.ready_writer_epoch IS NOT NULL AND NEW.ready_writer_epoch IS NOT OLD.ready_writer_epoch)
  OR (OLD.outcome IS NOT NULL AND NEW.outcome IS NOT OLD.outcome)
  OR (NEW.phase IS NOT OLD.phase AND NOT (
    (OLD.phase='preparing' AND NEW.phase IN ('ready','deferring','complete')) OR
    (OLD.phase='ready' AND NEW.phase IN ('stopping','deferring')) OR
    (OLD.phase='stopping' AND NEW.phase='recovering') OR
    (OLD.phase IN ('recovering','deferring') AND NEW.phase='complete')))
  OR (OLD.ended_at IS NOT NULL AND NEW.ended_at IS NOT OLD.ended_at)
BEGIN SELECT RAISE(ABORT,'maintenance fence identity is immutable'); END;
CREATE TRIGGER maintenance_cohort_immutable BEFORE UPDATE ON maintenance_cohort
BEGIN SELECT RAISE(ABORT,'maintenance cohort is immutable'); END;

-- Checkpoint intent remains separate from worker_lifecycle terminal finish.
-- No agent/attempt FK: history retention integration is a separate boundary.
CREATE TABLE maintenance_checkpoints (
  epoch INTEGER NOT NULL,
  agent_id TEXT NOT NULL,
  attempt_id TEXT NOT NULL,
  challenge_id TEXT NOT NULL UNIQUE CHECK(length(challenge_id)=64 AND challenge_id NOT GLOB '*[^0-9a-f]*'),
  requested_at REAL NOT NULL CHECK(requested_at>=0),
  command_id INTEGER REFERENCES commands(id),
  closing_event_seq INTEGER CHECK(closing_event_seq IS NULL OR closing_event_seq>0),
  closing_event_sha256 TEXT CHECK(closing_event_sha256 IS NULL OR (length(closing_event_sha256)=64 AND closing_event_sha256 NOT GLOB '*[^0-9a-f]*')),
  closing_at REAL CHECK(closing_at IS NULL OR closing_at>=0),
  checkpoint_json TEXT CHECK(checkpoint_json IS NULL OR json_valid(checkpoint_json)),
  checkpoint_sha256 TEXT CHECK(checkpoint_sha256 IS NULL OR length(checkpoint_sha256)=64),
  accepted_at REAL,
  receipt_observed INTEGER NOT NULL DEFAULT 0 CHECK(receipt_observed IN (0,1)),
  PRIMARY KEY(epoch,attempt_id),
  CHECK ((closing_event_seq IS NULL AND closing_event_sha256 IS NULL AND closing_at IS NULL) OR (closing_event_seq IS NOT NULL AND closing_event_sha256 IS NOT NULL AND closing_at IS NOT NULL)),
  FOREIGN KEY(epoch,agent_id) REFERENCES maintenance_cohort(epoch,agent_id),
  CHECK ((checkpoint_json IS NULL AND checkpoint_sha256 IS NULL AND accepted_at IS NULL AND receipt_observed=0)
    OR (checkpoint_json IS NOT NULL AND checkpoint_sha256 IS NOT NULL AND accepted_at IS NOT NULL))
);
CREATE INDEX maintenance_checkpoints_agent ON maintenance_checkpoints(agent_id,epoch);
CREATE UNIQUE INDEX maintenance_checkpoint_command ON maintenance_checkpoints(command_id) WHERE command_id IS NOT NULL;
CREATE TRIGGER maintenance_checkpoint_immutable BEFORE UPDATE ON maintenance_checkpoints
WHEN NEW.epoch IS NOT OLD.epoch OR NEW.agent_id IS NOT OLD.agent_id
  OR NEW.attempt_id IS NOT OLD.attempt_id OR NEW.challenge_id IS NOT OLD.challenge_id
  OR NEW.requested_at IS NOT OLD.requested_at OR NEW.receipt_observed<OLD.receipt_observed
  OR (OLD.command_id IS NOT NULL AND NEW.command_id IS NOT OLD.command_id)
  OR (OLD.closing_event_seq IS NOT NULL AND (NEW.closing_event_seq IS NOT OLD.closing_event_seq OR NEW.closing_event_sha256 IS NOT OLD.closing_event_sha256 OR NEW.closing_at IS NOT OLD.closing_at))
  OR (OLD.checkpoint_json IS NOT NULL AND (NEW.checkpoint_json IS NOT OLD.checkpoint_json
    OR NEW.checkpoint_sha256 IS NOT OLD.checkpoint_sha256 OR NEW.accepted_at IS NOT OLD.accepted_at))
BEGIN SELECT RAISE(ABORT,'maintenance checkpoint intent is immutable'); END;

-- Nonterminal park evidence. Source agents/attempts/events are deliberately
-- not foreign keys: retention deletes safely closed park rows before cohort.
CREATE TABLE maintenance_parks (
  epoch INTEGER NOT NULL,
  agent_id TEXT NOT NULL,
  attempt_id TEXT NOT NULL,
  native_event_seq INTEGER NOT NULL UNIQUE,
  proof_sha256 TEXT NOT NULL CHECK(length(proof_sha256)=64),
  parked_at REAL NOT NULL CHECK(parked_at>=0),
  PRIMARY KEY(epoch,agent_id),
  UNIQUE(epoch,attempt_id),
  FOREIGN KEY(epoch,agent_id) REFERENCES maintenance_cohort(epoch,agent_id)
);
CREATE INDEX maintenance_parks_agent ON maintenance_parks(agent_id,epoch);
CREATE TRIGGER maintenance_park_immutable BEFORE UPDATE ON maintenance_parks
BEGIN SELECT RAISE(ABORT,'maintenance park evidence is immutable'); END;

-- An allocation receipt is not a launch permit. Uncertain effects reconcile
-- the same target attempt; source execution history is retained independently.
CREATE TABLE maintenance_resumes (
  epoch INTEGER NOT NULL,
  agent_id TEXT NOT NULL,
  request_id TEXT NOT NULL UNIQUE CHECK(length(request_id) BETWEEN 1 AND 256),
  source_attempt_id TEXT NOT NULL,
  target_attempt_id TEXT NOT NULL UNIQUE,
  target_number INTEGER NOT NULL CHECK(target_number>=2),
  account_id TEXT NOT NULL,
  source_proof_sha256 TEXT NOT NULL CHECK(length(source_proof_sha256)=64),
  writer_epoch INTEGER NOT NULL CHECK(writer_epoch>=1),
  claimed_at REAL NOT NULL CHECK(claimed_at>=0),
  preflight_json TEXT NOT NULL CHECK(json_valid(preflight_json)),
  PRIMARY KEY(epoch,agent_id),
  FOREIGN KEY(epoch,agent_id) REFERENCES maintenance_cohort(epoch,agent_id)
);
CREATE INDEX maintenance_resumes_agent ON maintenance_resumes(agent_id,epoch);
CREATE TRIGGER maintenance_resume_immutable BEFORE UPDATE ON maintenance_resumes
BEGIN SELECT RAISE(ABORT,'maintenance resume allocation is immutable'); END;
