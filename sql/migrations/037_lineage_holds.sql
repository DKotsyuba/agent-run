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
