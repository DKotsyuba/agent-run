-- Execution lifetimes are unlimited. Preserve retired numeric policy as an
-- immutable historical event, never as active request/authorization controls.
-- Original request/identity JSON and prior events remain byte-for-byte intact.
INSERT INTO events(agent_id,attempt_id,at,kind,data_json)
SELECT id,NULL,COALESCE(finished_at,started_at,created_at),
       'historical_execution_policy',
       json_object('version',1,'timeout_seconds',timeout_seconds,
                   'warned',warned,'silent_seconds',silent_seconds)
FROM agents;
ALTER TABLE agents DROP COLUMN timeout_seconds;
ALTER TABLE agents DROP COLUMN warned;
ALTER TABLE agents DROP COLUMN silent_seconds;
-- The enrollment challenge stays pinned to the exact live attempt. Historical
-- lifetime pins remain events; no pending awareness is expired by a job clock.
INSERT INTO events(agent_id,attempt_id,at,kind,data_json)
SELECT run_id,attempt_id,created_at,'historical_enrollment_policy',
       json_object('version',1,'deadline',deadline)
FROM pool_enrollments;
DROP INDEX idx_pool_enrollments_pending;
DROP TRIGGER pool_enrollment_identity_immutable;
ALTER TABLE pool_enrollments DROP COLUMN deadline;
CREATE INDEX idx_pool_enrollments_pending ON pool_enrollments(state);
CREATE TRIGGER pool_enrollment_identity_immutable BEFORE UPDATE ON pool_enrollments
WHEN NEW.agent_id IS NOT OLD.agent_id OR NEW.run_id IS NOT OLD.run_id
  OR NEW.attempt_id IS NOT OLD.attempt_id OR NEW.challenge IS NOT OLD.challenge
  OR NEW.created_at IS NOT OLD.created_at
  OR (OLD.state='joined' AND NEW.state<>'joined')
  OR (OLD.attention_issued=1 AND NEW.attention_issued<>1)
BEGIN SELECT RAISE(ABORT,'pool enrollment identity is immutable'); END;
