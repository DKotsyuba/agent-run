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
