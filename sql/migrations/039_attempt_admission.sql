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
