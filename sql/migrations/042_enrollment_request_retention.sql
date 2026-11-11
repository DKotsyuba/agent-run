-- A terminal enrollment key keeps its route, fingerprint, transport pin and
-- receipt or tombstone permanently, but not the original start request body
-- (task text). Admission and abort clear it in the same transition; existing
-- terminal rows are compacted here. Only prepared keys count against capacity.
CREATE TABLE managed_enrollments_v42 (
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
INSERT INTO managed_enrollments_v42(pool_route_json,pool_store_id,request_id,fingerprint_version,fingerprint,state,request_json,grant_json,receipt_id,receipt_json)
SELECT pool_route_json,pool_store_id,request_id,fingerprint_version,fingerprint,state,
  CASE WHEN state='prepared' THEN request_json END,grant_json,receipt_id,receipt_json
FROM managed_enrollments;
DROP TABLE managed_enrollments;
ALTER TABLE managed_enrollments_v42 RENAME TO managed_enrollments;
CREATE INDEX managed_enrollments_prepared ON managed_enrollments(state) WHERE state='prepared';
CREATE TRIGGER managed_enrollments_immutable BEFORE UPDATE ON managed_enrollments
WHEN NEW.pool_route_json IS NOT OLD.pool_route_json OR NEW.pool_store_id IS NOT OLD.pool_store_id
  OR NEW.request_id IS NOT OLD.request_id OR NEW.fingerprint_version IS NOT OLD.fingerprint_version
  OR NEW.fingerprint IS NOT OLD.fingerprint OR NEW.request_json IS NOT NULL OR NEW.grant_json IS NOT OLD.grant_json
  OR OLD.state!='prepared' OR NEW.state NOT IN ('admitted','aborted')
BEGIN SELECT RAISE(ABORT, 'invalid managed enrollment transition'); END;
CREATE TRIGGER managed_enrollments_no_delete BEFORE DELETE ON managed_enrollments
BEGIN SELECT RAISE(ABORT, 'managed enrollment is permanent'); END;
