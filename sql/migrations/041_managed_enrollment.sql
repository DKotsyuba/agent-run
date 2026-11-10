-- Permanent Pool-resident enrollment keys. An abort is a rejecting tombstone;
-- an admission and all of its member ownership rows commit in one transaction.
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
  CHECK ((state='admitted' AND request_json IS NOT NULL AND grant_json IS NOT NULL AND receipt_id IS NOT NULL AND receipt_json IS NOT NULL)
      OR (state='prepared' AND request_json IS NOT NULL AND grant_json IS NOT NULL AND receipt_id IS NULL AND receipt_json IS NULL)
      OR (state='aborted' AND receipt_id IS NULL AND receipt_json IS NULL)),
  PRIMARY KEY(pool_route_json,pool_store_id,request_id),
  UNIQUE(pool_store_id,request_id)
);
CREATE TRIGGER managed_enrollments_immutable BEFORE UPDATE ON managed_enrollments
WHEN NEW.pool_route_json IS NOT OLD.pool_route_json OR NEW.pool_store_id IS NOT OLD.pool_store_id
  OR NEW.request_id IS NOT OLD.request_id OR NEW.fingerprint_version IS NOT OLD.fingerprint_version
  OR NEW.fingerprint IS NOT OLD.fingerprint OR NEW.request_json IS NOT OLD.request_json OR NEW.grant_json IS NOT OLD.grant_json
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
