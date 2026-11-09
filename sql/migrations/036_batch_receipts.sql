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
