-- One immutable publication intention per existing local outbox notice.
-- Local 'delivered' means publication released; Bridge custody remains distinct
-- from native acceptance and application processing in public projections.
CREATE TABLE bridge_delivery_intents (
  delivery_id TEXT PRIMARY KEY REFERENCES deliveries(id) ON DELETE CASCADE,
  intent_json TEXT CHECK(intent_json IS NULL OR json_valid(intent_json)),
  custody_receipt_json TEXT CHECK(custody_receipt_json IS NULL OR json_valid(custody_receipt_json)),
  custody_state TEXT NOT NULL DEFAULT 'pending'
    CHECK(custody_state IN ('pending','custody_accepted','outcome_unknown'))
);
