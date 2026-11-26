-- Keep release audit evidence independently of attempts and cooldown retention.
CREATE TABLE binding_cooldown_releases (
  seq INTEGER PRIMARY KEY,
  provider_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  cooldown_reason TEXT NOT NULL,
  cooldown_observed_at REAL NOT NULL,
  blocked_until REAL NOT NULL,
  at REAL NOT NULL,
  reason TEXT NOT NULL,
  operator_uid INTEGER,
  evidence_observed_at REAL,
  CHECK ((operator_uid IS NOT NULL AND evidence_observed_at IS NULL)
      OR (operator_uid IS NULL AND reason = 'quota_recovered'
          AND evidence_observed_at IS NOT NULL AND evidence_observed_at > cooldown_observed_at))
);
