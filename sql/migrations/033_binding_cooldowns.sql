-- Billing and native quota refusals temporarily exclude only this binding.
CREATE TABLE binding_cooldowns (
  provider_id TEXT NOT NULL CHECK (length(trim(provider_id)) > 0),
  account_id TEXT NOT NULL REFERENCES provider_accounts(account_id),
  reason TEXT NOT NULL CHECK (reason IN ('billing_unavailable', 'quota_exhausted')),
  observed_at REAL NOT NULL CHECK (observed_at >= 0),
  blocked_until REAL NOT NULL CHECK (blocked_until > observed_at),
  attempt_id TEXT NOT NULL CHECK (length(trim(attempt_id)) > 0),
  PRIMARY KEY (account_id, provider_id)
);
CREATE INDEX idx_binding_cooldowns_expiry ON binding_cooldowns(blocked_until);
