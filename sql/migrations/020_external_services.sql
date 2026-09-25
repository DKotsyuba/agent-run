-- Existing service generations retain supervisor ownership after upgrading.
-- External generations are recorded explicitly and must never be terminated by the broker.
ALTER TABLE managed_service_generations ADD COLUMN ownership TEXT NOT NULL DEFAULT 'managed' CHECK(ownership IN ('managed','external'));
