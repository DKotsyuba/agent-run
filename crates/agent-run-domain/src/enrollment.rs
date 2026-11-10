//! Private Pool-resident identity enrollment, independent of public start DTOs.
use crate::{
    ProviderStartRequest, Result, StoreId,
    attempt_binding::{self, Route},
    batch::{BatchPrecondition, ExecutionRef, MAX_BATCH_JOBS, PinId},
    domain::AgentId,
    error::invalid,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Start,
    Replace,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Selector {
    pub version: u32,
    pub role_name: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Job {
    pub member_id: String,
    pub selector: Selector,
    /// Original parsed spec, not normalized through the provider DTO for hashing.
    pub spec: Value,
}
impl std::fmt::Debug for Job {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Job")
            .field("member_id", &self.member_id)
            .field("selector", &self.selector)
            .field("spec", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Start {
    pub version: u32,
    pub expected_store_id: StoreId,
    pub pool_store_id: String,
    pub pool_id: String,
    pub request_id: String,
    pub mode: Mode,
    pub jobs: Vec<Job>,
    pub preconditions: Vec<BatchPrecondition>,
}

impl Start {
    /// Check object-only shape and immutable input before config or database I/O.
    pub fn parse(value: &Value) -> Result<(Self, String)> {
        let object = value
            .as_object()
            .ok_or_else(|| invalid("enrollment request invalid"))?;
        let jobs = object
            .get("jobs")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("enrollment request invalid"))?;
        if jobs.iter().any(|job| {
            let Some(job) = job.as_object() else {
                return true;
            };
            !job.get("selector").is_some_and(Value::is_object)
                || !job.get("spec").is_some_and(Value::is_object)
                || job
                    .get("spec")
                    .and_then(Value::as_object)
                    .is_some_and(|spec| spec.contains_key("request_id"))
        }) || object
            .get("preconditions")
            .and_then(Value::as_array)
            .is_none_or(|conditions| conditions.iter().any(|condition| !condition.is_object()))
        {
            return Err(invalid("enrollment request invalid"));
        }
        let request: Self = serde_json::from_value(value.clone())
            .map_err(|_| invalid("enrollment request invalid"))?;
        request.validate()?;
        let fingerprint = attempt_binding::enrollment_start_fingerprint(value)?;
        Ok((request, fingerprint))
    }

    pub fn validate(&self) -> Result<()> {
        if self.version != 1 {
            return Err(crate::Error::Unsupported(
                "attempt version unsupported".into(),
            ));
        }
        attempt_binding::validate_uuid(&self.pool_store_id)?;
        attempt_binding::validate_id(&self.pool_id)?;
        attempt_binding::validate_id(&self.request_id)?;
        if !(1..=MAX_BATCH_JOBS).contains(&self.jobs.len())
            || match self.mode {
                Mode::Start => !self.preconditions.is_empty(),
                Mode::Replace => self.jobs.len() != 1 || self.preconditions.len() != 1,
            }
        {
            return Err(invalid("enrollment request invalid"));
        }
        if !self.preconditions.is_empty() {
            crate::batch::validate_preconditions(&self.preconditions)?;
        }
        let mut members = HashSet::new();
        for job in &self.jobs {
            attempt_binding::validate_id(&job.member_id)?;
            attempt_binding::validate_id(&job.selector.role_name)?;
            if !members.insert(&job.member_id) || job.selector.version != 1 {
                return Err(invalid("enrollment request invalid"));
            }
            if !job.spec.is_object() || job.spec.get("request_id").is_some() {
                return Err(invalid("enrollment request invalid"));
            }
            let mut spec: ProviderStartRequest = serde_json::from_value(job.spec.clone())
                .map_err(|_| invalid("enrollment request invalid"))?;
            spec.validate_intent()?;
            if spec.profile != job.selector.role_name || spec.write || !spec.explicit_finish {
                return Err(invalid("enrollment role mismatch"));
            }
        }
        Ok(())
    }
}

/// A permanently scoped exact-key lookup or abort.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Key {
    pub version: u32,
    pub expected_store_id: StoreId,
    pub pool_store_id: String,
    pub request_id: String,
    pub fingerprint: String,
}
impl Key {
    pub fn validate(&self, pool_route: &Route) -> Result<()> {
        if self.version != 1 {
            return Err(crate::Error::Unsupported(
                "attempt version unsupported".into(),
            ));
        }
        pool_route.validate()?;
        attempt_binding::validate_uuid(&self.pool_store_id)?;
        attempt_binding::validate_id(&self.request_id)?;
        attempt_binding::validate_digest(&self.fingerprint)?;
        if self.pool_store_id != pool_route.component_store_id {
            return Err(invalid("route mismatch"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Admission {
    pub index: usize,
    pub member_id: String,
    pub lineage_root_id: AgentId,
    pub execution_id: AgentId,
    pub sequence: u32,
    pub execution_ref: ExecutionRef,
    pub pin_id: PinId,
    pub managed_role_name: String,
    pub role_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub version: u32,
    pub store_id: StoreId,
    pub request_id: String,
    pub receipt_id: String,
    pub fingerprint: String,
    pub state: String,
    pub jobs: Vec<Admission>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn p3_enrollment_fingerprint_vector_and_strict_selector() {
        let value = json!({"version":1,"expected_store_id":"store_0123456789abcdef0123456789abcdef","pool_store_id":"00000000-0000-4000-8000-000000000001","pool_id":"pool-1","request_id":"enroll-1","mode":"start","jobs":[{"member_id":"member-1","selector":{"version":1,"role_name":"managed"},"spec":{"provider":"codex","model":"model","profile":"managed","task":"hello","workdir":"/work","explicit_finish":true,"write":false}}],"preconditions":[]});
        let (_, digest) = Start::parse(&value).unwrap();
        assert_eq!(
            digest,
            "33052668086558da6746ae6863012ab5005befc4d30523ad6b6e0b39e3a2a162"
        );
        let mut altered = value.clone();
        altered["jobs"][0]["spec"]["write"] = json!(true);
        assert!(Start::parse(&altered).is_err());
        let mut nested = value.clone();
        nested["jobs"][0]["spec"]["request_id"] = Value::Null;
        assert!(Start::parse(&nested).is_err());
        let mut wrong = value;
        wrong["jobs"][0]["selector"]["role_name"] = json!("other");
        assert!(Start::parse(&wrong).is_err());
    }

    #[test]
    fn p3_exact_selector_batch_bounds_and_original_spec_fingerprint() {
        let spec = json!({"provider":"codex","model":"model","profile":"managed","task":"hello","workdir":"/work"});
        let job = |index| json!({"member_id":format!("member-{index}"),"selector":{"version":1,"role_name":"managed"},"spec":spec});
        let mut value = json!({"version":1,"expected_store_id":"store_0123456789abcdef0123456789abcdef","pool_store_id":"00000000-0000-4000-8000-000000000001","pool_id":"pool","request_id":"key","mode":"start","jobs":[job(0)],"preconditions":[]});
        let (_, absent) = Start::parse(&value).unwrap();
        value["jobs"][0]["spec"]["write"] = json!(false);
        let (_, explicit) = Start::parse(&value).unwrap();
        assert_ne!(absent, explicit);
        value["jobs"][0]["spec"]["explicit_finish"] = json!(false);
        assert!(Start::parse(&value).is_err());
        value["jobs"] = json!((0..20).map(job).collect::<Vec<_>>());
        assert!(Start::parse(&value).is_ok());
        value["jobs"].as_array_mut().unwrap().push(job(20));
        assert!(Start::parse(&value).is_err());
        value["jobs"] = json!([job(0), job(0)]);
        assert!(Start::parse(&value).is_err());
        value["jobs"] = json!([job(0)]);
        value["mode"] = json!("replace");
        assert!(Start::parse(&value).is_err());
        value["preconditions"] = json!([{"agent_id":"ag-20260826-120000-0123456789","sequence":1,"execution_ref":format!("ex1_{}","a".repeat(64)),"pin_id":"pin_550e8400e29b41d4a716446655440000"}]);
        assert!(Start::parse(&value).is_ok());
        value["preconditions"][0]
            .as_object_mut()
            .unwrap()
            .remove("pin_id");
        assert!(Start::parse(&value).is_err());
    }
}
