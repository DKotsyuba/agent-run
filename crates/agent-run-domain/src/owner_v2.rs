//! Strict owner-v2 outbox/request contracts. A value is never peer authority.
use crate::{
    Result,
    attempt_binding::{Binding, Incarnation, Route, validate_digest, validate_id, validate_uuid},
    error::invalid,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

/// Maximum unmodified UTF-8 payload bytes accepted by owner v2.
pub const TEXT_BYTES: usize = 98_304;
/// One immutable, attempt-bound Pool outbox effect.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxIntent {
    pub effect_id: String,
    pub producer_scope: String,
    pub event_key: String,
    pub payload_sha256: String,
    pub payload_length: u64,
    pub target_binding: Binding,
    pub expected_incarnation: Incarnation,
    pub bridge_route: Route,
    pub mode: String,
    pub retention: String,
}
impl std::fmt::Debug for OutboxIntent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OutboxIntent([REDACTED])")
    }
}
impl OutboxIntent {
    pub fn validate(&self) -> Result<()> {
        validate_id(&self.effect_id)?;
        validate_id(&self.producer_scope)?;
        validate_id(&self.event_key)?;
        validate_digest(&self.payload_sha256)?;
        self.target_binding.validate()?;
        self.expected_incarnation.validate()?;
        self.bridge_route.validate()?;
        validate_uuid(&self.bridge_route.component_store_id)?;
        if self.payload_length > TEXT_BYTES as u64
            || self.mode != "message"
            || self.retention != "keep_all"
            || self.bridge_route != self.target_binding.bridge_route
            || self.expected_incarnation.uid != self.bridge_route.worker_uid
            || self.expected_incarnation.uid != self.target_binding.run_route.worker_uid
        {
            return Err(invalid("invalid owner effect"));
        }
        Ok(())
    }
}

/// Exact owner-v2 input, redacted in all debug output.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestV2 {
    pub version: u32,
    pub thread_id: String,
    pub call_id: String,
    pub outbox_intent: OutboxIntent,
    pub effect_digest: String,
    pub text: String,
    pub wake_only: bool,
}
impl std::fmt::Debug for RequestV2 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RequestV2([REDACTED])")
    }
}
impl RequestV2 {
    /// Verify exact target, payload bytes, version and complete canonical digest.
    pub fn validate(&self) -> Result<()> {
        self.outbox_intent.validate()?;
        validate_id(&self.thread_id)?;
        validate_id(&self.call_id)?;
        validate_digest(&self.effect_digest)?;
        if self.version != 2
            || !self.wake_only
            || self.thread_id != self.outbox_intent.target_binding.actor.native_id
            || self.text.len() > TEXT_BYTES
            || self.outbox_intent.payload_length != self.text.len() as u64
            || self.outbox_intent.payload_sha256
                != crate::canonical::hex_digest(&Sha256::digest(self.text.as_bytes()))
            || self.effect_digest != outbox_digest(&self.outbox_intent)?
        {
            return Err(invalid("invalid owner request"));
        }
        Ok(())
    }
}

/// Versioned reply, no diagnostic payload or caller arguments.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplyV2 {
    pub version: u32,
    pub call_id: String,
    pub effect_id: String,
    pub effect_digest: String,
    pub state: String,
    pub protocol: Option<String>,
    pub native_id: Option<String>,
    pub reason: Option<String>,
}

fn tagged(v: &Value) -> Result<Value> {
    Ok(match v {
        Value::Null => json!(["n"]),
        Value::Bool(b) => json!(["b", b]),
        Value::String(s) => json!(["s", s]),
        Value::Number(n) if n.is_i64() => json!([
            "i",
            n.as_i64()
                .ok_or_else(|| invalid("invalid canonical number"))?
                .to_string()
        ]),
        Value::Number(n) if n.is_u64() => json!([
            "i",
            n.as_u64()
                .ok_or_else(|| invalid("invalid canonical number"))?
                .to_string()
        ]),
        Value::Number(n) => {
            let value = n
                .as_f64()
                .filter(|v| v.is_finite())
                .ok_or_else(|| invalid("invalid canonical number"))?;
            let bits = if value == 0.0 { 0 } else { value.to_bits() };
            json!(["f", format!("{bits:016x}")])
        }
        Value::Array(items) => json!(["a", items.iter().map(tagged).collect::<Result<Vec<_>>>()?]),
        Value::Object(map) => {
            let mut keys: Vec<_> = map.keys().collect();
            keys.sort();
            let pairs = keys
                .into_iter()
                .map(|key| Ok(json!([key, tagged(&map[key])?])))
                .collect::<Result<Vec<_>>>()?;
            json!(["o", pairs])
        }
    })
}
fn digest_value(value: &Value) -> Result<(usize, String)> {
    let bytes = crate::canonical::dumps(&tagged(value)?, false);
    let mut hash = Sha256::new();
    hash.update(b"agent-mcp/attempt-binding/egress-intent/v1\0");
    hash.update(&bytes);
    Ok((bytes.len(), crate::canonical::hex_digest(&hash.finalize())))
}
/// Hash all frozen outbox fields under the pinned B2 effect domain.
pub fn outbox_digest(intent: &OutboxIntent) -> Result<String> {
    Ok(digest_value(&serde_json::to_value(intent)?)?.1)
}

/// Recursively reject duplicate keys before a JSON value can overwrite one.
struct Strict(Value);
impl<'de> Deserialize<'de> for Strict {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        use serde::de::{self, MapAccess, SeqAccess, Visitor};
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Strict;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("strict owner JSON")
            }
            fn visit_bool<E: de::Error>(self, v: bool) -> std::result::Result<Strict, E> {
                Ok(Strict(v.into()))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> std::result::Result<Strict, E> {
                Ok(Strict(v.into()))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> std::result::Result<Strict, E> {
                Ok(Strict(v.into()))
            }
            fn visit_f64<E: de::Error>(self, v: f64) -> std::result::Result<Strict, E> {
                serde_json::Number::from_f64(v)
                    .map(|n| Strict(n.into()))
                    .ok_or_else(|| E::custom("invalid number"))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<Strict, E> {
                Ok(Strict(v.into()))
            }
            fn visit_string<E: de::Error>(self, v: String) -> std::result::Result<Strict, E> {
                Ok(Strict(v.into()))
            }
            fn visit_unit<E: de::Error>(self) -> std::result::Result<Strict, E> {
                Ok(Strict(Value::Null))
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut source: A,
            ) -> std::result::Result<Strict, A::Error> {
                let mut out = Vec::new();
                while let Some(v) = source.next_element::<Strict>()? {
                    out.push(v.0)
                }
                Ok(Strict(out.into()))
            }
            fn visit_map<A: MapAccess<'de>>(
                self,
                mut source: A,
            ) -> std::result::Result<Strict, A::Error> {
                let mut out = serde_json::Map::new();
                while let Some(k) = source.next_key::<String>()? {
                    if out.contains_key(&k) {
                        return Err(de::Error::custom("duplicate key"));
                    }
                    let v = source.next_value::<Strict>()?;
                    out.insert(k, v.0);
                }
                Ok(Strict(out.into()))
            }
        }
        d.deserialize_any(V)
    }
}
/// Decode a single bounded request, preserving nested duplicate/key and object checks.
pub fn decode_request(bytes: &[u8]) -> Result<RequestV2> {
    if bytes.is_empty() || bytes.len() > 1024 * 1024 {
        return Err(invalid("invalid owner frame"));
    }
    let value: Strict =
        serde_json::from_slice(bytes).map_err(|_| invalid("invalid owner frame"))?;
    for path in [
        "",
        "/outbox_intent",
        "/outbox_intent/target_binding",
        "/outbox_intent/target_binding/attempt",
        "/outbox_intent/target_binding/actor",
        "/outbox_intent/target_binding/run_route",
        "/outbox_intent/target_binding/bridge_route",
        "/outbox_intent/expected_incarnation",
        "/outbox_intent/bridge_route",
    ] {
        if !value.0.pointer(path).is_some_and(Value::is_object) {
            return Err(invalid("invalid owner frame"));
        }
    }
    let request: RequestV2 =
        serde_json::from_value(value.0).map_err(|_| invalid("invalid owner frame"))?;
    request.validate()?;
    Ok(request)
}

/// Recover only unambiguous, bounded response identifiers from a malformed
/// frame. Duplicate keys or invalid identifiers cannot be safely echoed.
pub fn invalid_request_echo(bytes: &[u8]) -> Option<(String, String, String)> {
    if bytes.is_empty() || bytes.len() > 1024 * 1024 {
        return None;
    }
    let value: Strict = serde_json::from_slice(bytes).ok()?;
    let call = value.0.get("call_id")?.as_str()?;
    let effect = value.0.pointer("/outbox_intent/effect_id")?.as_str()?;
    let digest = value.0.get("effect_digest")?.as_str()?;
    validate_id(call).ok()?;
    validate_id(effect).ok()?;
    validate_digest(digest).ok()?;
    Some((call.into(), effect.into(), digest.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn canonical_outbox_vector() {
        let raw = r#"{"effect_id":"effect","producer_scope":"scope","event_key":"event","payload_sha256":"982d9e3eb996f559e633f4d194def3761d909f5a3b647d1a851fead67c32c9d1","payload_length":4,"target_binding":{"pool_store_id":"pool-store","pool_id":"pool","member_id":"member","binding_generation":1,"attempt":{"run_store_id":"store_run","lineage_root_id":"root","execution_id":"exec","sequence":1,"attempt_id":"attempt","launch_id":"01234567-89ab-4cde-8fab-0123456789ab"},"actor":{"address":"lab/codex/store/session","role":"root","native_id":"session","epoch":1},"run_route":{"component_store_id":"store_run","route_id":"run-route","worker_uid":1000},"bridge_route":{"component_store_id":"01234567-89ab-4cde-8fab-0123456789ab","route_id":"bridge-route","worker_uid":1000}},"expected_incarnation":{"host_id":"lab","boot_id":"01234567-89ab-4cde-8fab-0123456789ab","pid_namespace_inode":2,"uid":1000,"pid":42,"start_ticks":7},"bridge_route":{"component_store_id":"01234567-89ab-4cde-8fab-0123456789ab","route_id":"bridge-route","worker_uid":1000},"mode":"message","retention":"keep_all"}"#;
        let value: Value = serde_json::from_str(raw).unwrap();
        assert_eq!(
            digest_value(&value).unwrap(),
            (
                1436,
                "72518e5ded118c2d5922aabd377f6f85fd4a906a6cd2775512daaadf7d77a6e9".into()
            )
        );
        // The frozen vector deliberately uses nonproduction StoreId/AgentId
        // placeholders; construct only those scalars without production parsing.
        let mut typed_value = value.clone();
        typed_value["target_binding"]["attempt"]["run_store_id"] =
            json!("store_0123456789abcdef0123456789abcdef");
        typed_value["target_binding"]["attempt"]["lineage_root_id"] =
            json!("ag-20261109-120000-0000000001");
        typed_value["target_binding"]["attempt"]["execution_id"] =
            json!("ag-20261109-120000-0000000002");
        let mut typed: OutboxIntent = serde_json::from_value(typed_value).unwrap();
        typed.target_binding.attempt.run_store_id = crate::StoreId::vector_placeholder("store_run");
        typed.target_binding.attempt.lineage_root_id =
            crate::domain::AgentId::vector_placeholder("root");
        typed.target_binding.attempt.execution_id =
            crate::domain::AgentId::vector_placeholder("exec");
        assert_eq!(
            outbox_digest(&typed).unwrap(),
            "72518e5ded118c2d5922aabd377f6f85fd4a906a6cd2775512daaadf7d77a6e9"
        );
    }
    #[test]
    fn duplicate_and_nonobject_request_fail_closed() {
        assert!(decode_request(br#"{"version":2,"version":2}"#).is_err());
        assert!(decode_request(br#"[]"#).is_err());
        assert!(decode_request(br#"{"outbox_intent":{"x":1,"x":2}}"#).is_err());
        let digest = "a".repeat(64);
        let raw = json!({"call_id":"call","effect_digest":digest,"outbox_intent":{"effect_id":"effect"},"invalid":true}).to_string();
        assert_eq!(
            invalid_request_echo(raw.as_bytes()),
            Some(("call".into(), "effect".into(), digest))
        );
        assert!(invalid_request_echo(br#"{"call_id":"a","call_id":"b"}"#).is_none());
    }
}
