//! Strict v1 JSON data contracts for future operator lineage-hold operations.
//!
//! This module does not provide a socket, authorization, storage, or signature
//! verification. Parse requests directly from JSON rather than through a map or
//! `serde_json::Value`, which would discard duplicate keys before validation.
//! Transport owners must additionally bound the full request and response sizes.

use crate::{
    Error, Result, StoreId,
    canonical::hex_digest,
    domain::{AgentId, external_id},
    error::invalid,
    lineage_hold::{
        HoldId, LINEAGE_HOLD_VERSION, LineageHoldAcquire, LineageHoldTarget, MAX_HOLD_TARGETS,
    },
};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{Error as _, IgnoredAny, MapAccess, SeqAccess, Visitor},
    ser::SerializeStruct,
};
use std::{fmt, marker::PhantomData};

/// A fixed 32-byte value represented as exactly 64 lowercase hex characters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LowerHex32([u8; 32]);

impl LowerHex32 {
    /// Returns the exact decoded bytes, without hashing or changing their order.
    pub fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl From<[u8; 32]> for LowerHex32 {
    /// Wraps bytes for canonical lowercase hexadecimal serialization.
    fn from(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

impl Serialize for LowerHex32 {
    /// Serializes all 32 bytes as lowercase hexadecimal, preserving leading zeroes.
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&hex_digest(&self.0))
    }
}

impl<'de> Deserialize<'de> for LowerHex32 {
    /// Requires a fixed-size, lowercase hexadecimal string.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        decode_hex(&text).map(Self).map_err(D::Error::custom)
    }
}

/// Decodes exact lowercase hex without including the input in an error.
fn decode_hex<const N: usize>(text: &str) -> Result<[u8; N]> {
    if text.len() != N * 2 {
        return Err(invalid("invalid fixed-length lowercase hexadecimal"));
    }
    let mut bytes = [0; N];
    let (pairs, _) = text.as_bytes().as_chunks::<2>();
    for (out, pair) in bytes.iter_mut().zip(pairs) {
        let nibble = |byte| match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            _ => None,
        };
        *out = (nibble(pair[0])
            .ok_or_else(|| invalid("invalid fixed-length lowercase hexadecimal"))?
            << 4)
            | nibble(pair[1])
                .ok_or_else(|| invalid("invalid fixed-length lowercase hexadecimal"))?;
    }
    Ok(bytes)
}

/// A release proof that cannot be serialized or printed by the request type.
struct ReleaseSignature([u8; 64]);

impl fmt::Debug for ReleaseSignature {
    /// Redacts the complete proof in every debug context.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

impl<'de> Deserialize<'de> for ReleaseSignature {
    /// Decodes a proof without putting malformed proof values in type errors.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct SignatureVisitor;
        impl<'de> Visitor<'de> for SignatureVisitor {
            type Value = ReleaseSignature;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a 128-character lowercase hexadecimal signature")
            }

            fn visit_str<E: serde::de::Error>(
                self,
                value: &str,
            ) -> std::result::Result<Self::Value, E> {
                decode_hex(value)
                    .map(ReleaseSignature)
                    .map_err(|_| E::custom("invalid signature"))
            }

            fn visit_bool<E: serde::de::Error>(
                self,
                _: bool,
            ) -> std::result::Result<Self::Value, E> {
                Err(E::custom("invalid signature"))
            }
            fn visit_i64<E: serde::de::Error>(self, _: i64) -> std::result::Result<Self::Value, E> {
                Err(E::custom("invalid signature"))
            }
            fn visit_u64<E: serde::de::Error>(self, _: u64) -> std::result::Result<Self::Value, E> {
                Err(E::custom("invalid signature"))
            }
            fn visit_f64<E: serde::de::Error>(self, _: f64) -> std::result::Result<Self::Value, E> {
                Err(E::custom("invalid signature"))
            }
            fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<Self::Value, E> {
                Err(E::custom("invalid signature"))
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                _: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                Err(A::Error::custom("invalid signature"))
            }
            fn visit_map<A: MapAccess<'de>>(
                self,
                _: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                Err(A::Error::custom("invalid signature"))
            }
        }
        deserializer.deserialize_any(SignatureVisitor)
    }
}

/// Deserializes only a JSON object, preserving Serde's duplicate-key checks.
struct ObjectOnly<T>(T);

impl<'de, T: Deserialize<'de>> Deserialize<'de> for ObjectOnly<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct ObjectVisitor<T>(PhantomData<T>);
        impl<'de, T: Deserialize<'de>> Visitor<'de> for ObjectVisitor<T> {
            type Value = ObjectOnly<T>;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an object")
            }
            fn visit_map<M: MapAccess<'de>>(
                self,
                map: M,
            ) -> std::result::Result<Self::Value, M::Error> {
                T::deserialize(serde::de::value::MapAccessDeserializer::new(map)).map(ObjectOnly)
            }
        }
        deserializer.deserialize_map(ObjectVisitor(PhantomData))
    }
}

/// Implements strict map-only request decoding without deriving positional arrays.
macro_rules! object_request {
    ($name:ident { $($(#[$attr:meta])* $field:ident: $ty:ty),* $(,)? }) => {
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Fields { $($(#[$attr])* $field: $ty),* }
                let fields = ObjectOnly::<Fields>::deserialize(deserializer)?.0;
                Ok(Self { $($field: fields.$field),* })
            }
        }
    };
}

/// Requires target objects and bounds allocation before accepting a 21st item.
fn hold_targets<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<LineageHoldTarget>, D::Error> {
    struct TargetsVisitor;
    impl<'de> Visitor<'de> for TargetsVisitor {
        type Value = Vec<LineageHoldTarget>;
        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("an array of at most 20 target objects")
        }
        fn visit_seq<A: SeqAccess<'de>>(
            self,
            mut seq: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            let mut targets = Vec::new();
            while targets.len() < MAX_HOLD_TARGETS {
                match seq.next_element::<ObjectOnly<LineageHoldTarget>>()? {
                    Some(ObjectOnly(target)) => targets.push(target),
                    None => return Ok(targets),
                }
            }
            if seq.next_element::<IgnoredAny>()?.is_some() {
                return Err(A::Error::custom("lineage hold requires 1 to 20 targets"));
            }
            Ok(targets)
        }
    }
    deserializer.deserialize_seq(TargetsVisitor)
}

/// Requires a field to be present while accepting explicit JSON null.
fn required_nullable<'de, D: Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> std::result::Result<Option<T>, D::Error> {
    Option::<T>::deserialize(deserializer)
}

/// Rejects unsupported versions before an operation is passed to the store.
fn validate_version(version: u32) -> Result<()> {
    if version != LINEAGE_HOLD_VERSION {
        return Err(invalid("unsupported lineage hold version"));
    }
    Ok(())
}

/// Applies the same request identifier bounds used by acquire and store lookup.
fn validate_request_id(request_id: &str) -> Result<()> {
    if request_id.len() > 2048 {
        return Err(invalid("hold request_id exceeds 2048 UTF-8 bytes"));
    }
    external_id("hold request_id", request_id)
}

/// Passive acquire JSON input. Convert with `TryFrom` before any store call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineageHoldAcquireRequest {
    pub version: u32,
    pub expected_store_id: StoreId,
    pub request_id: String,
    pub targets: Vec<LineageHoldTarget>,
    /// Structurally sized only; Store must perform Ed25519 key validation.
    pub verifying_key: LowerHex32,
    /// Omission and null both mean no intent binding.
    pub intent_digest: Option<LowerHex32>,
}

object_request!(LineageHoldAcquireRequest {
    version: u32,
    expected_store_id: StoreId,
    request_id: String,
    #[serde(deserialize_with = "hold_targets")]
    targets: Vec<LineageHoldTarget>,
    verifying_key: LowerHex32,
    intent_digest: Option<LowerHex32>,
});

impl TryFrom<LineageHoldAcquireRequest> for LineageHoldAcquire {
    type Error = Error;
    /// Validates through the existing byte codec and preserves every field's bytes and order.
    ///
    /// # Errors
    /// Returns the existing canonical codec's structural validation error.
    fn try_from(request: LineageHoldAcquireRequest) -> Result<Self> {
        let acquire = Self {
            version: request.version,
            store_id: request.expected_store_id,
            request_id: request.request_id,
            targets: request.targets,
            verifying_key: request.verifying_key.into_bytes(),
            intent_digest: request.intent_digest.map(LowerHex32::into_bytes),
        };
        acquire.canonical_bytes()?;
        Ok(acquire)
    }
}

/// Passive lookup JSON input; call `validate` before querying committed history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineageHoldLookupRequest {
    pub version: u32,
    pub expected_store_id: StoreId,
    pub request_id: String,
    pub fingerprint: LowerHex32,
}

object_request!(LineageHoldLookupRequest {
    version: u32,
    expected_store_id: StoreId,
    request_id: String,
    fingerprint: LowerHex32,
});

impl LineageHoldLookupRequest {
    /// Validates version and the same request identifier bounds as the store.
    ///
    /// # Errors
    /// Rejects unsupported versions and invalid request identifiers.
    pub fn validate(&self) -> Result<()> {
        validate_version(self.version)?;
        validate_request_id(&self.request_id)
    }
}

/// State filter for a bounded recovery page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LineageHoldListState {
    All,
    Active,
    Released,
}

/// Passive list JSON input; every field is mandatory, including a nullable cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineageHoldListRequest {
    pub version: u32,
    pub expected_store_id: StoreId,
    pub state: LineageHoldListState,
    pub limit: usize,
    /// Exclusive cursor; null starts at the first hold.
    pub after_hold_id: Option<HoldId>,
}

object_request!(LineageHoldListRequest {
    version: u32,
    expected_store_id: StoreId,
    state: LineageHoldListState,
    limit: usize,
    #[serde(deserialize_with = "required_nullable")]
    after_hold_id: Option<HoldId>,
});

impl LineageHoldListRequest {
    /// Validates version and the store's inclusive page-size limit.
    ///
    /// # Errors
    /// Rejects unsupported versions and limits outside 1..=100.
    pub fn validate(&self) -> Result<()> {
        validate_version(self.version)?;
        if !(1..=100).contains(&self.limit) {
            return Err(invalid("lineage hold limit must be 1..=100"));
        }
        Ok(())
    }
}

/// Passive release JSON input. Its proof is redacted in Debug and never serialized.
#[derive(Debug)]
pub struct LineageHoldReleaseRequest {
    pub version: u32,
    pub expected_store_id: StoreId,
    pub hold_id: HoldId,
    signature: ReleaseSignature,
}

object_request!(LineageHoldReleaseRequest {
    version: u32,
    expected_store_id: StoreId,
    hold_id: HoldId,
    signature: ReleaseSignature,
});

impl LineageHoldReleaseRequest {
    /// Validates the version; proof verification remains the store's responsibility.
    ///
    /// # Errors
    /// Rejects an unsupported version without exposing proof data.
    pub fn validate(&self) -> Result<()> {
        validate_version(self.version)
    }

    /// Returns the exact 64 proof bytes for the existing store or broker API.
    /// Callers must never print or include these bytes in an output DTO or error.
    pub fn signature_bytes(&self) -> [u8; 64] {
        self.signature.0
    }
}

/// The durable lifecycle state of a hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LineageHoldWireState {
    Active,
    Released,
}

/// Public receipt; target and canonical-root positions correspond and retain store order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LineageHoldWireReceipt {
    pub version: u32,
    pub store_id: StoreId,
    pub hold_id: HoldId,
    pub fingerprint: LowerHex32,
    pub targets: Vec<LineageHoldTarget>,
    pub canonical_roots: Vec<AgentId>,
}

/// One public receipt and its current lifecycle state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LineageHoldWireObservation {
    pub receipt: LineageHoldWireReceipt,
    pub state: LineageHoldWireState,
}

/// Acquired or replayed receipt; a replay may already have been released.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LineageHoldAcquireResponse {
    pub receipt: LineageHoldWireReceipt,
    pub state: LineageHoldWireState,
    pub replayed: bool,
}

/// Lookup snapshot with no receipt or state when the hold was not found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineageHoldLookupResponse {
    store_id: StoreId,
    observation: Option<LineageHoldWireObservation>,
}

impl LineageHoldLookupResponse {
    /// Creates a lookup result from the store-scoped snapshot and optional observation.
    pub fn new(store_id: StoreId, observation: Option<LineageHoldWireObservation>) -> Self {
        Self {
            store_id,
            observation,
        }
    }
}

impl Serialize for LineageHoldLookupResponse {
    /// Serializes `found: false` without a receipt or state when no row was observed.
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut result = serializer.serialize_struct(
            "LineageHoldLookupResponse",
            if self.observation.is_some() { 4 } else { 2 },
        )?;
        result.serialize_field("store_id", &self.store_id)?;
        result.serialize_field("found", &self.observation.is_some())?;
        if let Some(observation) = &self.observation {
            result.serialize_field("receipt", &observation.receipt)?;
            result.serialize_field("state", &observation.state)?;
        }
        result.end()
    }
}

/// A store-scoped list page; a cursor means another matching row was observed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LineageHoldListResponse {
    pub store_id: StoreId,
    pub holds: Vec<LineageHoldWireObservation>,
    pub next_after_hold_id: Option<HoldId>,
}

/// Release lookup result; an absent row is not an authenticated release.
///
/// Absence from this snapshot does not prove an earlier unknown-outcome operation
/// could not have committed elsewhere or be in flight.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineageHoldReleaseResponse {
    store_id: StoreId,
    release: Option<(HoldId, bool)>,
}

impl LineageHoldReleaseResponse {
    /// Builds a store-scoped result; the boolean reports an already-released hold.
    pub fn new(store_id: StoreId, release: Option<(HoldId, bool)>) -> Self {
        Self { store_id, release }
    }
}

impl Serialize for LineageHoldReleaseResponse {
    /// Serializes a release only for a found, authenticated hold.
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut result = serializer.serialize_struct(
            "LineageHoldReleaseResponse",
            if self.release.is_some() { 5 } else { 2 },
        )?;
        result.serialize_field("store_id", &self.store_id)?;
        result.serialize_field("found", &self.release.is_some())?;
        if let Some((hold_id, already_released)) = &self.release {
            result.serialize_field("hold_id", hold_id)?;
            result.serialize_field("state", &LineageHoldWireState::Released)?;
            result.serialize_field("already_released", already_released)?;
        }
        result.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::de::DeserializeOwned;
    use serde_json::{Value, json};

    const STORE: &str = "store_00112233445566778899aabbccddeeff";
    const HOLD: &str = "hold_0123456789ab4cde8f0123456789abcd";
    const AGENT: &str = "ag-20261107-010203-0123456789";
    const PIN: &str = "pin_112233445566478899aabbccddeeff00";
    const PUBLIC_KEY: &str = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a";
    const FINGERPRINT: &str = "5407d58ddf96ca0e0b2fe7c7f3a74b363a7b502c121e4b594d7c0aa190d12f66";

    /// Makes the public fixed-vector target without any release proof.
    fn target() -> Value {
        json!({ "agent_id": AGENT, "sequence": 2, "execution_ref": format!("ex1_{}", "2".repeat(64)), "pin_id": PIN })
    }

    /// Makes the existing codec's public known-answer acquire as JSON.
    fn acquire() -> Value {
        json!({ "version": 1, "expected_store_id": STORE, "request_id": "hold-fixture-1", "targets": [target()], "verifying_key": PUBLIC_KEY })
    }

    /// Decodes test values using the same raw text entry point as the future socket.
    fn parse<T: DeserializeOwned>(value: &Value) -> serde_json::Result<T> {
        serde_json::from_str(&value.to_string())
    }

    /// Decodes the public vector to a receipt with one ordered root.
    fn receipt() -> LineageHoldWireReceipt {
        let request: LineageHoldAcquireRequest = parse(&acquire()).unwrap();
        let original = LineageHoldAcquire::try_from(request).unwrap();
        LineageHoldWireReceipt {
            version: 1,
            store_id: original.store_id,
            hold_id: HOLD.parse().unwrap(),
            fingerprint: serde_json::from_value(json!(FINGERPRINT)).unwrap(),
            targets: original.targets,
            canonical_roots: vec![AGENT.parse().unwrap()],
        }
    }

    /// Pins JSON-to-existing-codec byte compatibility to the established public vector.
    #[test]
    fn lineage_hold_wire_known_answer_and_order() {
        let request: LineageHoldAcquireRequest = parse(&acquire()).unwrap();
        let decoded = LineageHoldAcquire::try_from(request).unwrap();
        assert_eq!(hex_digest(&decoded.fingerprint().unwrap()), FINGERPRINT);
        assert_eq!(hex_digest(&decoded.verifying_key), PUBLIC_KEY);
        assert!(decoded.intent_digest.is_none());

        let mut with_intent = acquire();
        with_intent["intent_digest"] = json!("0a".repeat(32));
        let with_intent =
            LineageHoldAcquire::try_from(parse::<LineageHoldAcquireRequest>(&with_intent).unwrap())
                .unwrap();
        assert_eq!(with_intent.intent_digest, Some([0x0a; 32]));
        assert_ne!(
            decoded.fingerprint().unwrap(),
            with_intent.fingerprint().unwrap()
        );

        let mut ordered = acquire();
        let mut second = target();
        second["sequence"] = json!(3);
        ordered["targets"] = json!([target(), second]);
        let forward =
            LineageHoldAcquire::try_from(parse::<LineageHoldAcquireRequest>(&ordered).unwrap())
                .unwrap();
        ordered["targets"].as_array_mut().unwrap().reverse();
        let reverse =
            LineageHoldAcquire::try_from(parse::<LineageHoldAcquireRequest>(&ordered).unwrap())
                .unwrap();
        assert_eq!(forward.targets[0], reverse.targets[1]);
        assert_ne!(
            forward.fingerprint().unwrap(),
            reverse.fingerprint().unwrap()
        );
    }

    /// Requires lowercase fixed-width hex in every request field and in output.
    #[test]
    fn lineage_hold_wire_hex_is_fixed_width_and_canonical() {
        for bad in [
            "",
            "0",
            "a".repeat(63).as_str(),
            "a".repeat(65).as_str(),
            "A".repeat(64).as_str(),
            "g".repeat(64).as_str(),
            "é".repeat(32).as_str(),
        ] {
            for key in ["verifying_key", "intent_digest"] {
                let mut value = acquire();
                value[key] = json!(bad);
                assert!(parse::<LineageHoldAcquireRequest>(&value).is_err());
            }
            let value = json!({ "version": 1, "expected_store_id": STORE, "request_id": "x", "fingerprint": bad });
            assert!(parse::<LineageHoldLookupRequest>(&value).is_err());
        }
        let fingerprint: LowerHex32 = serde_json::from_str(&format!("\"{FINGERPRINT}\"")).unwrap();
        assert_eq!(serde_json::to_value(fingerprint).unwrap(), FINGERPRINT);
        let mut absent = acquire();
        absent["intent_digest"] = Value::Null;
        assert!(
            parse::<LineageHoldAcquireRequest>(&absent)
                .unwrap()
                .intent_digest
                .is_none()
        );
    }

    /// Rejects unknown or repeated keys in raw JSON, including nested targets and null options.
    #[test]
    fn lineage_hold_wire_rejects_duplicate_unknown_and_nonobject_fields() {
        let source = acquire().to_string();
        let duplicate_version = source.replacen("\"version\":1", "\"version\":1,\"version\":1", 1);
        assert!(serde_json::from_str::<LineageHoldAcquireRequest>(&duplicate_version).is_err());
        let duplicate_null = source.replacen(
            "\"version\":1",
            "\"intent_digest\":null,\"intent_digest\":null,\"version\":1",
            1,
        );
        assert!(serde_json::from_str::<LineageHoldAcquireRequest>(&duplicate_null).is_err());
        let duplicate_target =
            source.replacen("\"sequence\":2", "\"sequence\":2,\"sequence\":2", 1);
        assert!(serde_json::from_str::<LineageHoldAcquireRequest>(&duplicate_target).is_err());
        let unknown_target = source.replacen("\"sequence\":2", "\"extra\":true,\"sequence\":2", 1);
        assert!(serde_json::from_str::<LineageHoldAcquireRequest>(&unknown_target).is_err());
        let unknown = source.replacen("\"version\":1", "\"extra\":true,\"version\":1", 1);
        assert!(serde_json::from_str::<LineageHoldAcquireRequest>(&unknown).is_err());
        assert!(serde_json::from_str::<LineageHoldAcquireRequest>("[]").is_err());
        let mut array_target = acquire();
        array_target["targets"] = json!([[AGENT, 2, format!("ex1_{}", "2".repeat(64)), PIN]]);
        assert!(parse::<LineageHoldAcquireRequest>(&array_target).is_err());

        let list = format!(
            "{{\"version\":1,\"expected_store_id\":\"{STORE}\",\"state\":\"all\",\"limit\":1,\"after_hold_id\":null,\"after_hold_id\":null}}"
        );
        assert!(serde_json::from_str::<LineageHoldListRequest>(&list).is_err());
        let lookup = format!(
            "{{\"version\":1,\"expected_store_id\":\"{STORE}\",\"request_id\":\"x\",\"fingerprint\":\"{FINGERPRINT}\",\"request_id\":\"x\"}}"
        );
        assert!(serde_json::from_str::<LineageHoldLookupRequest>(&lookup).is_err());
        let release = format!(
            "{{\"version\":1,\"expected_store_id\":\"{STORE}\",\"hold_id\":\"{HOLD}\",\"signature\":\"{}\",\"version\":1}}",
            "ab".repeat(64)
        );
        assert!(serde_json::from_str::<LineageHoldReleaseRequest>(&release).is_err());
    }

    /// Verifies target count, sequence, identity, and request boundaries against the codec.
    #[test]
    fn lineage_hold_wire_acquire_validates_domain_bounds() {
        for request_id in [
            String::new(),
            "  \t".into(),
            "a\0b".into(),
            "a".repeat(513),
            "😀".repeat(513),
        ] {
            let mut value = acquire();
            value["request_id"] = json!(request_id);
            assert!(
                LineageHoldAcquire::try_from(parse::<LineageHoldAcquireRequest>(&value).unwrap())
                    .is_err()
            );
        }
        let mut value = acquire();
        value["request_id"] = json!("😀".repeat(512));
        assert!(
            LineageHoldAcquire::try_from(parse::<LineageHoldAcquireRequest>(&value).unwrap())
                .is_ok()
        );
        value["targets"] = json!([]);
        assert!(
            LineageHoldAcquire::try_from(parse::<LineageHoldAcquireRequest>(&value).unwrap())
                .is_err()
        );
        let targets: Vec<_> = (1..=21)
            .map(|seq| {
                let mut target = target();
                target["sequence"] = json!(seq);
                target
            })
            .collect();
        value["targets"] = json!(&targets[..20]);
        assert!(
            LineageHoldAcquire::try_from(parse::<LineageHoldAcquireRequest>(&value).unwrap())
                .is_ok()
        );
        value["targets"] = json!(targets);
        assert!(parse::<LineageHoldAcquireRequest>(&value).is_err());
        value["targets"] = json!([target(), target()]);
        assert!(
            LineageHoldAcquire::try_from(parse::<LineageHoldAcquireRequest>(&value).unwrap())
                .is_err()
        );
        value["targets"] = json!([target()]);
        value["targets"][0]["sequence"] = json!(0);
        assert!(
            LineageHoldAcquire::try_from(parse::<LineageHoldAcquireRequest>(&value).unwrap())
                .is_err()
        );
        value["targets"][0]["sequence"] = json!(u32::MAX);
        assert!(
            LineageHoldAcquire::try_from(parse::<LineageHoldAcquireRequest>(&value).unwrap())
                .is_ok()
        );
        value["targets"][0]["sequence"] = json!(u64::from(u32::MAX) + 1);
        assert!(parse::<LineageHoldAcquireRequest>(&value).is_err());
        value["targets"][0]["sequence"] = json!(2);
        for (field, bad) in [
            ("agent_id", "ag-20261307-010203-0123456789"),
            ("execution_ref", "ex1_bad"),
            ("pin_id", "pin_bad"),
        ] {
            let mut bad_target = value.clone();
            bad_target["targets"][0][field] = json!(bad);
            assert!(parse::<LineageHoldAcquireRequest>(&bad_target).is_err());
        }
        value["expected_store_id"] = json!("store_00112233445566778899AABBCCDDEEFF");
        assert!(parse::<LineageHoldAcquireRequest>(&value).is_err());
        value = acquire();
        value["version"] = json!(2);
        assert!(
            LineageHoldAcquire::try_from(parse::<LineageHoldAcquireRequest>(&value).unwrap())
                .is_err()
        );
    }

    /// Requires explicit list fields and validates all non-acquire versions and bounds.
    #[test]
    fn lineage_hold_wire_lookup_and_list_validate_before_use() {
        let mut lookup = json!({ "version": 1, "expected_store_id": STORE, "request_id": "x", "fingerprint": FINGERPRINT });
        let request: LineageHoldLookupRequest = parse(&lookup).unwrap();
        assert!(request.validate().is_ok());
        assert_eq!(hex_digest(&request.fingerprint.into_bytes()), FINGERPRINT);
        for bad in ["", " ", "x\0y"] {
            lookup["request_id"] = json!(bad);
            assert!(
                parse::<LineageHoldLookupRequest>(&lookup)
                    .unwrap()
                    .validate()
                    .is_err()
            );
        }
        lookup["request_id"] = json!("a".repeat(513));
        assert!(
            parse::<LineageHoldLookupRequest>(&lookup)
                .unwrap()
                .validate()
                .is_err()
        );
        lookup["request_id"] = json!("😀".repeat(512));
        assert!(
            parse::<LineageHoldLookupRequest>(&lookup)
                .unwrap()
                .validate()
                .is_ok()
        );
        lookup["version"] = json!(2);
        assert!(
            parse::<LineageHoldLookupRequest>(&lookup)
                .unwrap()
                .validate()
                .is_err()
        );

        let mut list = json!({ "version": 1, "expected_store_id": STORE, "state": "all", "limit": 1, "after_hold_id": null });
        for state in ["all", "active", "released"] {
            list["state"] = json!(state);
            for limit in [1, 100] {
                list["limit"] = json!(limit);
                assert!(
                    parse::<LineageHoldListRequest>(&list)
                        .unwrap()
                        .validate()
                        .is_ok()
                );
            }
        }
        for limit in [0, 101] {
            list["limit"] = json!(limit);
            assert!(
                parse::<LineageHoldListRequest>(&list)
                    .unwrap()
                    .validate()
                    .is_err()
            );
        }
        list["limit"] = json!(1);
        list["after_hold_id"] = json!(HOLD);
        assert!(
            parse::<LineageHoldListRequest>(&list)
                .unwrap()
                .validate()
                .is_ok()
        );
        list["after_hold_id"] = json!("hold_0123456789ab4cde8f0123456789abcD");
        assert!(parse::<LineageHoldListRequest>(&list).is_err());
        list["after_hold_id"] = Value::Null;
        list["version"] = json!(0);
        assert!(
            parse::<LineageHoldListRequest>(&list)
                .unwrap()
                .validate()
                .is_err()
        );
        list["version"] = json!(1);
        list["state"] = json!("unknown");
        assert!(parse::<LineageHoldListRequest>(&list).is_err());
        list["state"] = json!("all");
        for field in ["state", "limit", "after_hold_id"] {
            let mut missing = list.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(parse::<LineageHoldListRequest>(&missing).is_err());
        }
    }

    /// Release proof errors and Debug must not disclose proof content or bytes.
    #[test]
    fn lineage_hold_wire_release_proof_is_never_printed() {
        let proof = "ab".repeat(64);
        let mut value = json!({ "version": 1, "expected_store_id": STORE, "hold_id": HOLD, "signature": proof });
        let request: LineageHoldReleaseRequest = parse(&value).unwrap();
        assert!(request.validate().is_ok());
        assert!(request.signature_bytes().iter().all(|byte| *byte == 0xab));
        let debug = format!("{request:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains(&proof));
        assert!(!debug.contains("171, 171"));
        value["version"] = json!(2);
        assert!(
            parse::<LineageHoldReleaseRequest>(&value)
                .unwrap()
                .validate()
                .is_err()
        );
        value["version"] = json!(1);
        for bad in [
            "a".repeat(127),
            "a".repeat(129),
            "A".repeat(128),
            "g".repeat(128),
            "é".repeat(64),
        ] {
            value["signature"] = json!(bad);
            let error = parse::<LineageHoldReleaseRequest>(&value)
                .unwrap_err()
                .to_string();
            assert!(!error.contains(&bad));
        }
        for bad in [
            json!(123456789),
            json!(true),
            Value::Null,
            json!(["private-proof"]),
            json!({"private-proof": true}),
        ] {
            value["signature"] = bad;
            let error = parse::<LineageHoldReleaseRequest>(&value)
                .unwrap_err()
                .to_string();
            assert!(!error.contains("123456789"));
            assert!(!error.contains("true"));
            assert!(!error.contains("private-proof"));
        }
    }

    /// Responses expose only recovery facts and preserve stored target/root order.
    #[test]
    fn lineage_hold_wire_responses_omit_proofs_and_preserve_order() {
        let mut receipt = receipt();
        let mut second_target = receipt.targets[0].clone();
        second_target.sequence = 3;
        receipt.targets.push(second_target);
        let second_root: AgentId = "ag-20261107-010204-0123456789".parse().unwrap();
        receipt.canonical_roots.push(second_root.clone());
        let observation = LineageHoldWireObservation {
            receipt: receipt.clone(),
            state: LineageHoldWireState::Active,
        };
        let acquire = LineageHoldAcquireResponse {
            receipt,
            state: LineageHoldWireState::Released,
            replayed: true,
        };
        let serialized = serde_json::to_value(&acquire).unwrap();
        assert_eq!(serialized["receipt"]["fingerprint"], FINGERPRINT);
        assert_eq!(serialized["receipt"]["targets"][0]["sequence"], 2);
        assert_eq!(serialized["receipt"]["targets"][1]["sequence"], 3);
        assert_eq!(
            serialized["receipt"]["canonical_roots"][1],
            second_root.as_str()
        );
        assert_eq!(serialized["state"], "released");
        assert_eq!(serialized["replayed"], true);
        for forbidden in ["verifying_key", "signature", "request_id", "intent_digest"] {
            assert!(!serialized.to_string().contains(forbidden));
        }
        let store_id: StoreId = STORE.parse().unwrap();
        let missing =
            serde_json::to_value(LineageHoldLookupResponse::new(store_id.clone(), None)).unwrap();
        assert_eq!(missing["found"], false);
        assert!(missing.get("receipt").is_none());
        assert!(missing.get("state").is_none());
        let found = serde_json::to_value(LineageHoldLookupResponse::new(
            store_id.clone(),
            Some(observation.clone()),
        ))
        .unwrap();
        assert_eq!(found["found"], true);
        assert_eq!(found["receipt"]["hold_id"], HOLD);
        assert_eq!(found["state"], "active");
        let page = LineageHoldListResponse {
            store_id: store_id.clone(),
            holds: vec![observation],
            next_after_hold_id: None,
        };
        assert_eq!(
            serde_json::to_value(page).unwrap()["holds"][0]["receipt"]["fingerprint"],
            FINGERPRINT
        );
        let missing_release =
            serde_json::to_value(LineageHoldReleaseResponse::new(store_id.clone(), None)).unwrap();
        assert_eq!(missing_release["found"], false);
        assert!(missing_release.get("receipt").is_none());
        assert!(missing_release.get("state").is_none());
        let found_release = serde_json::to_value(LineageHoldReleaseResponse::new(
            store_id,
            Some((HOLD.parse().unwrap(), true)),
        ))
        .unwrap();
        assert_eq!(found_release["found"], true);
        assert_eq!(found_release["hold_id"], HOLD);
        assert_eq!(found_release["state"], "released");
        assert_eq!(found_release["already_released"], true);
        assert!(!found_release.to_string().contains("signature"));
    }
}
