//! Byte-exact v1 codec for a generic hold over ordered execution lineages.
//!
//! This module performs no storage, signing, signature verification, or permission
//! checks. A 32-byte public key is accepted structurally only. Before admitting a
//! hold, operational acquire MUST use a reviewed Ed25519 library to reject
//! noncanonical, weak, and non-prime-order public keys. Computing a fingerprint
//! here does not establish that a key is usable or that release is authorized.

use crate::{
    Error, Result,
    batch::{ExecutionRef, PinId},
    domain::{AgentId, external_id},
    error::invalid,
    types::StoreId,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::HashSet, fmt, str::FromStr};

/// The only supported encoding version; versions are never silently upgraded.
pub const LINEAGE_HOLD_VERSION: u32 = 1;
/// Maximum ordered exact targets in one hold acquisition.
pub const MAX_HOLD_TARGETS: usize = 20;

const ACQUIRE_DOMAIN: &[u8] = b"agent-run:lineage-hold:acquire:v1\0";
const RELEASE_DOMAIN: &[u8] = b"agent-run:lineage-hold:release:v1\0";

/// One exact target; its positive sequence is checked during acquire encoding.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LineageHoldTarget {
    /// Exact validated agent spelling, preserved without normalization.
    pub agent_id: AgentId,
    /// Positive execution sequence within the agent's lineage.
    pub sequence: u32,
    /// Exact execution incarnation protected by this target.
    pub execution_ref: ExecutionRef,
    /// Retention pin associated with this exact target.
    pub pin_id: PinId,
}

/// Passive acquire input; encoding validates version, bounds, and exact duplicates.
///
/// This is a codec input, not an admitted hold or a public transport schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LineageHoldAcquire {
    /// Must equal [`LINEAGE_HOLD_VERSION`].
    pub version: u32,
    /// Persistent store identity, included in the fingerprint.
    pub store_id: StoreId,
    /// Nonblank NUL-free identifier of at most 512 Unicode scalar values.
    pub request_id: String,
    /// One to twenty unique exact targets; caller order is significant.
    pub targets: Vec<LineageHoldTarget>,
    /// Raw public Ed25519 key, structurally sized but not cryptographically checked.
    pub verifying_key: [u8; 32],
    /// Optional opaque intent binding; no interpretation or normalization occurs.
    pub intent_digest: Option<[u8; 32]>,
}

impl LineageHoldAcquire {
    /// Encodes the exact v1 acquire preimage, preserving request and target bytes.
    ///
    /// The layout is domain, store bytes, u16-BE request byte length, request
    /// bytes, u8 target count, ordered targets (29 ASCII agent bytes, u32-BE
    /// sequence, 32 execution bytes, 16 pin bytes), public key, and a one-byte
    /// intent presence tag followed by its 32 bytes when present.
    ///
    /// # Errors
    /// Returns validation errors for unsupported versions, malformed request
    /// identifiers, target counts outside 1..=20, zero sequences, duplicate exact
    /// targets, or invalid identifier encodings. Error messages contain no input.
    /// Public-key curve validity is explicitly outside this method's contract.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        if self.version != LINEAGE_HOLD_VERSION {
            return Err(invalid("unsupported lineage hold version"));
        }
        if self.request_id.len() > 2048 {
            return Err(invalid("hold request_id exceeds 2048 UTF-8 bytes"));
        }
        external_id("hold request_id", &self.request_id)?;
        let request_len = u16::try_from(self.request_id.len())
            .map_err(|_| invalid("hold request_id byte length is invalid"))?;
        if !(1..=MAX_HOLD_TARGETS).contains(&self.targets.len()) {
            return Err(invalid("lineage hold requires 1 to 20 targets"));
        }
        let target_count = u8::try_from(self.targets.len())
            .map_err(|_| invalid("lineage hold target count is invalid"))?;
        let mut seen = HashSet::with_capacity(self.targets.len());
        let mut bytes = Vec::new();
        bytes.extend_from_slice(ACQUIRE_DOMAIN);
        bytes.extend_from_slice(&decode_id::<16>(self.store_id.as_str(), "store_")?);
        bytes.extend_from_slice(&request_len.to_be_bytes());
        bytes.extend_from_slice(self.request_id.as_bytes());
        bytes.push(target_count);
        for target in &self.targets {
            if target.sequence == 0 {
                return Err(invalid("hold target sequence must be positive"));
            }
            if !seen.insert(target) {
                return Err(invalid("duplicate exact lineage hold target"));
            }
            let agent = target.agent_id.as_str();
            if agent.len() != 29 || !agent.is_ascii() {
                return Err(invalid("invalid hold target agent_id encoding"));
            }
            bytes.extend_from_slice(agent.as_bytes());
            bytes.extend_from_slice(&target.sequence.to_be_bytes());
            bytes.extend_from_slice(&decode_id::<32>(target.execution_ref.as_str(), "ex1_")?);
            bytes.extend_from_slice(&decode_id::<16>(target.pin_id.as_str(), "pin_")?);
        }
        bytes.extend_from_slice(&self.verifying_key);
        match self.intent_digest {
            None => bytes.push(0),
            Some(digest) => {
                bytes.push(1);
                bytes.extend_from_slice(&digest);
            }
        }
        Ok(bytes)
    }

    /// Computes SHA-256 over [`Self::canonical_bytes`] without admitting a hold.
    ///
    /// # Errors
    /// Returns the same structural validation errors as canonical encoding.
    pub fn fingerprint(&self) -> Result<[u8; 32]> {
        Ok(Sha256::digest(self.canonical_bytes()?).into())
    }
}

/// Canonical `hold_` plus 32 lowercase hexadecimal UUIDv4 digits.
///
/// This identifier is public and does not itself authorize release.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct HoldId(String);

impl HoldId {
    /// Returns the exact validated 37-byte spelling.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for HoldId {
    type Err = Error;

    /// Parses canonical lowercase simple UUIDv4, rejecting other versions/variants.
    ///
    /// # Errors
    /// Returns a validation error without echoing malformed input.
    fn from_str(value: &str) -> Result<Self> {
        let raw = decode_id::<16>(value, "hold_")?;
        if raw[6] >> 4 != 4 || raw[8] >> 6 != 2 {
            return Err(invalid("hold_id must contain an RFC4122 UUIDv4"));
        }
        Ok(Self(value.into()))
    }
}

impl fmt::Display for HoldId {
    /// Writes the canonical public identifier without any capability material.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for HoldId {
    /// Decodes persisted receipts through the same canonical identifier checks.
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

/// Encodes the 98-byte v1 release message to be signed with Ed25519.
///
/// The layout is domain, 16 store bytes, 16 hold bytes, and the 32-byte acquire
/// fingerprint. The caller must fetch the persisted fingerprint for this hold
/// and verify the signature against its persisted, cryptographically validated
/// key before release. This function never verifies or authorizes a release.
///
/// # Errors
/// Returns a validation error if an identifier cannot be encoded canonically.
pub fn release_signed_bytes(
    store_id: &StoreId,
    hold_id: &HoldId,
    fingerprint: &[u8; 32],
) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(98);
    bytes.extend_from_slice(RELEASE_DOMAIN);
    bytes.extend_from_slice(&decode_id::<16>(store_id.as_str(), "store_")?);
    bytes.extend_from_slice(&decode_id::<16>(hold_id.as_str(), "hold_")?);
    bytes.extend_from_slice(fingerprint);
    Ok(bytes)
}

/// Decodes a fixed-size prefixed lowercase hex identity without echoing input.
fn decode_id<const N: usize>(value: &str, prefix: &str) -> Result<[u8; N]> {
    let raw = value
        .strip_prefix(prefix)
        .ok_or_else(|| invalid("invalid lineage hold identifier prefix"))?;
    if raw.len() != N * 2 {
        return Err(invalid("invalid lineage hold identifier length"));
    }
    let mut decoded = [0; N];
    let (pairs, _) = raw.as_bytes().as_chunks::<2>();
    for (output, pair) in decoded.iter_mut().zip(pairs) {
        *output = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Ok(decoded)
}

/// Returns a lowercase hex digit's value, rejecting any noncanonical spelling.
fn hex_nibble(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(invalid("invalid lineage hold identifier hex")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::canonical::hex_digest;

    /// Persisted receipt decoding cannot bypass the public identifier's canonical checks.
    #[test]
    fn hold_receipt_identifier_serde_preserves_validation() {
        let valid = "hold_112233445566478899aabbccddeeff00";
        let id: HoldId = valid.parse().unwrap();
        assert_eq!(serde_json::to_value(&id).unwrap(), valid);
        assert_eq!(
            serde_json::from_value::<HoldId>(serde_json::json!(valid)).unwrap(),
            id
        );
        for invalid in [
            "hold_bad",
            "hold_112233445566178899aabbccddeeff00",
            "hold_112233445566478899AABBCCDDEEFF00",
        ] {
            assert!(serde_json::from_value::<HoldId>(serde_json::json!(invalid)).is_err());
        }
    }

    /// Builds the fixed public interoperability fixture without private key material.
    fn fixture() -> LineageHoldAcquire {
        LineageHoldAcquire {
            version: 1,
            store_id: "store_00112233445566778899aabbccddeeff".parse().unwrap(),
            request_id: "hold-fixture-1".into(),
            targets: vec![LineageHoldTarget {
                agent_id: "ag-20261107-010203-0123456789".parse().unwrap(),
                sequence: 2,
                execution_ref: format!("ex1_{}", "2".repeat(64)).parse().unwrap(),
                pin_id: "pin_112233445566478899aabbccddeeff00".parse().unwrap(),
            }],
            verifying_key: decode_id::<32>(
                "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a",
                "",
            )
            .unwrap(),
            intent_digest: None,
        }
    }

    /// Locks the cross-language fingerprint and each release field's byte boundary.
    #[test]
    fn known_answer_and_release_layout() {
        let acquire = fixture();
        let fingerprint = acquire.fingerprint().unwrap();
        assert_eq!(
            hex_digest(&fingerprint),
            "5407d58ddf96ca0e0b2fe7c7f3a74b363a7b502c121e4b594d7c0aa190d12f66"
        );
        let hold_id: HoldId = "hold_0123456789ab4cde8f0123456789abcd".parse().unwrap();
        let release = release_signed_bytes(&acquire.store_id, &hold_id, &fingerprint).unwrap();
        assert_eq!(release.len(), 98);
        assert_eq!(&release[..34], RELEASE_DOMAIN);
        assert_eq!(
            hex_digest(&release[34..50]),
            "00112233445566778899aabbccddeeff"
        );
        assert_eq!(
            hex_digest(&release[50..66]),
            "0123456789ab4cde8f0123456789abcd"
        );
        assert_eq!(&release[66..], &fingerprint);
    }

    /// Detects altered targets and reordered target vectors in the fingerprint.
    #[test]
    fn order_and_every_exact_target_component_are_bound() {
        let original = fixture();
        let expected = original.fingerprint().unwrap();
        for field in 0..4 {
            let mut changed = original.clone();
            match field {
                0 => changed.targets[0].agent_id = "ag-20261107-010204-0123456789".parse().unwrap(),
                1 => changed.targets[0].sequence += 1,
                2 => {
                    changed.targets[0].execution_ref =
                        format!("ex1_{}", "3".repeat(64)).parse().unwrap()
                }
                _ => {
                    changed.targets[0].pin_id =
                        "pin_212233445566478899aabbccddeeff00".parse().unwrap()
                }
            }
            assert_ne!(expected, changed.fingerprint().unwrap());
        }
        let mut ordered = original;
        let mut second = ordered.targets[0].clone();
        second.sequence += 1;
        ordered.targets.push(second);
        let first = ordered.fingerprint().unwrap();
        ordered.targets.reverse();
        assert_ne!(first, ordered.fingerprint().unwrap());
    }

    /// Exercises byte versus scalar limits and opaque delimiter-containing identifiers.
    #[test]
    fn request_utf8_length_is_framed_without_normalization_or_separators() {
        let mut acquire = fixture();
        let length_offset = ACQUIRE_DOMAIN.len() + 16;
        let data_offset = length_offset + 2;
        for request in ["a|b", "a:b", " a|b ", "é", "e\u{301}", "😀"] {
            acquire.request_id = request.into();
            let bytes = acquire.canonical_bytes().unwrap();
            assert_eq!(
                &bytes[length_offset..data_offset],
                &(request.len() as u16).to_be_bytes()
            );
            assert_eq!(
                &bytes[data_offset..data_offset + request.len()],
                request.as_bytes()
            );
            assert_eq!(bytes[data_offset + request.len()], 1);
        }
        acquire.request_id = "😀".repeat(512);
        assert!(acquire.fingerprint().is_ok());
        acquire.request_id.push('a');
        assert!(acquire.fingerprint().is_err());
        for request in [
            String::new(),
            " \t\n".into(),
            "a\0b".into(),
            "a".repeat(513),
            "é".repeat(513),
        ] {
            acquire.request_id = request;
            assert!(acquire.fingerprint().is_err());
        }
    }

    /// Preserves an established non-normalized spelling accepted by AgentId.
    #[test]
    fn preserves_unusual_valid_agent_ascii_space() {
        let mut acquire = fixture();
        let agent = "ag-20261106-2 0000-0000000001";
        acquire.targets[0].agent_id = agent.parse().unwrap();
        let bytes = acquire.canonical_bytes().unwrap();
        let offset = ACQUIRE_DOMAIN.len() + 16 + 2 + acquire.request_id.len() + 1;
        assert_eq!(&bytes[offset..offset + 29], agent.as_bytes());
        assert_ne!(
            acquire.fingerprint().unwrap(),
            fixture().fingerprint().unwrap()
        );
    }

    /// Rejects malformed acquisitions and accepts the exact upper target bound.
    #[test]
    fn rejects_versions_counts_duplicates_and_zero_sequence() {
        for version in [0, 2, u32::MAX] {
            let mut acquire = fixture();
            acquire.version = version;
            assert!(acquire.fingerprint().is_err());
        }
        let mut acquire = fixture();
        acquire.targets.clear();
        assert!(acquire.fingerprint().is_err());
        acquire = fixture();
        acquire.targets.push(acquire.targets[0].clone());
        assert!(acquire.fingerprint().is_err());
        acquire = fixture();
        acquire.targets[0].sequence = 0;
        assert!(acquire.fingerprint().is_err());
        acquire = fixture();
        let target = acquire.targets[0].clone();
        acquire.targets = (1..=20)
            .map(|sequence| LineageHoldTarget {
                sequence,
                ..target.clone()
            })
            .collect();
        assert!(acquire.fingerprint().is_ok());
        acquire.targets.push(LineageHoldTarget {
            sequence: 21,
            ..target
        });
        assert!(acquire.fingerprint().is_err());
    }

    /// Binds store, opaque request, public key, and both intent presence and contents.
    #[test]
    fn store_key_and_optional_digest_are_bound() {
        let original = fixture();
        let expected = original.fingerprint().unwrap();
        let mut changed = original.clone();
        changed.verifying_key[0] ^= 1;
        assert_ne!(expected, changed.fingerprint().unwrap());
        changed = original.clone();
        changed.intent_digest = Some([0; 32]);
        let zero_digest = changed.fingerprint().unwrap();
        assert_ne!(expected, zero_digest);
        changed.intent_digest = Some([1; 32]);
        assert_ne!(zero_digest, changed.fingerprint().unwrap());
        changed = original.clone();
        changed.request_id.push('|');
        assert_ne!(expected, changed.fingerprint().unwrap());
        changed = original;
        changed.store_id = "store_10112233445566778899aabbccddeeff".parse().unwrap();
        assert_ne!(expected, changed.fingerprint().unwrap());
    }

    /// Keeps malformed identities out of encoding and out of validation diagnostics.
    #[test]
    fn rejects_invalid_identifiers_without_echoing_inputs() {
        for invalid_id in [
            "hold_0123456789ab4cde8f0123456789abcD",
            "hold_0123456789ab3cde8f0123456789abcd",
            "hold_0123456789ab4cde7f0123456789abcd",
            "hold_01234567-89ab-4cde-8f01-23456789abcd",
            "hold_0123456789ab4cde8f0123456789abcg",
            "hold_secret-must-not-appear",
        ] {
            let error = invalid_id.parse::<HoldId>().unwrap_err();
            assert!(!error.to_string().contains(invalid_id));
        }
        assert!(
            "store_00112233445566778899AABBCCDDEEFF"
                .parse::<StoreId>()
                .is_err()
        );
        assert!("ag-20261307-010203-0123456789".parse::<AgentId>().is_err());
        assert!(
            format!("ex1_{}", "G".repeat(64))
                .parse::<ExecutionRef>()
                .is_err()
        );
        assert!(
            "pin_112233445566378899aabbccddeeff00"
                .parse::<PinId>()
                .is_err()
        );
        assert!(decode_id::<16>("hold_éééééééééééééééé", "hold_").is_err());
    }

    /// Prevents structural acceptance from being mistaken for curve verification.
    #[test]
    fn structural_key_acceptance_does_not_claim_curve_validation() {
        let mut acquire = fixture();
        acquire.verifying_key = [0; 32];
        assert!(acquire.canonical_bytes().is_ok());
    }
}
