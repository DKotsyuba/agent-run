//! Strict public-key and signature checks for durable lineage-hold releases.
//!
//! Public keys are validated before an acquisition can be committed. No secret
//! key is handled by Run, and failures never include key or signature bytes.

use agent_run_domain::{Result, error::invalid};
use curve25519_dalek::edwards::CompressedEdwardsY;
use ed25519_dalek::{Signature, VerifyingKey};

/// Parses a canonical, nonidentity prime-order Ed25519 public key.
fn public_key(bytes: &[u8; 32]) -> Result<VerifyingKey> {
    let invalid_key = || invalid("invalid lineage hold public key");
    let point = CompressedEdwardsY(*bytes)
        .decompress()
        .ok_or_else(invalid_key)?;
    if point.compress().to_bytes() != *bytes || point.is_small_order() || !point.is_torsion_free() {
        return Err(invalid_key());
    }
    VerifyingKey::from_bytes(bytes).map_err(|_| invalid_key())
}

/// Rejects malformed or weak public keys before they can create a durable hold.
pub(crate) fn validate_public_key(bytes: &[u8; 32]) -> Result<()> {
    public_key(bytes).map(|_| ())
}

/// Verifies one domain-separated release message using the persisted public key.
pub(crate) fn verify_release(key: &[u8; 32], message: &[u8], signature: &[u8; 64]) -> Result<()> {
    public_key(key)?
        .verify_strict(message, &Signature::from_bytes(signature))
        .map_err(|_| invalid("invalid lineage hold release proof"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use curve25519_dalek::constants::{ED25519_BASEPOINT_POINT, EIGHT_TORSION};

    /// Decodes fixed public interoperability vectors without handling secrets.
    fn hex<const N: usize>(text: &str) -> [u8; N] {
        assert_eq!(text.len(), N * 2);
        let mut result = [0; N];
        for (index, byte) in result.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&text[index * 2..index * 2 + 2], 16).unwrap();
        }
        result
    }

    /// The independently signed release vector accepts only its exact scope.
    #[test]
    fn release_signature_known_answer_and_scope() {
        let key = hex::<32>("d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a");
        let signature = hex::<64>(
            "7a1f92ead45c670c2d1d578fae83fda73e805e131fec87f516869a2eeb4cb84f4da6685b1ae4c44607e3ed2a9ddd7e73d9f47cc9d606b6dcb434f67b14ee9106",
        );
        let mut message = hex::<98>(
            "6167656e742d72756e3a6c696e656167652d686f6c643a72656c656173653a76310000112233445566778899aabbccddeeff0123456789ab4cde8f0123456789abcd5407d58ddf96ca0e0b2fe7c7f3a74b363a7b502c121e4b594d7c0aa190d12f66",
        );
        assert!(verify_release(&key, &message, &signature).is_ok());
        for index in [0, 34, 50, 66] {
            message[index] ^= 1;
            assert!(verify_release(&key, &message, &signature).is_err());
            message[index] ^= 1;
        }
        assert!(verify_release(&key, &message, &[0; 64]).is_err());
    }

    /// A mixed-order point is not necessarily weak but must not acquire a hold.
    #[test]
    fn rejects_weak_noncanonical_and_mixed_order_keys() {
        let mut identity = [0; 32];
        identity[0] = 1;
        let noncanonical =
            hex::<32>("eeffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f");
        for key in [[0; 32], identity, noncanonical] {
            assert!(validate_public_key(&key).is_err());
        }
        let mixed = (ED25519_BASEPOINT_POINT + EIGHT_TORSION[1])
            .compress()
            .to_bytes();
        let parsed = VerifyingKey::from_bytes(&mixed).unwrap();
        assert!(!parsed.is_weak());
        assert!(!parsed.to_edwards().is_torsion_free());
        assert!(validate_public_key(&mixed).is_err());
    }
}
