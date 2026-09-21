//! Byte-exact parity tests against Python-generated canonical JSON vectors.
//!
//! Fixtures live in `tests/fixtures/canonical/vectors.json`, produced by the
//! *actual* Python functions and are frozen; the generator is not part of this
//! repository (see `docs/artifact-snapshots.md`). This test never reformats
//! Python's output — it feeds each vector's `payload` into
//! `agent_run::canonical::dumps` and asserts the raw bytes and SHA-256 match
//! what CPython actually produced.

use agent_run_domain::canonical::{dumps, python_float_repr, sha256_hex};
use serde_json::Value;
use sha2::Digest;

fn vectors() -> Value {
    let raw = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/canonical/vectors.json"
    ))
    .expect("vectors.json fixture is present (frozen; do not regenerate)");
    serde_json::from_str(&raw).expect("vectors.json is valid JSON")
}

/// Minimal standard-alphabet (RFC 4648) base64 decoder, just for reading
/// this test's own fixture bytes -- not worth a new crate dependency.
fn b64_decode(s: &str) -> Vec<u8> {
    fn value(byte: u8) -> Option<u8> {
        match byte {
            b'A'..=b'Z' => Some(byte - b'A'),
            b'a'..=b'z' => Some(byte - b'a' + 26),
            b'0'..=b'9' => Some(byte - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let mut out = Vec::new();
    let mut buffer = 0u32;
    let mut bits = 0u32;
    for byte in s.bytes() {
        let Some(v) = value(byte) else { continue };
        buffer = (buffer << 6) | v as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buffer >> bits) as u8);
        }
    }
    out
}

/// Rebuild the exact `serde_json::Value` a primitive vector describes.
/// Scalars needing exact bit fidelity (ints beyond safe f64 range, floats)
/// carry their own decimal/hex encoding instead of a bare JSON literal, so
/// this loader never routes them through a lossy intermediate.
fn primitive_value(case: &Value) -> Value {
    match case["kind"].as_str().unwrap() {
        "json" | "string" => case["value"].clone(),
        "int" => {
            let decimal = case["decimal"].as_str().unwrap();
            if let Ok(i) = decimal.parse::<i64>() {
                Value::from(i)
            } else {
                Value::from(decimal.parse::<u64>().expect("int vector fits i64 or u64"))
            }
        }
        "float" => {
            let bits = u64::from_str_radix(case["bits_hex"].as_str().unwrap(), 16)
                .expect("bits_hex is valid hex");
            Value::from(f64::from_bits(bits))
        }
        other => panic!("unknown primitive vector kind: {other}"),
    }
}

#[test]
fn primitives_match_python_byte_for_byte() {
    let vectors = vectors();
    let cases = vectors["primitives"].as_array().expect("primitives array");
    assert!(!cases.is_empty(), "fixture must not be empty");
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let ensure_ascii = case["ensure_ascii"].as_bool().unwrap();
        let value = primitive_value(case);
        let expected = b64_decode(case["expected_base64"].as_str().unwrap());
        let actual = dumps(&value, ensure_ascii);
        assert_eq!(
            actual, expected,
            "case {name}: canonical bytes diverge from Python (ensure_ascii={ensure_ascii})"
        );
        let expected_sha = case["expected_sha256"].as_str().unwrap();
        assert_eq!(
            sha256_hex(&value, ensure_ascii),
            expected_sha,
            "case {name}: sha256 diverges from Python"
        );

        if case["kind"] == "float" {
            let f = value.as_f64().unwrap();
            let expected_repr = case["python_repr"].as_str().unwrap();
            assert_eq!(
                python_float_repr(f),
                expected_repr,
                "case {name}: python_float_repr diverges from CPython's repr()"
            );
        }
    }
}

#[test]
fn real_documents_match_python_byte_for_byte() {
    let vectors = vectors();
    let docs = vectors["documents"].as_array().expect("documents array");
    assert!(!docs.is_empty(), "fixture must not be empty");
    for doc in docs {
        let name = doc["name"].as_str().unwrap();
        let ensure_ascii = doc["ensure_ascii"].as_bool().unwrap();
        let trailing_newline = doc["trailing_newline"].as_bool().unwrap();
        let payload = &doc["payload"];
        let mut actual = dumps(payload, ensure_ascii);
        if trailing_newline {
            actual.push(b'\n');
        }
        let expected = b64_decode(doc["document_base64"].as_str().unwrap());
        assert_eq!(
            actual,
            expected,
            "document {name} ({}): canonical bytes diverge from Python",
            doc["source"].as_str().unwrap_or("")
        );
        let expected_sha = doc["sha256"].as_str().unwrap();
        let actual_sha = format!("{:x}", sha2::Sha256::digest(&actual));
        assert_eq!(
            actual_sha, expected_sha,
            "document {name}: sha256 diverges from Python"
        );
    }
}
