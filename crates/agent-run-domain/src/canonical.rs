//! Byte-exact equivalent of CPython's
//! `json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=...)`.
//!
//! The Rust binary must reproduce, byte for byte, the canonical JSON the Python
//! release already hashed and persisted (role-plan `config_revision`,
//! config-snapshot and runtime-snapshot-index documents, answer proofs,
//! `request_json`/`identity_json` rows). Any divergence makes an existing run
//! unresumable or a replay falsely rejected, so this is the one serializer for
//! persisted cross-version boundaries (see `docs/artifact-snapshots.md`).
//!
//! Two Python call sites are covered, selected by `ensure_ascii`:
//! - `ensure_ascii=True` (Python's default; used by role_plan/snapshot
//!   config/runtime-snapshot-index/answer-proof documents): non-ASCII
//!   characters are escaped as `\uXXXX`, with UTF-16 surrogate pairs for
//!   astral code points.
//! - `ensure_ascii=False` (used by `request_json`/`identity_json`/capacity
//!   route payloads/context-receipt keys): non-ASCII characters are written
//!   as raw UTF-8.
//!
//! `sort_keys=True` and `separators=(",", ":")` are constant across every
//! persisted/hashed call site found in the inventory, so they are not
//! parameterized.
//!
//! ponytail: `serde_json::Value` numbers are limited to i64/u64/f64
//! (no `arbitrary_precision` feature — that flag is process-wide via Cargo
//! feature unification and would change float/number handling for every
//! other `serde_json::Value` consumer in this crate, which is out of scope
//! for this spike). No inventoried document carries a Python int outside
//! that range today. If one ever does, add a small typed integer variant
//! here rather than flipping the crate-wide feature.

use serde_json::{Number, Value};
use sha2::{Digest, Sha256};

/// Serialize `value` exactly as CPython's
/// `json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=ensure_ascii)`
/// would, returning the raw (valid UTF-8) bytes.
pub fn dumps(value: &Value, ensure_ascii: bool) -> Vec<u8> {
    let mut out = Vec::new();
    write_value(value, ensure_ascii, &mut out);
    out
}

/// `dumps` followed by SHA-256, lowercase hex — matches
/// `hashlib.sha256(json.dumps(...).encode("utf-8")).hexdigest()`.
pub fn sha256_hex(value: &Value, ensure_ascii: bool) -> String {
    format!("{:x}", Sha256::digest(dumps(value, ensure_ascii)))
}

fn write_value(value: &Value, ensure_ascii: bool, out: &mut Vec<u8>) {
    match value {
        Value::Null => out.extend_from_slice(b"null"),
        Value::Bool(true) => out.extend_from_slice(b"true"),
        Value::Bool(false) => out.extend_from_slice(b"false"),
        Value::Number(n) => write_number(n, out),
        Value::String(s) => write_string(s, ensure_ascii, out),
        Value::Array(items) => {
            out.push(b'[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                write_value(item, ensure_ascii, out);
            }
            out.push(b']');
        }
        Value::Object(map) => {
            out.push(b'{');
            // Python's `sort_keys=True` orders by Unicode code point
            // (`str.__lt__`). Rust's `str`/`String` `Ord` compares UTF-8
            // bytes lexicographically, which is the same order for valid
            // UTF-8, so a plain sort matches without decoding to `char`.
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            for (index, key) in keys.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                write_string(key, ensure_ascii, out);
                out.push(b':');
                write_value(&map[key.as_str()], ensure_ascii, out);
            }
            out.push(b'}');
        }
    }
}

fn write_string(s: &str, ensure_ascii: bool, out: &mut Vec<u8>) {
    out.push(b'"');
    for c in s.chars() {
        let cp = c as u32;
        match c {
            '"' => out.extend_from_slice(b"\\\""),
            '\\' => out.extend_from_slice(b"\\\\"),
            '\u{8}' => out.extend_from_slice(b"\\b"),
            '\u{9}' => out.extend_from_slice(b"\\t"),
            '\u{a}' => out.extend_from_slice(b"\\n"),
            '\u{c}' => out.extend_from_slice(b"\\f"),
            '\u{d}' => out.extend_from_slice(b"\\r"),
            _ if cp < 0x20 => push_u_escape(cp, out),
            // Python's `ESCAPE_ASCII` regex escapes anything outside the
            // printable ASCII range 0x20-0x7e (so DEL 0x7f is escaped too),
            // with a UTF-16 surrogate pair above the BMP.
            _ if ensure_ascii && cp > 0x7e => {
                if cp < 0x10000 {
                    push_u_escape(cp, out);
                } else {
                    let n = cp - 0x10000;
                    push_u_escape(0xd800 + (n >> 10), out);
                    push_u_escape(0xdc00 + (n & 0x3ff), out);
                }
            }
            _ => {
                let mut buf = [0u8; 4];
                out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
            }
        }
    }
    out.push(b'"');
}

fn push_u_escape(code_point: u32, out: &mut Vec<u8>) {
    out.extend_from_slice(format!("\\u{code_point:04x}").as_bytes());
}

fn write_number(n: &Number, out: &mut Vec<u8>) {
    if let Some(i) = n.as_i64() {
        out.extend_from_slice(i.to_string().as_bytes());
    } else if let Some(u) = n.as_u64() {
        out.extend_from_slice(u.to_string().as_bytes());
    } else {
        // `serde_json::Number` without the `arbitrary_precision` feature is
        // always i64, u64, or f64 — this is the f64 case.
        let f = n.as_f64().expect("serde_json::Number is i64, u64, or f64");
        out.extend_from_slice(python_float_repr(f).as_bytes());
    }
}

/// Format `f` exactly as CPython's `repr(float)` (and therefore the stock
/// JSON encoder's `floatstr`) does: the shortest decimal that round-trips,
/// Python's fixed/scientific threshold, and `allow_nan=True` literals for
/// non-finite values (every inventoried call site relies on the `allow_nan`
/// default of `True`; none of them ever hash a non-finite float today).
pub fn python_float_repr(f: f64) -> String {
    if f.is_nan() {
        return "NaN".to_string();
    }
    if f.is_infinite() {
        return if f > 0.0 {
            "Infinity".to_string()
        } else {
            "-Infinity".to_string()
        };
    }
    let sign = if f.is_sign_negative() { "-" } else { "" };
    if f == 0.0 {
        return format!("{sign}0.0");
    }

    // Rust's `{:e}` without an explicit precision already yields the
    // shortest decimal digit string that round-trips to `f` -- the same
    // contract Python's float repr relies on -- just always in normalized
    // scientific form (one digit, then '.', then the rest, then "e<exp>").
    // We re-derive Python's fixed-vs-scientific layout from those digits.
    let sci = format!("{:e}", f.abs());
    let (mantissa, exp_str) = sci.split_once('e').expect("LowerExp always emits 'e'");
    let exp: i32 = exp_str
        .parse()
        .expect("LowerExp exponent is a plain integer");
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    // Position of the decimal point relative to the start of `digits`:
    // value == 0.<digits> * 10^decpt.
    let decpt = exp + 1;

    let body = if decpt <= -4 || decpt > 16 {
        let e = decpt - 1;
        let mut s = String::with_capacity(digits.len() + 6);
        s.push_str(&digits[..1]);
        if digits.len() > 1 {
            s.push('.');
            s.push_str(&digits[1..]);
        }
        s.push('e');
        s.push(if e >= 0 { '+' } else { '-' });
        s.push_str(&format!("{:02}", e.abs()));
        s
    } else if decpt <= 0 {
        format!("0.{}{digits}", "0".repeat((-decpt) as usize))
    } else if (decpt as usize) >= digits.len() {
        format!("{digits}{}.0", "0".repeat(decpt as usize - digits.len()))
    } else {
        format!(
            "{}.{}",
            &digits[..decpt as usize],
            &digits[decpt as usize..]
        )
    };
    format!("{sign}{body}")
}
