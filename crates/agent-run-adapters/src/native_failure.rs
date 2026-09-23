//! Closed classification of native runtime failure signals.
//!
//! Only an authoritative structured control signal of the harness protocol
//! can classify a failure as quota exhaustion; message, tool and model text
//! is never inspected. Provenance of the recognized shapes:
//!
//! * Codex app-server 0.155.1, schema generated locally with
//!   `codex app-server generate-json-schema`: `TurnError.codexErrorInfo` is
//!   either one of the strings `contextWindowExceeded`,
//!   `sessionBudgetExceeded`, `usageLimitExceeded`, `rateLimitExceeded`,
//!   `serverOverloaded`, `cyberPolicy`, `misalignmentPolicyViolation`,
//!   `internalServerError`, `unauthorized`, `badRequest`,
//!   `threadRollbackFailed`, `sandboxError`, `other`, or a one-key object
//!   such as `httpConnectionFailed`, `responseStreamConnectionFailed`,
//!   `responseStreamDisconnected` or `responseTooManyFailedAttempts` (each
//!   with an optional `httpStatusCode`). Only `usageLimitExceeded` is quota
//!   exhaustion; `rateLimitExceeded` is request throttling and an HTTP 429
//!   inside a connection variant is generic.
//! * Claude Code 2.1.280, SDK message schema bundled in the executable: a
//!   top-level stdout frame `{"type":"rate_limit_event","rate_limit_info":
//!   {"status":"allowed"|"allowed_warning"|"rejected","resetsAt"?:int,
//!   "rateLimitType"?:"five_hour"|"seven_day"|"seven_day_opus"|
//!   "seven_day_sonnet"|"seven_day_overage_included"|"overage",...},
//!   "uuid","session_id"}`. Only `rejected` with a recognized usage window
//!   is quota exhaustion; anything else carries no exhaustion evidence.
//!
//! Account, provider, model and attempt are never taken from these payloads;
//! the supervisor attaches them from its own trusted context.

use serde::Serialize;
use serde_json::Value;

/// The closed disposition of one native failure signal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "class", rename_all = "snake_case")]
pub enum NativeFailure {
    /// Authoritative account quota exhaustion.
    QuotaExhausted {
        /// Which protocol signal proved it (`codex.usageLimitExceeded` or
        /// `claude.rate_limit_event.rejected`).
        signal: &'static str,
        /// The provider's usage window, when the signal names one.
        window: Option<String>,
        /// Epoch seconds when the window resets, when reported.
        resets_at: Option<i64>,
    },
    /// Request throttling (rate limit, overload, generic 429): not exhaustion.
    Throttled,
    /// Authentication or authorization failure.
    Auth,
    /// The conversation exceeded the model's context window.
    ContextWindow,
    /// Transport or stream connectivity failure.
    Network,
    /// Any other, unknown or malformed signal.
    Other,
}

/// Classifies a Codex `TurnError` (the `error` of a failed turn).
pub fn codex_turn_error(error: &Value) -> NativeFailure {
    match &error["codexErrorInfo"] {
        Value::String(code) => match code.as_str() {
            "usageLimitExceeded" => NativeFailure::QuotaExhausted {
                signal: "codex.usageLimitExceeded",
                window: None,
                resets_at: None,
            },
            "rateLimitExceeded" | "serverOverloaded" => NativeFailure::Throttled,
            "unauthorized" => NativeFailure::Auth,
            "contextWindowExceeded" => NativeFailure::ContextWindow,
            _ => NativeFailure::Other,
        },
        Value::Object(variant) if variant.len() == 1 => {
            let (name, detail) = variant.iter().next().expect("one entry");
            let status = detail["httpStatusCode"].as_u64();
            match name.as_str() {
                "httpConnectionFailed"
                | "responseStreamConnectionFailed"
                | "responseStreamDisconnected"
                | "responseTooManyFailedAttempts" => {
                    if status == Some(429) {
                        NativeFailure::Throttled
                    } else if matches!(status, Some(401 | 403)) {
                        NativeFailure::Auth
                    } else {
                        NativeFailure::Network
                    }
                }
                _ => NativeFailure::Other,
            }
        }
        _ => NativeFailure::Other,
    }
}

/// Classifies one top-level Claude Code stdout frame; `Some` only for an
/// authoritative `rate_limit_event` rejection of a recognized usage window.
/// Frames of any other type (assistant, user, tool results, results) are
/// never inspected, so quota-looking text inside them cannot qualify.
pub fn claude_frame(frame: &Value) -> Option<NativeFailure> {
    if frame["type"] != "rate_limit_event" {
        return None;
    }
    let info = &frame["rate_limit_info"];
    let window = info["rateLimitType"].as_str()?;
    if info["status"] != "rejected"
        || !matches!(
            window,
            "five_hour"
                | "seven_day"
                | "seven_day_opus"
                | "seven_day_sonnet"
                | "seven_day_overage_included"
                | "overage"
        )
    {
        return None;
    }
    Some(NativeFailure::QuotaExhausted {
        signal: "claude.rate_limit_event.rejected",
        window: Some(window.to_owned()),
        resets_at: info["resetsAt"].as_i64(),
    })
}

#[cfg(test)]
mod tests {
    use super::{claude_frame, codex_turn_error, NativeFailure};
    use serde_json::json;

    /// Only `usageLimitExceeded` is exhaustion; throttling, generic 429,
    /// auth, context, network, malformed/unknown shapes and message text
    /// naming a quota code are not.
    #[test]
    fn codex_signals_follow_the_generated_schema() {
        assert!(matches!(
            codex_turn_error(&json!({"message":"x","codexErrorInfo":"usageLimitExceeded"})),
            NativeFailure::QuotaExhausted {
                signal: "codex.usageLimitExceeded",
                ..
            }
        ));
        for (error, expected) in [
            (
                json!({"codexErrorInfo":"rateLimitExceeded"}),
                NativeFailure::Throttled,
            ),
            (
                json!({"codexErrorInfo":"serverOverloaded"}),
                NativeFailure::Throttled,
            ),
            (
                json!({"codexErrorInfo":{"httpConnectionFailed":{"httpStatusCode":429}}}),
                NativeFailure::Throttled,
            ),
            (
                json!({"codexErrorInfo":"unauthorized"}),
                NativeFailure::Auth,
            ),
            (
                json!({"codexErrorInfo":"contextWindowExceeded"}),
                NativeFailure::ContextWindow,
            ),
            (
                json!({"codexErrorInfo":{"responseStreamDisconnected":{"httpStatusCode":null}}}),
                NativeFailure::Network,
            ),
            (
                json!({"codexErrorInfo":"sessionBudgetExceeded"}),
                NativeFailure::Other,
            ),
            (
                json!({"codexErrorInfo":"usage_limit_exceeded"}),
                NativeFailure::Other,
            ),
            (
                json!({"codexErrorInfo":{"usageLimitExceeded":{}}}),
                NativeFailure::Other,
            ),
            (
                json!({"codexErrorInfo":{"a":{},"b":{}}}),
                NativeFailure::Other,
            ),
            (json!({"codexErrorInfo":7}), NativeFailure::Other),
            (
                json!({"message":"usageLimitExceeded: you hit your usage limit"}),
                NativeFailure::Other,
            ),
        ] {
            assert_eq!(codex_turn_error(&error), expected, "{error}");
        }
    }

    /// Only a top-level `rate_limit_event` rejecting a known window counts;
    /// warnings, unknown windows, missing fields and the same JSON quoted in
    /// assistant, user or tool content do not.
    #[test]
    fn claude_signals_follow_the_bundled_sdk_schema() {
        let event = json!({"type":"rate_limit_event","rate_limit_info":{"status":"rejected","rateLimitType":"five_hour","resetsAt":1790200000},"uuid":"u","session_id":"s"});
        assert_eq!(
            claude_frame(&event),
            Some(NativeFailure::QuotaExhausted {
                signal: "claude.rate_limit_event.rejected",
                window: Some("five_hour".into()),
                resets_at: Some(1790200000),
            })
        );
        let quoted = event.to_string();
        for frame in [
            json!({"type":"rate_limit_event","rate_limit_info":{"status":"allowed_warning","rateLimitType":"five_hour"}}),
            json!({"type":"rate_limit_event","rate_limit_info":{"status":"rejected"}}),
            json!({"type":"rate_limit_event","rate_limit_info":{"status":"rejected","rateLimitType":"per_minute"}}),
            json!({"type":"rate_limit_event"}),
            json!({"type":"assistant","message":{"content":[{"type":"text","text":quoted}]}}),
            json!({"type":"user","message":{"content":[{"type":"tool_result","content":quoted}]}}),
            json!({"type":"result","subtype":"error_during_execution","is_error":true,"result":quoted}),
            json!({"type":"assistant","error":"rate_limit","message":{"content":[]}}),
        ] {
            assert_eq!(claude_frame(&frame), None, "{frame}");
        }
    }
}
