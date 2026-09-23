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
//!   is quota exhaustion; anything else carries no exhaustion evidence. The
//!   same schema types the top-level assistant frame's optional `error` as
//!   one of `authentication_failed`, `oauth_org_not_allowed`,
//!   `account_on_hold`, `verification_required`, `billing_error`,
//!   `rate_limit`, `overloaded`, `invalid_request`, `model_not_found`,
//!   `server_error`, `unknown`, `max_output_tokens`,
//!   `cloud_credential_error`. [`ClaudeSignals`] tracks both in order: a
//!   later `allowed`/`allowed_warning` (or malformed) rate-limit event clears
//!   a rejection, and a later assistant error of another class is the
//!   terminal cause instead.
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

/// The current authoritative native state of one Claude Code attempt, fed
/// every top-level stdout frame in order and asked once at the failed result.
#[derive(Debug, Default)]
pub struct ClaudeSignals {
    /// Frames observed so far (orders the two signals below).
    seen: u64,
    /// The latest rate-limit state: `Some` only while a rejection stands.
    rejected: Option<(u64, NativeFailure)>,
    /// The latest main-loop assistant `error` control and when it came.
    error: Option<(u64, String)>,
}

impl ClaudeSignals {
    /// Observes one top-level frame. Rate-limit events replace the standing
    /// state (a rejection of a known window sets it; any other status or a
    /// malformed event clears it). A main-loop assistant frame's `error`
    /// control is recorded; subagent frames (non-null `parent_tool_use_id`)
    /// and every text field are ignored.
    pub fn observe(&mut self, frame: &Value) {
        self.seen += 1;
        match frame["type"].as_str() {
            Some("rate_limit_event") => {
                self.rejected = claude_frame(frame).map(|failure| (self.seen, failure));
            }
            Some("assistant") if frame["parent_tool_use_id"].is_null() => {
                if let Some(error) = frame["error"].as_str() {
                    self.error = Some((self.seen, error.to_owned()));
                }
            }
            _ => {}
        }
    }

    /// The disposition of a failed turn from the current state: quota
    /// exhaustion only while a rejection stands and no later assistant error
    /// of another class ended the turn; otherwise that error's class, or
    /// `None` when no authoritative control was seen.
    pub fn terminal(&self) -> Option<NativeFailure> {
        let later_error = self.error.as_ref().filter(|(at, _)| {
            self.rejected
                .as_ref()
                .is_none_or(|(rejected, _)| at > rejected)
        });
        match (later_error, &self.rejected) {
            (Some((_, error)), _) if error != "rate_limit" => Some(match error.as_str() {
                "authentication_failed"
                | "oauth_org_not_allowed"
                | "account_on_hold"
                | "verification_required"
                | "cloud_credential_error" => NativeFailure::Auth,
                "overloaded" => NativeFailure::Throttled,
                _ => NativeFailure::Other,
            }),
            (_, Some((_, failure))) => Some(failure.clone()),
            (Some(_), None) => Some(NativeFailure::Throttled),
            (None, None) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{claude_frame, codex_turn_error, ClaudeSignals, NativeFailure};
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

    /// Signal order decides: a standing rejection is quota; an `allowed`
    /// event or a later non-rate-limit assistant error supersedes it; a
    /// rate-limit assistant error without a rejection is throttling; subagent
    /// errors and quoted text never count.
    #[test]
    fn claude_signal_state_follows_protocol_order() {
        let rejected = json!({"type":"rate_limit_event","rate_limit_info":{"status":"rejected","rateLimitType":"seven_day"}});
        let allowed = json!({"type":"rate_limit_event","rate_limit_info":{"status":"allowed"}});
        let error = |kind: &str| json!({"type":"assistant","parent_tool_use_id":null,"error":kind,"message":{"content":[]}});
        let run = |frames: &[&serde_json::Value]| {
            let mut state = ClaudeSignals::default();
            for frame in frames {
                state.observe(frame);
            }
            state.terminal()
        };
        assert!(matches!(
            run(&[&rejected]),
            Some(NativeFailure::QuotaExhausted { .. })
        ));
        assert!(matches!(
            run(&[&rejected, &error("rate_limit")]),
            Some(NativeFailure::QuotaExhausted { .. })
        ));
        assert_eq!(run(&[&rejected, &allowed]), None);
        assert_eq!(
            run(&[&rejected, &error("authentication_failed")]),
            Some(NativeFailure::Auth)
        );
        assert_eq!(
            run(&[&rejected, &error("server_error")]),
            Some(NativeFailure::Other)
        );
        assert_eq!(run(&[&error("rate_limit")]), Some(NativeFailure::Throttled));
        assert!(matches!(
            run(&[&error("authentication_failed"), &rejected]),
            Some(NativeFailure::QuotaExhausted { .. })
        ));
        let subagent = json!({"type":"assistant","parent_tool_use_id":"t1","error":"authentication_failed","message":{"content":[]}});
        assert!(matches!(
            run(&[&rejected, &subagent]),
            Some(NativeFailure::QuotaExhausted { .. })
        ));
        let quoted = json!({"type":"assistant","parent_tool_use_id":null,"message":{"content":[{"type":"text","text":rejected.to_string()}]}});
        assert_eq!(run(&[&quoted]), None);
        assert_eq!(
            run(&[&json!({"type":"rate_limit_event","rate_limit_info":7})]),
            None
        );
    }
}
