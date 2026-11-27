//! Adapter-shared explicit completion: receipt observation precedes owned cleanup.
use crate::{
    Result,
    domain::{AgentId, Outcome, Status},
    persist::Persist,
    state::{Record, Store},
};
use agent_run_adapters::{EngineResult, io::Process};
use agent_run_domain::worker::FinishRequest;
use agent_run_platform::verify;
use serde_json::Value;
use std::path::Path;

/// Observe a native private finish tool's reply only when it contains the
/// current immutable digest. Unknown or error tool output cannot close a run.
/// The caller has already verified native tool identity and session ownership.
/// An accepted summary carrying this launch's secret material refuses with
/// `Error::Validation` before the receipt is marked; harmless JSON formatting
/// of the exact accepted bytes is not secret material.
pub async fn receipt(
    process: &mut Process,
    store: &mut Persist,
    record: &Record,
    text: &str,
) -> Result<()> {
    if record.request.explicit_finish
        && store
            .worker_lifecycle_view(&record.id)?
            .and_then(|v| v["finish_sha256"].as_str().map(str::to_owned))
            .is_some_and(|digest| text.contains(&digest))
    {
        if store
            .worker_finish_intent(&record.id, false)?
            .is_some_and(|intent| intent.screen(|text| process.contains_secret(text)))
        {
            return Err(crate::Error::Validation(
                "finish summary contains launch secret material".into(),
            ));
        }
        process
            .observing(store.observe_worker_finish_receipt(&record.id))
            .await?;
    }
    Ok(())
}

/// Produce a result after the native finish reply, leaving cleanup and answer
/// sealing to the supervisor. Cancellation wins. Refuse a result carrying
/// launch secret material instead of publishing it; the answer is otherwise
/// the exact accepted summary or the rendering of a structured result.
pub fn result(
    process: &Process,
    store: &Store,
    record: &Record,
    session: Option<String>,
    usage: Option<Value>,
) -> Result<Option<EngineResult>> {
    if !record.request.explicit_finish {
        return Ok(None);
    }
    let Some(intent) = store.worker_finish_intent(&record.id, true)? else {
        return Ok(None);
    };
    let mut outcome = match intent.status.failure_kind() {
        None => Outcome::success(session.clone()),
        Some(kind) => Outcome::failure(kind),
    };
    outcome.runtime_session_id = session;
    let mut answer = Some(intent.answer_text()?);
    if intent.screen(|text| process.contains_secret(text)) {
        outcome = Outcome::failure("finish_secret_refused");
        answer = None;
    }
    if store.cancel_pending(&record.id)? {
        outcome.status = Status::Cancelled;
        answer = None;
    }
    Ok(Some(EngineResult {
        native_failure: None,
        outcome,
        answer,
        usage,
    }))
}

/// The only intent whose answer an explicit-finish run may seal at
/// finalization: the exact current-attempt result, provided its native finish
/// receipt was observed and it carries no launch secret material. An accepted
/// intent whose native reply was lost yields `None` whatever the run's
/// outcome, so an unobserved result never becomes even a failed run's answer.
pub fn answer(process: &Process, store: &Store, id: &AgentId) -> Result<Option<FinishRequest>> {
    Ok(store
        .worker_finish_intent(id, true)?
        .filter(|intent| !intent.screen(|text| process.contains_secret(text))))
}

/// Publish `answer.json` for a structured intent before its `answer.md` is
/// sealed. The JSON is the exact canonical finish payload; the rendered
/// `answer.md` names its digest, so it is verified through that answer's
/// durable proof. A plain intent writes nothing.
pub fn seal_structured(root: &Path, intent: &FinishRequest, system: bool) -> Result<()> {
    if intent.is_structured() {
        verify::seal_structured(root, &intent.canonical_payload()?, system)?;
    }
    Ok(())
}

/// Read the structured result behind an already verified `answer.md`.
/// `Ok(None)` for a plain answer; a missing, altered or invalid
/// `answer.json` is an integrity error.
pub fn structured_answer(root: &Path, answer: &str) -> Result<Option<FinishRequest>> {
    let Some(digest) = FinishRequest::structured_digest(answer) else {
        return Ok(None);
    };
    let text = verify::read_structured(root, digest)?;
    let intent: FinishRequest = serde_json::from_str(&text)
        .map_err(|_| crate::Error::AnswerIntegrity("structured answer is malformed".into()))?;
    intent.validate()?;
    if !intent.is_structured() {
        return Err(crate::Error::AnswerIntegrity(
            "structured answer has no structured fields".into(),
        ));
    }
    Ok(Some(intent))
}
