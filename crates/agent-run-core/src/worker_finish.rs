//! Adapter-shared explicit completion: receipt observation precedes owned cleanup.
use crate::{
    Result,
    domain::{Outcome, Status},
    state::{Record, Store},
};
use agent_run_adapters::{EngineResult, io::Process};
use agent_run_domain::worker::FinishStatus;
use serde_json::Value;

/// Observe a native private finish tool's reply only when it contains the
/// current immutable digest. Unknown or error tool output cannot close a run.
/// The caller has already verified native tool identity and session ownership.
pub fn receipt(process: &Process, store: &Store, record: &Record, text: &str) -> Result<()> {
    if record.request.explicit_finish
        && store
            .worker_lifecycle_view(&record.id)?
            .and_then(|v| v["finish_sha256"].as_str().map(str::to_owned))
            .is_some_and(|digest| text.contains(&digest))
    {
        if store
            .worker_finish_intent(&record.id, false)?
            .is_some_and(|intent| process.redact(&intent.summary) != intent.summary)
        {
            return Err(crate::Error::Validation(
                "finish summary contains launch secret material".into(),
            ));
        }
        store.observe_worker_finish_receipt(&record.id)?;
    }
    Ok(())
}

/// Produce a result after the native finish reply, leaving cleanup and answer
/// sealing to the supervisor. Cancellation wins. Refuse secret-redacted text
/// instead of publishing a summary different from the accepted callback.
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
    let mut outcome = match intent.status {
        FinishStatus::Done => Outcome::success(session.clone()),
        FinishStatus::Blocked => Outcome::failure("worker_blocked"),
        FinishStatus::Failed => Outcome::failure("worker_failed"),
    };
    outcome.runtime_session_id = session;
    let mut answer = Some(intent.summary.clone());
    if process.redact(&intent.summary) != intent.summary {
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
