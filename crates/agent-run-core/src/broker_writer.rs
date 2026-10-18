//! In-process producer lanes of the resident broker.
//!
//! The broker owns the one [`crate::writer::WriterHandle`]. Each broker
//! subsystem submits through its own [`crate::broker_writer::BrokerLane`] (one producer identity per
//! subsystem, so one subsystem's ordered stream never blocks another's). A lane
//! keeps its head lock across ordinal allocation, submission, retry and the
//! final outcome, retries the SAME envelope (same operation id, ordinal and
//! original deadline) while the writer refuses before acceptance or the outcome
//! is unknown, and never converts an unknown outcome into success or into proof
//! that nothing happened. Authority is always explicit: `Broker` unless the
//! caller names the worker authority of an authenticated worker tool call.

use crate::{
    Error, Result,
    domain::{AgentId, Outcome},
    persist::into_result_totals,
    storage_client::{Totals, expired, pause, remaining, transient},
    writer::WriterHandle,
};
use agent_run_domain::storage::{
    MonoDeadline, OperationId, ProducerId, ProducerKind, RejectCode, SubmitResult, UnknownCause,
    WriteAuthority,
};
use agent_run_platform::monotonic;
use agent_run_store::ops::{
    AppendJournal, CompleteTerminalCommands, FinishTerminal, JournalFragment, LocalEnvelope,
    LocalOp, RemoteOp, WriteEnvelope, WriteOp,
};
use serde_json::Value;
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};
use tokio::sync::Mutex;

/// Default budget of one broker operation from the moment it reaches the head
/// of its lane until its original monotonic deadline.
pub const BROKER_BUDGET: Duration = Duration::from_secs(30);

/// Factory of the broker's producer lanes over one writer handle.
#[derive(Clone)]
pub struct BrokerWriter {
    /// The resident writer.
    handle: WriterHandle,
    /// Per-process instance shared by every lane identity of this broker.
    instance: Arc<str>,
    /// Lanes by subsystem name: one subsystem is exactly one producer, so every
    /// caller of the same name shares one ordinal sequence and head lock.
    lanes: Arc<std::sync::Mutex<std::collections::HashMap<String, BrokerLane>>>,
}

impl BrokerWriter {
    /// A factory over `handle`.
    pub fn new(handle: WriterHandle) -> Self {
        Self {
            handle,
            instance: uuid::Uuid::new_v4().simple().to_string().into(),
            lanes: Arc::default(),
        }
    }

    /// The raw handle, for health and status reads.
    pub fn handle(&self) -> &WriterHandle {
        &self.handle
    }

    /// The lane of the named subsystem (`admission`, `commands`, `pools`,
    /// `workers`, `delivery`, `reconcile`, `retention`, `services`, `capacity`,
    /// `maintenance`, ...), created on first use. Every call with the same name
    /// returns a clone of the same lane (same producer, same ordinals).
    pub fn lane(&self, scope: &str) -> BrokerLane {
        let mut lanes = self
            .lanes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        lanes
            .entry(scope.to_owned())
            .or_insert_with(|| BrokerLane {
                handle: self.handle.clone(),
                id: ProducerId {
                    kind: ProducerKind::Broker,
                    scope: Some(scope.to_owned()),
                    instance: self.instance.to_string(),
                },
                head: Arc::new(Mutex::new(1)),
            })
            .clone()
    }
}

/// One ordered broker producer. Cheap to clone; clones share the head lock and
/// ordinal sequence.
#[derive(Clone)]
pub struct BrokerLane {
    /// The resident writer.
    handle: WriterHandle,
    /// Identity stamped into every envelope.
    id: ProducerId,
    /// Head lock holding the next ordinal.
    head: Arc<Mutex<u64>>,
}

/// One built envelope in either set.
enum Built {
    /// Broker-local operation.
    Local(Box<LocalEnvelope>),
    /// Wire-decodable operation submitted in-process.
    Remote(WriteEnvelope),
}

impl BrokerLane {
    /// Borrow the sole writer's maintenance/observation interface; no SQL is exposed.
    pub fn handle(&self) -> &WriterHandle {
        &self.handle
    }
    /// The producer identity of this lane.
    pub fn id(&self) -> &ProducerId {
        &self.id
    }

    /// Appends one event row for `execution` as the broker (a producer-side
    /// diagnostic such as `supervisor_handoff_error`).
    pub async fn event(&self, execution: &AgentId, kind: &str, data: Value) -> Result<()> {
        self.remote(
            AppendJournal {
                execution: execution.clone(),
                fragments: vec![JournalFragment::Event {
                    kind: kind.to_owned(),
                    data,
                    attempt: None,
                }],
            },
            BROKER_BUDGET,
        )
        .await
        .map(drop)
    }

    /// Ends `execution` with `outcome` as the broker (preparation or launch
    /// failure with no live supervisor) and answers its still-pending commands.
    /// Both are ordered operations of this lane; a failure of the second leaves
    /// the first committed.
    pub async fn finish_terminal(&self, execution: &AgentId, outcome: &Outcome) -> Result<()> {
        self.remote(
            FinishTerminal {
                execution: execution.clone(),
                outcome: outcome.clone(),
                proof: None,
                usage: None,
            },
            BROKER_BUDGET,
        )
        .await?;
        self.remote(
            CompleteTerminalCommands {
                execution: execution.clone(),
                at: crate::domain::now(),
            },
            BROKER_BUDGET,
        )
        .await
        .map(drop)
    }

    /// Submits a broker-local operation under the broker authority and returns
    /// its typed output.
    ///
    /// # Errors
    /// See [`into_result_totals`]; an unknown outcome is an error, never success.
    /// Closed last-refusal facts and actual retry totals survive deadline expiry.
    pub async fn local<P>(&self, op: P, budget: Duration) -> Result<P::Output>
    where
        P: WriteOp + Send + 'static,
        LocalOp: From<P>,
    {
        self.local_as(WriteAuthority::Broker, op, budget).await
    }

    /// Submits a broker-local operation under an explicit `authority` (the
    /// `Worker` authority of an authenticated worker tool call for rows 44-47).
    ///
    /// # Errors
    /// See [`into_result_totals`]; retains the same typed failures as [`Self::local`].
    pub async fn local_as<P>(
        &self,
        authority: WriteAuthority,
        op: P,
        budget: Duration,
    ) -> Result<P::Output>
    where
        P: WriteOp + Send + 'static,
        LocalOp: From<P>,
    {
        let (result, totals) = self.local_outcome(authority, op, budget).await;
        into_result_totals(result, &totals)
    }

    /// Submits a broker-local operation and returns the raw final outcome.
    pub async fn local_raw<P>(
        &self,
        authority: WriteAuthority,
        op: P,
        budget: Duration,
    ) -> SubmitResult
    where
        P: WriteOp + Send + 'static,
        LocalOp: From<P>,
    {
        self.local_outcome(authority, op, budget).await.0
    }

    /// Builds one local envelope and returns its raw outcome plus payload-free retry facts.
    /// Authority and budget have the same meaning as local_raw; no retry identity is renewed.
    async fn local_outcome<P>(
        &self,
        authority: WriteAuthority,
        op: P,
        budget: Duration,
    ) -> (SubmitResult, Totals)
    where
        P: WriteOp + Send + 'static,
        LocalOp: From<P>,
    {
        let id = self.id.clone();
        self.drive(authority, budget, move |operation_id, ordinal, deadline| {
            LocalEnvelope::new(operation_id, id, ordinal, deadline, LocalOp::from(op))
                .map(|envelope| Built::Local(Box::new(envelope)))
        })
        .await
    }

    /// Submits a wire-decodable operation as the broker (for example a cancel
    /// command enqueued on behalf of the API) and returns its typed output.
    ///
    /// # Errors
    /// See [`into_result_totals`]; closed refusal/timing facts survive bounded retries.
    pub async fn remote<P>(&self, op: P, budget: Duration) -> Result<P::Output>
    where
        P: WriteOp + Send + 'static,
        RemoteOp: From<P>,
    {
        let (result, totals) = self.remote_outcome(op, budget).await;
        into_result_totals(result, &totals)
    }

    /// Submits a wire-decodable operation as the broker and returns the raw
    /// final outcome.
    pub async fn remote_raw<P>(&self, op: P, budget: Duration) -> SubmitResult
    where
        P: WriteOp + Send + 'static,
        RemoteOp: From<P>,
    {
        self.remote_outcome(op, budget).await.0
    }

    /// Builds one remote envelope for the broker and returns outcome and actual retry totals.
    /// The same envelope and original budget govern every attempt; no transport is opened.
    async fn remote_outcome<P>(&self, op: P, budget: Duration) -> (SubmitResult, Totals)
    where
        P: WriteOp + Send + 'static,
        RemoteOp: From<P>,
    {
        let id = self.id.clone();
        self.drive(
            WriteAuthority::Broker,
            budget,
            move |operation_id, ordinal, deadline| {
                WriteEnvelope::new(operation_id, id, ordinal, deadline, RemoteOp::from(op))
                    .map(Built::Remote)
            },
        )
        .await
    }

    /// Runs one operation to its final outcome and closed retry totals under the head lock.
    ///
    /// The envelope is built once; every retry resubmits the identical value
    /// (same identity, ordinal and original deadline). `Unknown` is sticky:
    /// only a definitive receipt, a durable rejection or the deadline ends it.
    /// The returned future owns one heap allocation so its large retry state
    /// is not embedded and copied through every caller's synchronous poll stack.
    /// First poll acquires the head; cancellation and SQL ownership are unchanged.
    fn drive<F>(
        &self,
        authority: WriteAuthority,
        budget: Duration,
        build: F,
    ) -> Pin<Box<impl Future<Output = (SubmitResult, Totals)> + Send + '_>>
    where
        F: FnOnce(OperationId, u64, MonoDeadline) -> Result<Built> + Send + 'static,
    {
        Box::pin(async move {
            let mut head = self.head.lock().await;
            let started = std::time::Instant::now();
            let mut totals = Totals::default();
            let result = async {
                let ordinal = *head;
                *head += 1;
                let (boot, now) = match monotonic::now() {
                    Ok(reading) => reading,
                    Err(error) => return rejected(&Error::Io(error)),
                };
                let operation_id = OperationId::new(&boot, now);
                let deadline = MonoDeadline {
                    boot_id: boot,
                    ns: now.saturating_add(budget.as_nanos().min(u128::from(u64::MAX)) as u64),
                };
                let built = match build(operation_id, ordinal, deadline.clone()) {
                    Ok(built) => built,
                    Err(error) => return rejected(&error),
                };
                totals.identity = Some(match &built {
                    Built::Local(envelope) => (
                        envelope.header().operation_id.clone(),
                        envelope.kind().wire_id(),
                    ),
                    Built::Remote(envelope) => (
                        envelope.header().operation_id.clone(),
                        envelope.kind().wire_id(),
                    ),
                });
                let mut uncertain: Option<UnknownCause> = None;
                let mut backoff = Duration::from_millis(20);
                loop {
                    if remaining(&deadline).is_none() {
                        return expired(uncertain);
                    }
                    totals.attempts = totals.attempts.saturating_add(1);
                    let result = match &built {
                        Built::Local(envelope) => {
                            self.handle
                                .local(authority.clone(), (**envelope).clone())
                                .await
                        }
                        Built::Remote(envelope) => {
                            self.handle
                                .submit(authority.clone(), envelope.clone())
                                .await
                        }
                    };
                    match result {
                        SubmitResult::Committed(receipt) => {
                            return SubmitResult::Committed(receipt);
                        }
                        SubmitResult::Rejected(rejection) if rejection.durable => {
                            return SubmitResult::Rejected(rejection);
                        }
                        SubmitResult::Rejected(rejection)
                            if rejection.code == RejectCode::StorageUnavailable =>
                        {
                            totals.last_rejection = Some(rejection);
                            totals.last_not_enqueued = None;
                            pause(&mut backoff, &deadline).await;
                        }
                        SubmitResult::Rejected(rejection) => {
                            return match uncertain {
                                None => SubmitResult::Rejected(rejection),
                                Some(cause) => SubmitResult::Unknown(cause),
                            };
                        }
                        SubmitResult::NotEnqueued(reason) if transient(reason) => {
                            totals.last_not_enqueued = Some(reason);
                            totals.last_rejection = None;
                            pause(&mut backoff, &deadline).await;
                        }
                        SubmitResult::NotEnqueued(reason) => {
                            return match uncertain {
                                None => SubmitResult::NotEnqueued(reason),
                                Some(cause) => SubmitResult::Unknown(cause),
                            };
                        }
                        SubmitResult::Unknown(cause) => {
                            uncertain = Some(cause);
                            pause(&mut backoff, &deadline).await;
                        }
                    }
                }
            }
            .await;
            totals.elapsed = started.elapsed();
            (result, totals)
        })
    }
}

/// A gate rejection for an envelope that could not even be built (invalid
/// payload, oversize bound): non-durable, nothing reached the writer.
fn rejected(error: &Error) -> SubmitResult {
    SubmitResult::Rejected(agent_run_domain::storage::StorageRejection::from_error(
        error,
        agent_run_domain::storage::RejectStage::Gate,
    ))
}
