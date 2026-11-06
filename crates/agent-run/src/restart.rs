//! Operator-side orchestration for one planned service restart.

use crate::{
    Error, Result,
    cli::{CliBroker, CliOutput, Restart},
    error::invalid,
};
use serde_json::json;
use std::{future::Future, process::Stdio, time::Duration};

/// Run the existing system manager with only the caller's OS permissions.
pub(crate) async fn run(broker: &dyn CliBroker, output: &CliOutput, args: Restart) -> Result<i32> {
    let target = match args.target.as_deref() {
        Some(value)
            if !value.is_empty()
                && !value.starts_with('-')
                && !value.chars().any(char::is_control) =>
        {
            value.to_owned()
        }
        Some(_) => return Err(invalid("invalid service target")),
        None if cfg!(target_os = "linux") => "agent-run.service".into(),
        None if cfg!(target_os = "macos") => {
            // SAFETY: geteuid is a read-only process identity query.
            format!("gui/{}/com.agent-run.api", unsafe { libc::geteuid() })
        }
        None => {
            return Err(Error::Unsupported(
                "service restart supports Linux and macOS".into(),
            ));
        }
    };
    if args.systemd_user && !cfg!(target_os = "linux") {
        return Err(invalid("--systemd-user requires Linux"));
    }
    let manager = async move {
        let mut command = if cfg!(target_os = "linux") {
            let mut command = tokio::process::Command::new("/usr/bin/systemctl");
            if args.systemd_user {
                command.arg("--user");
            }
            command.args(["restart", "--", &target]);
            command
        } else {
            let mut command = tokio::process::Command::new("/bin/launchctl");
            command.args(["kickstart", "-k", &target]);
            command
        };
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        match tokio::time::timeout(Duration::from_secs(120), command.status()).await {
            Ok(Ok(status)) if status.success() => Manager::Succeeded,
            Ok(Ok(status)) => Manager::Failed(format!("service manager exited with {status}")),
            Ok(Err(error)) => {
                Manager::Failed(format!("service manager could not start: {}", error.kind()))
            }
            Err(_) => Manager::Unknown,
        }
    };
    drive(broker, output, args.request_id, args.recover, manager).await
}

/// A timeout after invoking the manager does not prove whether it restarted.
enum Manager {
    Succeeded,
    Failed(String),
    Unknown,
}

/// A status read or idempotent failure report may reconnect across broker shutdown.
fn transient_broker(error: &Error) -> bool {
    match error {
        Error::BrokerUnavailable => true,
        Error::Io(error) => matches!(
            error.kind(),
            std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::UnexpectedEof
        ),
        Error::Runtime(message) => message == "broker disconnected before its response",
        _ => false,
    }
}

/// Reconcile the same failure report without issuing another manager command.
async fn report_failure(broker: &dyn CliBroker, id: i64) -> Result<serde_json::Value> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let response = tokio::time::timeout_at(
            deadline,
            broker.call("_service_restart_failed", json!({"restart_id":id})),
        )
        .await
        .map_err(|_| {
            Error::Runtime("broker recovery confirmation timed out; outcome is unknown".into())
        })?;
        match response {
            Err(error) if transient_broker(&error) && tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            result => return result,
        }
    }
}

/// Reuse the broker's barrier, one manager claim and its existing recovery loop.
async fn drive<F: Future<Output = Manager>>(
    broker: &dyn CliBroker,
    output: &CliOutput,
    request_id: Option<String>,
    recover: Option<i64>,
    manager: F,
) -> Result<i32> {
    let request_id =
        request_id.unwrap_or_else(|| format!("restart-{}", uuid::Uuid::new_v4().simple()));
    let id = if let Some(id) = recover {
        if id <= 0 {
            return Err(invalid("invalid restart id"));
        }
        let recovered = report_failure(broker, id).await?;
        if recovered["restart_id"].as_i64() != Some(id) {
            let state = broker
                .call("_service_restart_status", json!({"restart_id":id}))
                .await?;
            if state["phase"] != "complete" {
                return Err(Error::Runtime(
                    "restart is not ready for recovery on this broker".into(),
                ));
            }
        }
        id
    } else {
        broker
            .call("_service_restart_begin", json!({"request_id":request_id}))
            .await?["restart_id"]
            .as_i64()
            .filter(|id| *id > 0)
            .ok_or_else(|| invalid("invalid restart receipt"))?
    };
    let mut output_error = output(
        &json!({"restart_id":id,"request_id":if recover.is_none(){Some(&request_id)}else{None}}),
    )
    .err();
    let mut manager = Some(manager);
    let mut manager_error = None;
    let mut reconnect_until = None;
    loop {
        let state = match broker
            .call("_service_restart_status", json!({"restart_id":id}))
            .await
        {
            Ok(value) => value,
            Err(error)
                if transient_broker(&error)
                    && reconnect_until
                        .is_some_and(|deadline| tokio::time::Instant::now() < deadline) =>
            {
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            }
            Err(error) => return Err(error),
        };
        match state["phase"].as_str() {
            Some("preparing") | Some("recovering") => {}
            Some("ready") if recover.is_none() => {
                let claim = broker
                    .call("_service_restart_claim", json!({"restart_id":id}))
                    .await?;
                if claim["claimed"] != true {
                    return Err(Error::Runtime(
                        "another caller is handling this restart".into(),
                    ));
                }
                match manager
                    .take()
                    .ok_or_else(|| invalid("service manager already invoked"))?
                    .await
                {
                    Manager::Succeeded => {}
                    Manager::Failed(reason) => {
                        if let Err(error) = report_failure(broker, id).await {
                            return Err(Error::Runtime(format!(
                                "{reason}; agent recovery is unconfirmed: {}",
                                error.public().kind
                            )));
                        }
                        manager_error = Some(reason);
                    }
                    Manager::Unknown => return Err(Error::Runtime(
                        "service manager outcome is unknown; inspect this restart before recovery"
                            .into(),
                    )),
                }
                reconnect_until = Some(tokio::time::Instant::now() + Duration::from_secs(60));
            }
            Some("requesting")
                if reconnect_until
                    .is_some_and(|deadline| tokio::time::Instant::now() < deadline) => {}
            Some("requesting") => return Err(Error::Runtime(
                "service manager outcome is not yet known; inspect this restart before recovery"
                    .into(),
            )),
            Some("complete") => {
                let last_output = output(
                    &json!({"restart_id":id,"phase":"complete","service_manager_failed":manager_error.is_some()}),
                );
                if output_error.is_none() {
                    output_error = last_output.err();
                }
                return match manager_error {
                    Some(reason) => Err(Error::Runtime(reason)),
                    None => output_error.map_or(Ok(0), Err),
                };
            }
            _ => return Err(invalid("invalid restart phase")),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::CliFuture;
    use serde_json::Value;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    };

    struct Broker {
        calls: Mutex<Vec<String>>,
        phases: Mutex<Vec<&'static str>>,
        claim: bool,
    }
    impl CliBroker for Broker {
        fn call<'a>(&'a self, method: &'a str, _: Value) -> CliFuture<'a> {
            self.calls.lock().unwrap().push(method.to_owned());
            let response = match method {
                "_service_restart_begin" => json!({"restart_id":7}),
                "_service_restart_status" => json!({"phase":self.phases.lock().unwrap().remove(0)}),
                "_service_restart_claim" => json!({"claimed":self.claim}),
                "_service_restart_failed" => json!({"restart_id":7}),
                _ => panic!("unexpected broker method"),
            };
            Box::pin(async move { Ok(response) })
        }
    }

    /// A manager failure triggers normal recovery and reaches only the caller as an error.
    #[tokio::test]
    async fn failed_manager_recovers_then_reports_the_error() {
        let broker = Broker {
            calls: Mutex::new(Vec::new()),
            phases: Mutex::new(vec!["ready", "complete"]),
            claim: true,
        };
        let output: CliOutput = Arc::new(|_| Ok(()));
        let error = drive(&broker, &output, Some("fixture".into()), None, async {
            Manager::Failed("manager refused".into())
        })
        .await
        .unwrap_err();
        assert!(error.to_string().contains("manager refused"));
        assert_eq!(
            *broker.calls.lock().unwrap(),
            [
                "_service_restart_begin",
                "_service_restart_status",
                "_service_restart_claim",
                "_service_restart_failed",
                "_service_restart_status"
            ]
        );
    }

    /// The initiating caller retains the manager error if recovery cannot be reached.
    #[tokio::test]
    async fn failed_manager_reports_unconfirmed_recovery() {
        struct Unavailable;
        impl CliBroker for Unavailable {
            fn call<'a>(&'a self, method: &'a str, _: Value) -> CliFuture<'a> {
                let result = match method {
                    "_service_restart_begin" => Ok(json!({"restart_id":7})),
                    "_service_restart_status" => Ok(json!({"phase":"ready"})),
                    "_service_restart_claim" => Ok(json!({"claimed":true})),
                    "_service_restart_failed" => Err(invalid("fixture recovery refusal")),
                    _ => panic!("unexpected broker method"),
                };
                Box::pin(async move { result })
            }
        }
        let output: CliOutput = Arc::new(|_| Ok(()));
        let error = drive(&Unavailable, &output, Some("fixture".into()), None, async {
            Manager::Failed("manager refused".into())
        })
        .await
        .unwrap_err();
        assert!(error.to_string().contains("manager refused"));
        assert!(error.to_string().contains("recovery is unconfirmed"));
    }

    /// Transient disconnects retry only status and the idempotent failure report.
    #[tokio::test]
    async fn brief_broker_disconnect_does_not_repeat_the_manager() {
        struct Brief {
            status: Mutex<u8>,
            recovery: Mutex<u8>,
        }
        impl CliBroker for Brief {
            fn call<'a>(&'a self, method: &'a str, _: Value) -> CliFuture<'a> {
                let result = match method {
                    "_service_restart_begin" => Ok(json!({"restart_id":7})),
                    "_service_restart_claim" => Ok(json!({"claimed":true})),
                    "_service_restart_failed" => {
                        let mut n = self.recovery.lock().unwrap();
                        *n += 1;
                        if *n == 1 {
                            Err(Error::BrokerUnavailable)
                        } else {
                            Ok(json!({"restart_id":7}))
                        }
                    }
                    "_service_restart_status" => {
                        let mut n = self.status.lock().unwrap();
                        *n += 1;
                        match *n {
                            1 => Ok(json!({"phase":"ready"})),
                            2 => Err(Error::Runtime(
                                "broker disconnected before its response".into(),
                            )),
                            _ => Ok(json!({"phase":"complete"})),
                        }
                    }
                    _ => panic!("unexpected broker method"),
                };
                Box::pin(async move { result })
            }
        }
        let broker = Brief {
            status: Mutex::new(0),
            recovery: Mutex::new(0),
        };
        let output: CliOutput = Arc::new(|_| Ok(()));
        let invocations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = invocations.clone();
        let error = drive(&broker, &output, Some("fixture".into()), None, async move {
            count.fetch_add(1, Ordering::SeqCst);
            Manager::Failed("manager refused".into())
        })
        .await
        .unwrap_err();
        assert!(error.to_string().contains("manager refused"));
        assert_eq!(invocations.load(Ordering::SeqCst), 1);
        assert_eq!(*broker.recovery.lock().unwrap(), 2);
    }

    /// A second caller cannot invoke the manager after another caller claims it.
    #[tokio::test]
    async fn failed_claim_never_invokes_the_manager() {
        let broker = Broker {
            calls: Mutex::new(Vec::new()),
            phases: Mutex::new(vec!["ready"]),
            claim: false,
        };
        let output: CliOutput = Arc::new(|_| Ok(()));
        let invoked = Arc::new(AtomicBool::new(false));
        let flag = invoked.clone();
        assert!(
            drive(&broker, &output, Some("fixture".into()), None, async move {
                flag.store(true, Ordering::SeqCst);
                Manager::Succeeded
            })
            .await
            .is_err()
        );
        assert!(!invoked.load(Ordering::SeqCst));
    }

    /// An acknowledged manager request may briefly leave the old broker up.
    #[tokio::test]
    async fn successful_manager_waits_for_the_new_broker() {
        let broker = Broker {
            calls: Mutex::new(Vec::new()),
            phases: Mutex::new(vec!["ready", "requesting", "complete"]),
            claim: true,
        };
        let output: CliOutput = Arc::new(|_| Ok(()));
        assert_eq!(
            drive(&broker, &output, Some("fixture".into()), None, async {
                Manager::Succeeded
            })
            .await
            .unwrap(),
            0
        );
        assert!(
            !broker
                .calls
                .lock()
                .unwrap()
                .iter()
                .any(|name| name == "_service_restart_failed")
        );
    }

    /// An unknown manager outcome must not start recovery on an assumed failure.
    #[tokio::test]
    async fn unknown_manager_outcome_is_not_replayed() {
        let broker = Broker {
            calls: Mutex::new(Vec::new()),
            phases: Mutex::new(vec!["ready"]),
            claim: true,
        };
        let output: CliOutput = Arc::new(|_| Ok(()));
        assert!(
            drive(&broker, &output, Some("fixture".into()), None, async {
                Manager::Unknown
            })
            .await
            .is_err()
        );
        assert!(
            !broker
                .calls
                .lock()
                .unwrap()
                .iter()
                .any(|name| name == "_service_restart_failed")
        );
    }

    /// A closed output stream must not leave already captured work paused.
    #[tokio::test]
    async fn closed_output_does_not_abandon_a_restart() {
        let broker = Broker {
            calls: Mutex::new(Vec::new()),
            phases: Mutex::new(vec!["ready", "complete"]),
            claim: true,
        };
        let output: CliOutput = Arc::new(|_| Err(invalid("output closed")));
        let invoked = Arc::new(AtomicBool::new(false));
        let flag = invoked.clone();
        assert!(
            drive(&broker, &output, Some("fixture".into()), None, async move {
                flag.store(true, Ordering::SeqCst);
                Manager::Succeeded
            })
            .await
            .is_err()
        );
        assert!(invoked.load(Ordering::SeqCst));
        assert_eq!(
            broker.calls.lock().unwrap().last().unwrap(),
            "_service_restart_status"
        );
    }

    /// Exercise the operator driver against a disposable broker with a fake manager.
    #[tokio::test]
    async fn failed_manager_runs_recovery_through_the_real_broker() {
        let temp = tempfile::tempdir().unwrap();
        crate::cli::init(temp.path()).unwrap();
        let home = temp.path().to_path_buf();
        let path = home.join("api.sock");
        let server = tokio::spawn({
            let home = home.clone();
            let path = path.clone();
            async move { crate::transport::socket::serve_at(&home, &path).await }
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while tokio::net::UnixStream::connect(&path).await.is_err() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        let broker = crate::cli::SocketBroker { home };
        let records = Arc::new(Mutex::new(Vec::new()));
        let capture = records.clone();
        let output: CliOutput = Arc::new(move |value| {
            capture.lock().unwrap().push(value.clone());
            Ok(())
        });
        let result = tokio::time::timeout(
            Duration::from_secs(12),
            drive(
                &broker,
                &output,
                Some("manager-fixture".into()),
                None,
                async { Manager::Failed("manager refused".into()) },
            ),
        )
        .await
        .unwrap();
        assert!(result.unwrap_err().to_string().contains("manager refused"));
        assert_eq!(
            records.lock().unwrap().last().unwrap()["service_manager_failed"],
            true
        );
        server.abort();
        let _ = server.await;
    }
}
