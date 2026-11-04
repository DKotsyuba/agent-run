//! Shared broker and unprivileged account-owned launch workers.
//!
//! The host selects accounts and controls socket access using its native
//! service and filesystem permissions. Workers launch only the fixed Agent
//! Run supervisor; the broker retains durable ownership and capability gates.

mod broker;
mod client;
mod config;
mod server;
mod wire;

pub use broker::BrokerLauncher;
pub use client::{Client, ClientError, PendingLaunch};
pub use config::Config;
pub use server::Worker;
pub use wire::Backend;

/// Serves a fixed account-owned launcher until shutdown, then waits for its
/// already-started supervisors. Socket access is governed by the host.
pub async fn serve_worker(home: &std::path::Path, socket: &std::path::Path) -> crate::Result<()> {
    use std::sync::Arc;
    use tokio::task::JoinSet;

    let (listener, _guard) = crate::transport::socket::bind_shared(socket).await?;
    let worker = Arc::new(Worker::new(std::env::current_exe()?, home.to_path_buf())?);
    let maintenance = std::env::var_os("AGENT_RUN_SYSTEM_OPERATOR_SOCKET").map(|socket| {
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(account_retention(
            home.to_path_buf(),
            socket.into(),
            stopped,
        ));
        (stop, task)
    });
    let mut tasks = JoinSet::new();
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            _ = term.recv() => break,
            accepted = listener.accept(), if tasks.len() < 64 => {
                let (stream, _) = accepted?;
                let stream = stream.into_std()?;
                stream.set_nonblocking(false)?;
                let worker = worker.clone();
                tasks.spawn_blocking(move || worker.serve_one(stream));
            }
            Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                if let Ok(Err(error)) = result {
                    eprintln!("launch worker request failed: {error}");
                }
            }
        }
    }
    drop(listener);
    while let Some(result) = tasks.join_next().await {
        if let Ok(Err(error)) = result {
            eprintln!("launch worker request failed: {error}");
        }
    }
    if let Some((stop, maintenance)) = maintenance {
        let _ = stop.send(());
        let _ = maintenance.await;
    }
    Ok(())
}

/// Periodically reclaims this account's private, unreferenced runtime homes.
async fn account_retention(
    home: std::path::PathBuf,
    socket: std::path::PathBuf,
    mut stop: tokio::sync::oneshot::Receiver<()>,
) {
    use std::time::Duration;
    let mut delay = Duration::from_secs(5);
    loop {
        tokio::select! {
            _ = &mut stop => break,
            _ = tokio::time::sleep(delay) => {}
        }
        delay = match account_retention_pass(home.clone(), socket.clone()).await {
            Ok(0) => Duration::from_secs(3600),
            Ok(_) => Duration::from_secs(1),
            Err(error) => {
                eprintln!(
                    "account runtime retention deferred: {}",
                    error.public().kind
                );
                Duration::from_secs(60)
            }
        };
    }
}

/// Runs a bounded pass outside the async executor using a validated broker
/// configuration snapshot and the worker account's own filesystem access.
async fn account_retention_pass(
    home: std::path::PathBuf,
    socket: std::path::PathBuf,
) -> crate::Result<usize> {
    use std::os::unix::fs::MetadataExt;
    let broker_uid = std::fs::metadata(&home)?.uid();
    let client =
        crate::transport::socket::BrokerClient::with_expected_server_uid(socket, broker_uid);
    let response = client
        .call_with_timeout("_runtime_retention", Some(serde_json::json!({})), 5.0)
        .await?;
    let roots: Vec<std::path::PathBuf> = serde_json::from_value(
        response
            .get("roots")
            .cloned()
            .ok_or_else(|| crate::error::invalid("runtime roots are unavailable"))?,
    )?;
    let protected: Vec<std::path::PathBuf> = serde_json::from_value(
        response
            .get("protected_paths")
            .cloned()
            .ok_or_else(|| crate::error::invalid("runtime protection is unavailable"))?,
    )?;
    tokio::task::spawn_blocking(move || {
        let store = crate::state::Store::open(&home)?;
        // SAFETY: geteuid only reads the effective process identity.
        let uid = unsafe { libc::geteuid() };
        let mut removed = 0;
        for root in roots {
            let Ok(meta) = std::fs::symlink_metadata(&root) else {
                continue;
            };
            if !meta.is_dir() || meta.uid() != uid {
                continue;
            }
            removed += agent_run_core::housekeeping::sweep_account_runtime(
                &root,
                crate::domain::now(),
                &store,
                &protected,
            )?;
        }
        Ok(removed)
    })
    .await
    .map_err(|_| crate::Error::Runtime("account runtime retention stopped".into()))?
}
