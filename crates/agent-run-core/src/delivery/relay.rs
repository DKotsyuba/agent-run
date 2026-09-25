//! v1-v4 Desktop relay interoperability, bounded to ten seconds.
use super::{Evidence, Notice};
use crate::{error::invalid, Result};
use serde_json::{json, Value};
use std::{
    os::unix::fs::{FileTypeExt, MetadataExt},
    path::Path,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::UnixStream,
};
const LIMIT: usize = 8192;
async fn write<W: AsyncWrite + Unpin>(stream: &mut W, value: &Value) -> Result<()> {
    let data = serde_json::to_vec(value)?;
    if data.is_empty() || data.len() > LIMIT {
        return Err(invalid("relay frame bound"));
    }
    stream.write_all(&(data.len() as u32).to_le_bytes()).await?;
    stream.write_all(&data).await?;
    stream.flush().await?;
    Ok(())
}
async fn read<R: AsyncRead + Unpin>(stream: &mut R) -> Result<Value> {
    let len = stream.read_u32_le().await? as usize;
    if len == 0 || len > LIMIT {
        return Err(invalid("relay frame bound"));
    }
    let mut data = vec![0; len];
    stream.read_exact(&mut data).await?;
    let v: Value = serde_json::from_slice(&data)?;
    if !v.is_object() {
        return Err(invalid("relay expected object"));
    }
    Ok(v)
}

/// Sends `notice` for the target Desktop session `thread` to the first eligible relay endpoint.
///
/// Candidates under `home` are restricted to same-user Unix sockets, ordered by
/// protocol preference and name, and attempted until the shared ten-second
/// deadline expires. Accepted and rejected replies are classified directly;
/// malformed or interrupted exchanges are ambiguous because delivery may have
/// reached the relay. No reachable endpoint returns `relay_unavailable`.
pub async fn send(home: &Path, thread: &str, notice: &Notice) -> Evidence {
    let mut paths: Vec<_> = std::fs::read_dir(home)
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            let name = p.file_name().unwrap_or_default().to_string_lossy();
            name.starts_with("ar-cdx-") && name.ends_with(".sock")
        })
        .collect();
    paths.sort_by_key(|p| {
        let n = p.file_name().unwrap_or_default().to_string_lossy();
        (
            !n.starts_with("ar-cdx-v4-"),
            !n.starts_with("ar-cdx-v3-"),
            !n.starts_with("ar-cdx-v2-"),
            n.to_string(),
        )
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut rejected = false;
    for path in paths {
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        let Ok(meta) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        // SAFETY: geteuid returns the current user's identity.
        if !meta.file_type().is_socket() || meta.uid() != unsafe { libc::geteuid() } {
            continue;
        }
        let name = path.file_name().unwrap_or_default().to_string_lossy();
        let version = if name.starts_with("ar-cdx-v4-") {
            4
        } else if name.starts_with("ar-cdx-v3-") {
            3
        } else if name.starts_with("ar-cdx-v2-") {
            2
        } else {
            1
        };
        let exact = notice.run_id.as_ref().unwrap_or(&notice.agent_id);
        // Old hosts have no separate run field: keep their ID pinned to the
        // completed execution instead of silently reporting a moving root alias.
        let agent_id = if version >= 4 {
            &notice.agent_id
        } else {
            exact
        };
        let mut request = json!({"version":version,"op":"completion","thread_id":thread,"notification_id":notice.notification_id,"agent_id":agent_id,"status":notice.status});
        if version >= 4 {
            request["run_id"] = json!(exact);
        }
        if version >= 2 {
            request["runtime"] = json!(notice.runtime);
            request["model"] = json!(notice.model);
            request["effort"] = json!(notice.effort);
        }
        if version >= 3 {
            request["failure_kind"] = json!(notice.failure_kind);
        }
        let mut stream = match tokio::time::timeout_at(deadline, UnixStream::connect(&path)).await {
            Ok(Ok(s)) => s,
            _ => continue,
        };
        let reply = tokio::time::timeout_at(deadline, async {
            write(&mut stream, &request).await?;
            read(&mut stream).await
        })
        .await;
        match reply {
            Ok(Ok(value)) if value == json!({"outcome":"accepted"}) => {
                return Evidence::new("relay_accepted", true, false)
            }
            Ok(Ok(value)) if value == json!({"outcome":"rejected"}) => {
                rejected = true;
                continue;
            }
            _ => return Evidence::new("relay_ambiguous", false, true),
        }
    }
    Evidence::new(
        if rejected {
            "relay_rejected"
        } else {
            "relay_unavailable"
        },
        false,
        false,
    )
}
#[cfg(test)]
mod tests {
    use super::*;

    /// New relays receive both ids; old relays still receive the completed run.
    #[tokio::test]
    async fn relay_keeps_run_identity_across_wire_versions() {
        for version in [4, 3, 2, 1] {
            let home = tempfile::tempdir().unwrap();
            let name = if version == 1 {
                "ar-cdx-legacy.sock".into()
            } else {
                format!("ar-cdx-v{version}-fixture.sock")
            };
            let listener = tokio::net::UnixListener::bind(home.path().join(name)).unwrap();
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = read(&mut stream).await.unwrap();
                write(&mut stream, &json!({"outcome":"accepted"}))
                    .await
                    .unwrap();
                request
            });
            let mut notice = Notice::legacy(
                "ntf_stable",
                "ag-20260925-000000-0000000001".parse().unwrap(),
                crate::domain::Status::Succeeded,
            );
            let run: crate::domain::AgentId = "ag-20260925-000000-0000000002".parse().unwrap();
            notice.run_id = Some(run.clone());
            assert_eq!(
                send(home.path(), "thread", &notice).await.classifier,
                "relay_accepted"
            );
            let request = tokio::time::timeout(Duration::from_secs(2), server)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(request["version"], version);
            if version == 4 {
                assert_eq!(request["agent_id"], notice.agent_id.as_str());
                assert_eq!(request["run_id"], run.as_str());
            } else {
                assert_eq!(request["agent_id"], run.as_str());
                assert!(request.get("run_id").is_none());
            }
        }
    }

    /// Mirrors `tests/test_codex_desktop_relay.py::RelayClientTests::test_frame_bound_is_enforced`.
    #[tokio::test]
    async fn relay_frame_bound_is_enforced_before_writing() {
        let mut sink = tokio::io::sink();
        let result = write(&mut sink, &json!({"text":"x".repeat(8192)})).await;
        assert!(result.is_err());
    }
}
