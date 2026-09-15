//! v1/v2/v3 interoperability with existing Desktop relays, bounded to ten seconds.
use super::{Evidence, Notice};
use crate::{domain::Status, error::invalid, fs, Result};
use serde_json::{json, Value};
use std::{
    os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    process::Command,
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
            !n.starts_with("ar-cdx-v3-"),
            !n.starts_with("ar-cdx-v2-"),
            n.to_string(),
        )
    });
    paths.truncate(16);
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
        let version = if name.starts_with("ar-cdx-v3-") {
            3
        } else if name.starts_with("ar-cdx-v2-") {
            2
        } else {
            1
        };
        let mut request = json!({"version":version,"op":"completion","thread_id":thread,"notification_id":notice.notification_id,"agent_id":notice.agent_id,"status":notice.status});
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
fn request(value: Value) -> Result<(String, Notice)> {
    let obj = value
        .as_object()
        .ok_or_else(|| invalid("invalid relay request"))?;
    let version = obj
        .get("version")
        .and_then(Value::as_u64)
        .ok_or_else(|| invalid("invalid relay version"))?;
    let allowed = match version {
        1 => vec![
            "version",
            "op",
            "thread_id",
            "notification_id",
            "agent_id",
            "status",
        ],
        2 => vec![
            "version",
            "op",
            "thread_id",
            "notification_id",
            "agent_id",
            "status",
            "runtime",
            "model",
            "effort",
        ],
        3 => vec![
            "version",
            "op",
            "thread_id",
            "notification_id",
            "agent_id",
            "status",
            "runtime",
            "model",
            "effort",
            "failure_kind",
        ],
        _ => return Err(invalid("unsupported relay version")),
    };
    if obj.len() != allowed.len()
        || obj.keys().any(|k| !allowed.contains(&k.as_str()))
        || obj.get("op").and_then(Value::as_str) != Some("completion")
    {
        return Err(invalid("invalid relay request fields"));
    }
    let string = |key: &str| {
        obj.get(key)
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("invalid relay identifier"))
    };
    let thread = string("thread_id")?.to_owned();
    crate::domain::external_id("thread_id", &thread)?;
    let opt = |key: &str| -> Result<Option<String>> {
        match obj.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(s)) => Ok(Some(s.clone())),
            _ => Err(invalid("invalid relay metadata")),
        }
    };
    let notice = Notice {
        notification_id: string("notification_id")?.into(),
        agent_id: string("agent_id")?.parse()?,
        status: string("status")?.parse::<Status>()?,
        runtime: opt("runtime")?,
        model: opt("model")?,
        effort: opt("effort")?,
        failure_kind: opt("failure_kind")?,
    };
    notice.validate()?;
    Ok((thread, notice))
}
pub struct Host {
    path: PathBuf,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Host {
    fn drop(&mut self) {
        self.task.abort();
        let _ = std::fs::remove_file(&self.path);
    }
}
/// The SDK and relay listener are Rust. The optional 80-line Node transport uses
/// the Desktop-supplied signed executable solely to cross its OS trust boundary.
pub fn host(home: &Path) -> Result<Option<Host>> {
    let (Some(pipe), Some(node)) = (
        std::env::var_os("CODEX_APP_TOOLS_PIPE_PATH"),
        std::env::var_os("CODEX_MCP_NODE_PATH"),
    ) else {
        return Ok(None);
    };
    let pipe = PathBuf::from(pipe);
    let node = PathBuf::from(node);
    if !pipe.is_absolute() || !node.is_absolute() {
        return Err(invalid("Desktop capability paths must be absolute"));
    }
    let path = home.join(format!(
        "ar-cdx-v3-rust-{}-{}.sock",
        std::process::id(),
        &uuid::Uuid::new_v4().simple().to_string()[..6]
    ));
    let listener = UnixListener::bind(&path)?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    let task = tokio::spawn(async move {
        let gate = std::sync::Arc::new(tokio::sync::Semaphore::new(8));
        while let Ok((mut stream, _)) = listener.accept().await {
            // SAFETY: geteuid is a read-only query.
            if stream
                .peer_cred()
                .map(|p| p.uid() != unsafe { libc::geteuid() })
                .unwrap_or(true)
            {
                continue;
            }
            let Ok(permit) = gate.clone().try_acquire_owned() else {
                continue;
            };
            let node = node.clone();
            let pipe = pipe.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let action = async {
                    let (thread, notice) = request(read(&mut stream).await?)?;
                    let outcome = host_send(&node, &pipe, &thread, &notice)
                        .await
                        .unwrap_or_else(|_| "ambiguous".into());
                    write(&mut stream, &json!({"outcome":outcome})).await
                };
                let _ = tokio::time::timeout(Duration::from_secs(9), action).await;
            });
        }
    });
    Ok(Some(Host { path, task }))
}
async fn host_send(node: &Path, pipe: &Path, thread: &str, notice: &Notice) -> Result<String> {
    let mut command = Command::new(node);
    command
        .args(["-e", include_str!("../../resources/desktop-transport.cjs")])
        .env_clear()
        .env("CODEX_APP_TOOLS_PIPE_PATH", pipe)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let mut child = command.spawn()?;
    let mut input = child
        .stdin
        .take()
        .ok_or_else(|| invalid("Node transport stdin missing"))?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    tokio::time::timeout_at(deadline,write(&mut input,&json!({"threadId":thread,"notificationId":notice.notification_id,"prompt":notice.render()?}))).await.map_err(|_|invalid("Desktop host write deadline"))??;
    drop(input);
    let mut output = child
        .stdout
        .take()
        .ok_or_else(|| invalid("Node transport stdout missing"))?;
    let value = tokio::time::timeout_at(deadline, read(&mut output))
        .await
        .map_err(|_| invalid("Desktop host deadline"))??;
    let _ = tokio::time::timeout_at(deadline, child.wait())
        .await
        .map_err(|_| invalid("Desktop host exit deadline"))??;
    value
        .get("outcome")
        .and_then(Value::as_str)
        .filter(|s| ["accepted", "rejected", "ambiguous"].contains(s))
        .map(str::to_owned)
        .ok_or_else(|| invalid("malformed Desktop outcome"))
}
