//! Bounded account-worker protocol; capability bytes are never logged.

use agent_run_domain::domain::AgentId;
use agent_run_platform::process::Identity;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    io::{self, Read, Write},
    os::{fd::AsRawFd, unix::net::UnixStream},
    time::{Duration, Instant},
};

const MAX_FRAME: usize = 4096;
const FRAME_READ_TIMEOUT: Duration = Duration::from_secs(30);
const FRAME_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// One execution claim for a worker already running as its account.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Start {
    pub execution: AgentId,
    pub capability_sha256: String,
}

/// The broker permits release only after its startup-owner write is durable.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Release {
    pub capability: String,
}

/// Replies distinguish refusal before a claim from an uncertain claimed launch.
#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Reply {
    Spawned {
        identity: Identity,
        backend: Backend,
    },
    Ready,
    Refused {
        reason: String,
    },
    Unknown {
        reason: String,
    },
}

/// Spawn evidence sent alongside the exact kernel process identity.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Backend {
    PosixSpawnSetsid,
    ForkSetsid,
}

impl From<agent_run_platform::launch::SpawnBackend> for Backend {
    /// Preserve the platform launch backend in durable startup evidence.
    fn from(value: agent_run_platform::launch::SpawnBackend) -> Self {
        match value {
            agent_run_platform::launch::SpawnBackend::PosixSpawnSetsid => Self::PosixSpawnSetsid,
            agent_run_platform::launch::SpawnBackend::ForkSetsid => Self::ForkSetsid,
        }
    }
}

/// Write one length-prefixed JSON frame, rejecting oversized values before writing.
pub(super) fn write<T: Serialize>(stream: &mut UnixStream, message: &T) -> io::Result<()> {
    write_until(stream, message, Instant::now() + FRAME_WRITE_TIMEOUT)
}

/// Write the entire frame before one absolute deadline, including partial writes.
pub(super) fn write_until<T: Serialize>(
    stream: &mut UnixStream,
    message: &T,
    deadline: Instant,
) -> io::Result<()> {
    let bytes = serde_json::to_vec(message).map_err(io::Error::other)?;
    if bytes.is_empty() || bytes.len() > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid coordinator frame size",
        ));
    }
    let size = u32::try_from(bytes.len()).map_err(io::Error::other)?;
    write_bytes(stream, &size.to_be_bytes(), deadline)?;
    write_bytes(stream, &bytes, deadline)
}

/// Read exactly one bounded JSON frame; malformed input is an error.
pub(super) fn read<T: DeserializeOwned>(stream: &mut UnixStream) -> io::Result<T> {
    read_until(stream, Instant::now() + FRAME_READ_TIMEOUT)
}

/// Read one frame before one absolute deadline, including partial reads.
pub(super) fn read_until<T: DeserializeOwned>(
    stream: &mut UnixStream,
    deadline: Instant,
) -> io::Result<T> {
    let mut length = [0_u8; 4];
    read_bytes(stream, &mut length, deadline)?;
    let length = u32::from_be_bytes(length) as usize;
    if length == 0 || length > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid coordinator frame size",
        ));
    }
    let mut bytes = vec![0_u8; length];
    read_bytes(stream, &mut bytes, deadline)?;
    serde_json::from_slice(&bytes)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

/// Read each partial chunk against the same deadline, including on nonblocking streams.
fn read_bytes(stream: &mut UnixStream, mut bytes: &mut [u8], deadline: Instant) -> io::Result<()> {
    while !bytes.is_empty() {
        wait_ready(stream, libc::POLLIN, deadline)?;
        stream.set_read_timeout(Some(remaining(deadline)?))?;
        match stream.read(bytes) {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
            Ok(read) => bytes = &mut bytes[read..],
            Err(error) if retryable(&error) => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

/// Write each partial chunk against the same deadline, including on nonblocking streams.
fn write_bytes(stream: &mut UnixStream, mut bytes: &[u8], deadline: Instant) -> io::Result<()> {
    while !bytes.is_empty() {
        wait_ready(stream, libc::POLLOUT, deadline)?;
        stream.set_write_timeout(Some(remaining(deadline)?))?;
        match stream.write(bytes) {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero)),
            Ok(written) => bytes = &bytes[written..],
            Err(error) if retryable(&error) => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

/// Wait for readable/writable socket state without renewing the deadline.
fn wait_ready(stream: &UnixStream, events: libc::c_short, deadline: Instant) -> io::Result<()> {
    loop {
        let wait = remaining(deadline)?;
        let millis = wait.as_millis().saturating_add(1).min(i32::MAX as u128) as i32;
        let mut fd = libc::pollfd {
            fd: stream.as_raw_fd(),
            events,
            revents: 0,
        };
        // SAFETY: fd is a live socket, and poll receives one initialized writable pollfd.
        let result = unsafe { libc::poll(&mut fd, 1, millis) };
        if result > 0 {
            remaining(deadline)?;
            return Ok(());
        }
        if result < 0 {
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }
}

/// Remaining time before the original frame deadline, never zero.
fn remaining(deadline: Instant) -> io::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|value| !value.is_zero())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "coordinator frame deadline expired",
            )
        })
}

/// Retry transient I/O only after checking the original deadline again.
fn retryable(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
    )
}

/// Validate the stable encoding used for digests and fresh launch capabilities.
pub(super) fn lowercase_hex64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::mpsc, thread};

    /// A peer sending one byte repeatedly cannot renew the whole-frame deadline.
    #[test]
    fn slow_bytes_do_not_extend_the_read_deadline() {
        let (mut reader, mut writer) = UnixStream::pair().unwrap();
        let (sent, received) = mpsc::channel();
        let (go, resume) = mpsc::channel();
        let worker = thread::spawn(move || {
            writer.write_all(&4096_u32.to_be_bytes()).unwrap();
            writer.write_all(b"{").unwrap();
            sent.send(()).unwrap();
            resume.recv().unwrap();
            for _ in 0..40 {
                thread::sleep(Duration::from_millis(25));
                if writer.write_all(b" ").is_err() {
                    break;
                }
            }
        });
        received.recv_timeout(Duration::from_secs(2)).unwrap();
        go.send(()).unwrap();
        let result = read_until::<serde_json::Value>(
            &mut reader,
            Instant::now() + Duration::from_millis(160),
        );
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
        drop(reader);
        worker.join().unwrap();
    }

    /// A peer which never drains its socket cannot block a frame write forever.
    #[test]
    fn blocked_writer_obeys_the_frame_deadline() {
        let (mut writer, _reader) = UnixStream::pair().unwrap();
        writer.set_nonblocking(true).unwrap();
        let bytes = [0_u8; 4096];
        loop {
            match writer.write(&bytes) {
                Ok(0) => panic!("socket closed while filling send buffer"),
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => panic!("cannot fill send buffer: {error}"),
            }
        }
        let error = write_until(
            &mut writer,
            &Reply::Ready,
            Instant::now() + Duration::from_millis(160),
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    }
}
