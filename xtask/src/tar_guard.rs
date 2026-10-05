//! Bounded pre-extraction tar inspection; no archive member is executed.
use std::{
    collections::BTreeSet,
    fs::File,
    io::{BufReader, Read},
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

/// Maximum ordinary members before an archive is refused.
pub const MAX_ENTRIES: usize = 10_000;
/// Maximum logical unpacked payload, including sparse-file declarations.
pub const MAX_UNPACKED: u64 = 2 * 1024 * 1024 * 1024;

/// Owns a native gzip decoder; early rejection and panic kill/reap this exact child.
struct Decoder(Child);
impl Drop for Decoder {
    /// Terminates only this owned decoder, never a process group.
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Inspects a regular tar or gzip tar before extraction. Native payloads accept
/// executable bits only on shipped binaries/collectors; source archives allow
/// ordinary executable files. Refuses links, specials, unsafe paths/modes,
/// duplicate normalized entries, malformed headers, extensions changing
/// extraction semantics, excess count/bytes and missing end markers.
pub fn inspect(path: &Path, native: bool) -> Result<(), String> {
    inspect_until(path, native, Instant::now() + Duration::from_secs(120))
}

/// Shares an enclosing monotonic deadline across native decoder and plain-file
/// reads; cancellation or expiry rejects before extraction and reaps the decoder.
pub(crate) fn inspect_until(path: &Path, native: bool, deadline: Instant) -> Result<(), String> {
    let metadata = std::fs::symlink_metadata(path).map_err(|_| "archive unavailable")?;
    if !metadata.is_file() || metadata.len() > 512 * 1024 * 1024 {
        return Err("archive type/size refused".into());
    }
    let mut magic = [0; 2];
    File::open(path)
        .map_err(|_| "archive unavailable")?
        .read_exact(&mut magic)
        .map_err(|_| "archive truncated")?;
    if magic == [0x1f, 0x8b] {
        inspect_decoder(
            Command::new("/usr/bin/gzip").arg("-dc").arg(path),
            native,
            deadline.saturating_duration_since(Instant::now()),
        )
    } else {
        inspect_stream(
            BufReader::new(TimedReader {
                pipe: File::open(path).map_err(|_| "archive unavailable")?,
                deadline,
                cancel: None,
            }),
            native,
        )
    }
}

/// Pipe reader with one operation-wide monotonic deadline, including stalls.
/// poll observes only this owned descriptor and never signals a process.
pub(crate) struct TimedReader<R> {
    /// Owned decoder stdout; closing it on rejection cannot affect other pipes.
    pub(crate) pipe: R,
    /// Deadline established before decoder spawn.
    pub(crate) deadline: Instant,
    /// Optional owned-drain cancellation; checked at least every 50ms.
    pub(crate) cancel: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
}
impl<R: Read + std::os::fd::AsRawFd> Read for TimedReader<R> {
    /// Waits only until the operation deadline, then performs one ready pipe read.
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        loop {
            let remaining = self.deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero()
                || crate::delivery::CANCELLED.load(std::sync::atomic::Ordering::Relaxed)
                || self
                    .cancel
                    .as_ref()
                    .is_some_and(|c| c.load(std::sync::atomic::Ordering::Acquire))
            {
                return Err(std::io::Error::from(std::io::ErrorKind::TimedOut));
            }
            let mut fd = libc::pollfd {
                fd: self.pipe.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            // SAFETY: fd names this owned live pipe; poll receives one valid record.
            let ready =
                unsafe { libc::poll(&mut fd, 1, remaining.as_millis().clamp(1, 50) as i32) };
            if ready > 0 {
                return self.pipe.read(bytes);
            }
            if ready == 0 {
                continue;
            }
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }
}

/// Owns decoder/parser/wait under one deadline; stalled and oversized streams
/// unwind through the exact-child kill/reap guard before returning an error.
fn inspect_decoder(command: &mut Command, native: bool, timeout: Duration) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    let mut child = Decoder(
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|_| "gzip unavailable")?,
    );
    let pipe = child.0.stdout.take().ok_or("decoder output unavailable")?;
    inspect_stream(
        BufReader::new(TimedReader {
            pipe,
            deadline,
            cancel: None,
        }),
        native,
    )?;
    loop {
        if let Some(status) = child.0.try_wait().map_err(|_| "decoder wait failed")? {
            return if status.success() {
                Ok(())
            } else {
                Err("invalid gzip stream".into())
            };
        }
        if Instant::now() >= deadline {
            return Err("decoder deadline exceeded".into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Parses positive octal tar fields, rejecting base-256 and overflow.
fn octal(bytes: &[u8]) -> Result<u64, String> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| "invalid tar number")?
        .trim_matches(|c| c == '\0' || c == ' ');
    if text.is_empty() {
        return Ok(0);
    }
    if !text.bytes().all(|b| (b'0'..=b'7').contains(&b)) {
        return Err("invalid tar number".into());
    }
    u64::from_str_radix(text, 8).map_err(|_| "tar number overflow".into())
}

/// Decodes a bounded NUL-terminated header string without control characters.
fn text(bytes: &[u8]) -> Result<String, String> {
    let bytes = bytes.split(|b| *b == 0).next().ok_or("invalid tar text")?;
    let value = std::str::from_utf8(bytes).map_err(|_| "tar path is not UTF-8")?;
    if value.chars().any(char::is_control) {
        return Err("unsafe tar text".into());
    }
    Ok(value.into())
}

/// Normalizes only harmless ./ and trailing directory slash; traversal/absolute
/// names and repeated normalized aliases are rejected before filesystem writes.
fn name(header: &[u8; 512]) -> Result<String, String> {
    let mut path = text(&header[..100])?;
    let prefix = text(&header[345..500])?;
    if !prefix.is_empty() {
        path = format!("{prefix}/{path}");
    }
    if path.starts_with('/') || path.split('/').any(|s| s == "..") {
        return Err("unsafe archive member path".into());
    }
    while path.starts_with("./") {
        path = path[2..].into();
    }
    let path = path.trim_end_matches('/');
    if path == "." || path.is_empty() {
        return Ok(String::new());
    }
    if path.split('/').any(|part| part.is_empty() || part == ".") {
        return Err("ambiguous archive path".into());
    }
    Ok(path.into())
}

/// Reads complete headers/data with constant scratch memory and explicit limits.
fn inspect_stream(mut input: impl Read, native: bool) -> Result<(), String> {
    let mut seen = BTreeSet::new();
    let mut pending_path: Option<String> = None;
    let mut total = 0u64;
    let mut count = 0usize;
    let mut header = [0u8; 512];
    loop {
        input
            .read_exact(&mut header)
            .map_err(|_| "truncated tar header")?;
        if header.iter().all(|b| *b == 0) {
            input
                .read_exact(&mut header)
                .map_err(|_| "missing tar end marker")?;
            if header.iter().any(|b| *b != 0) {
                return Err("invalid tar end marker".into());
            }
            let mut remaining = 1024 * 1024usize;
            let mut trailing = [0; 8192];
            loop {
                let read = input.read(&mut trailing).map_err(|_| "tar read failed")?;
                if read > remaining {
                    return Err("tar padding limit exceeded".into());
                }
                remaining -= read;
                if read == 0 {
                    break;
                }
                if trailing[..read].iter().any(|b| *b != 0) {
                    return Err("concatenated/trailing archive data refused".into());
                }
            }
            return if count > 0 {
                Ok(())
            } else {
                Err("empty archive".into())
            };
        }
        count += 1;
        if count > MAX_ENTRIES {
            return Err("archive entry limit exceeded".into());
        }
        let checksum = octal(&header[148..156])?;
        let sum: u64 = header
            .iter()
            .enumerate()
            .map(|(i, b)| {
                if (148..156).contains(&i) {
                    32
                } else {
                    u64::from(*b)
                }
            })
            .sum();
        if checksum != sum {
            return Err("invalid tar header checksum".into());
        }
        if &header[257..263] != b"ustar\0" && &header[257..263] != b"ustar " {
            return Err("unsupported tar header format".into());
        }
        if &header[257..263] == b"ustar " && header[345..500].iter().any(|b| *b != 0) {
            return Err("unsafe GNU tar extension".into());
        }
        let size = octal(&header[124..136])?;
        let mode = octal(&header[100..108])?;
        let kind = header[156];
        let path = pending_path.take().unwrap_or(name(&header)?);
        if matches!(kind, b'g' | b'x') {
            // Accept inert comment/time and validated source-only path records;
            // link/size/mode overrides never reach extraction.
            if size == 0 || size > 4096 {
                return Err("archive metadata extension refused".into());
            }
            let mut bytes = vec![0; size as usize];
            input
                .read_exact(&mut bytes)
                .map_err(|_| "truncated tar metadata")?;
            let meta = std::str::from_utf8(&bytes).map_err(|_| "invalid tar metadata")?;
            if kind == b'x' && !native {
                for line in meta.lines() {
                    if let Some((_, record)) = line.split_once(' ')
                        && let Some(value) = record.strip_prefix("path=")
                    {
                        let mut synthetic = [0; 512];
                        if value.len() >= 100 {
                            if value.starts_with('/')
                                || value
                                    .split('/')
                                    .any(|p| p == ".." || p.is_empty() || p == ".")
                                || value.chars().any(char::is_control)
                            {
                                return Err("unsafe tar path metadata".into());
                            }
                            pending_path = Some(value.trim_end_matches('/').into());
                        } else {
                            synthetic[..value.len()].copy_from_slice(value.as_bytes());
                            pending_path = Some(name(&synthetic)?);
                        }
                    }
                }
            }
            if meta.lines().any(|line| {
                !line.split_once(' ').is_some_and(|(length, record)| {
                    length.parse::<usize>().ok() == Some(line.len() + 1)
                        && record.split_once('=').is_some_and(|(key, value)| {
                            (kind == b'g' && !native && key == "comment")
                                || (kind == b'x' && !native && key == "path")
                                || (matches!(key, "mtime" | "atime" | "ctime")
                                    && value
                                        .parse::<f64>()
                                        .is_ok_and(|n| n.is_finite() && n >= 0.0))
                        })
                })
            }) {
                return Err("unsafe tar metadata".into());
            }
            std::io::copy(
                &mut (&mut input).take((512 - size % 512) % 512),
                &mut std::io::sink(),
            )
            .map_err(|_| "tar padding failed")?;
            continue;
        }
        if !matches!(kind, 0 | b'0' | b'5') || header[157..257].iter().any(|b| *b != 0) {
            return Err("archive links and special files are forbidden".into());
        }
        if path.is_empty() && kind != b'5' {
            return Err("empty archive file name".into());
        }
        if !seen.insert(path.clone()) {
            return Err("duplicate archive entry".into());
        }
        if mode & !0o777 != 0 || (native && mode & 0o022 != 0) {
            return Err("unsafe archive mode".into());
        }
        if native
            && kind != b'5'
            && mode & 0o111 != 0
            && !matches!(
                path.as_str(),
                "bin/agent-run"
                    | "bin/agent-run-tui"
                    | "bin/agent-run-deploy"
                    | "collectors/codex.sh"
                    | "collectors/claude.sh"
                    | "collectors/glm.sh"
            )
        {
            return Err("unexpected executable archive member".into());
        }
        total = total.checked_add(size).ok_or("archive size overflow")?;
        if total > MAX_UNPACKED {
            return Err("archive unpacked byte limit exceeded".into());
        }
        if kind == b'5' && size != 0 {
            return Err("directory carries archive data".into());
        }
        let length = size.checked_add(511).ok_or("archive size overflow")? / 512 * 512;
        let copied = std::io::copy(&mut (&mut input).take(length), &mut std::io::sink())
            .map_err(|_| "tar data read failed")?;
        if copied != length {
            return Err("truncated archive member".into());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Creates one strict synthetic header; all mutation cases remain local bytes.
    fn header(name: &str, kind: u8, mode: u64, size: u64) -> [u8; 512] {
        let mut h = [0; 512];
        h[..name.len()].copy_from_slice(name.as_bytes());
        h[100..108].copy_from_slice(format!("{mode:07o}\0").as_bytes());
        h[124..136].copy_from_slice(format!("{size:011o}\0").as_bytes());
        h[156] = kind;
        h[257..263].copy_from_slice(b"ustar\0");
        h[148..156].fill(b' ');
        let sum: u64 = h.iter().map(|b| u64::from(*b)).sum();
        h[148..156].copy_from_slice(format!("{sum:06o}\0 ").as_bytes());
        h
    }
    /// Malicious metadata, aliases, modes, sizes and links are rejected pre-write.
    #[test]
    fn rejects_malicious_headers_and_duplicate_aliases() {
        for h in [
            header("/escape", b'0', 0o644, 0),
            header("../escape", b'0', 0o644, 0),
            header("bin/agent-run", b'0', 0o4755, 0),
            header("unknown", b'0', 0o755, 0),
            header("link", b'2', 0o777, 0),
            header("pax", b'x', 0o644, 0),
            header("huge", b'0', 0o644, MAX_UNPACKED + 1),
        ] {
            let mut bytes = h.to_vec();
            bytes.extend([0; 1024]);
            assert!(inspect_stream(bytes.as_slice(), true).is_err());
        }
        let mut bytes = header("a", b'0', 0o644, 0).to_vec();
        bytes.extend(header("./a", b'0', 0o644, 0));
        bytes.extend([0; 1024]);
        assert!(
            inspect_stream(bytes.as_slice(), true)
                .unwrap_err()
                .contains("duplicate")
        );
        let mut bytes = Vec::new();
        for n in 0..=MAX_ENTRIES {
            bytes.extend(header(&format!("a{n}"), b'0', 0o644, 0));
        }
        assert!(
            inspect_stream(bytes.as_slice(), true)
                .unwrap_err()
                .contains("entry limit")
        );
    }

    /// The real committed source format's inert metadata must remain readable.
    #[test]
    fn committed_source_archive_metadata_remains_readable() {
        let dir = tempfile::tempdir().unwrap();
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
        crate::archive::build(root, &dir.path().join("source.tar"), "HEAD").unwrap();
    }

    /// A stalled decoder is bounded during reads and its owned leader is reaped;
    /// an oversized zero stream is refused without waiting for successful exit.
    #[test]
    fn decoder_stall_and_oversized_output_are_bounded() {
        let root = tempfile::tempdir().unwrap();
        let pid_file = root.path().join("pid");
        let start = Instant::now();
        let mut command = Command::new("/bin/sh");
        command
            .args(["-c", "echo $$ > \"$1\"; exec /bin/sleep 2", "fixture"])
            .arg(&pid_file);
        assert!(inspect_decoder(&mut command, true, Duration::from_millis(200)).is_err());
        assert!(start.elapsed() < Duration::from_secs(2));
        let pid: i32 = std::fs::read_to_string(pid_file)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert!(
            agent_run_platform::process::inspect(pid).is_err(),
            "decoder must be reaped"
        );
        let mut command = Command::new("/usr/bin/head");
        command.args(["-c", "1049601", "/dev/zero"]);
        let error = inspect_decoder(&mut command, true, Duration::from_secs(2)).unwrap_err();
        assert!(error.contains("padding limit"), "{error}");
    }
}
