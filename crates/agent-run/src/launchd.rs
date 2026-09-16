//! macOS LaunchAgent documents for the broker and bounded maintenance jobs.

use crate::{error::invalid, Error, Result};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

/// Escapes text placed in the deliberately small XML plist emitter.
fn xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Returns an absolute path or rejects the caller-provided launchd path.
fn absolute(path: PathBuf, field: &str) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path)
    } else {
        Err(invalid(format!("{field} must be an absolute path")))
    }
}

/// Renders one Python-compatible LaunchAgent descriptor without registering it.
///
/// `kind` is one of `api`, `capacity`, or `delivery`; `interval` is used only
/// by one-shot workers. The function never calls `launchctl`, so callers may
/// safely render a plist in a disposable directory. Linux is explicitly
/// unsupported because systemd is a separate product surface.
pub fn render(
    home: &Path,
    binary: PathBuf,
    kind: &str,
    interval: u64,
    label: &str,
    stdout_log: PathBuf,
    stderr_log: PathBuf,
) -> Result<Value> {
    if !cfg!(target_os = "macos") {
        return Err(Error::Unsupported(
            "launchd is only available on macOS; systemd support is not implemented".into(),
        ));
    }
    if label.trim().is_empty() {
        return Err(invalid("launchd label must be a nonblank string"));
    }
    let binary = absolute(binary, "binary")?;
    let stdout_log = absolute(stdout_log, "stdout_log")?;
    let stderr_log = absolute(stderr_log, "stderr_log")?;
    let args: &[&str] = match kind {
        "api" => &["api", "serve"],
        "capacity" => &["capacity", "collect", "--once"],
        "delivery" => &["delivery", "dispatch"],
        _ => return Err(invalid("unknown launchd job")),
    };
    let mut argv = vec![binary.display().to_string()];
    if kind != "capacity" {
        argv.extend(["--home".into(), home.display().to_string()]);
    }
    argv.extend(args.iter().map(|value| (*value).into()));
    let program_arguments = argv
        .iter()
        .map(|value| format!("      <string>{}</string>\n", xml(value)))
        .collect::<String>();
    let mut environment = String::new();
    if kind != "delivery" {
        environment.push_str(&format!(
            "  <key>EnvironmentVariables</key><dict><key>HOME</key><string>{}</string>",
            xml(&std::env::var("HOME").map_err(|_| invalid("HOME is missing"))?)
        ));
        if let Ok(path) = std::env::var("PATH") {
            environment.push_str(&format!("<key>PATH</key><string>{}</string>", xml(&path)));
        }
        environment.push_str("</dict>\n");
    }
    let schedule = match kind {
        "api" => "  <key>RunAtLoad</key><true/>\n  <key>KeepAlive</key><true/>\n".to_owned(),
        _ => {
            if interval == 0 {
                return Err(invalid("interval_seconds must be an integer of at least 1"));
            }
            format!(
                "  <key>StartInterval</key><integer>{interval}</integer>\n  <key>RunAtLoad</key><false/>\n"
            )
        }
    };
    let limits = if kind == "api" {
        "  <key>SoftResourceLimits</key><dict><key>NumberOfFiles</key><integer>65536</integer></dict>\n"
    } else {
        ""
    };
    let plist = format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\"><dict>\n  <key>Label</key><string>{}</string>\n  <key>ProgramArguments</key><array>\n{}  </array>\n{}{}{}  <key>StandardOutPath</key><string>{}</string>\n  <key>StandardErrorPath</key><string>{}</string>\n</dict></plist>\n", xml(label), program_arguments, environment, schedule, limits, xml(&stdout_log.display().to_string()), xml(&stderr_log.display().to_string()));
    Ok(if kind == "api" {
        json!({"label":label,"argv":argv,"plist":plist})
    } else {
        json!({"label":label,"interval_seconds":interval,"argv":argv,"plist":plist})
    })
}
