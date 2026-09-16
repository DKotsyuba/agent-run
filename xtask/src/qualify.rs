//! Fail-closed qualification reporting from the recorded migration evidence.

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

/// The number of scenarios in the authority scope map.
const SCENARIO_COUNT: usize = 84;

/// The live portions that a fixture cannot prove, named by the scope rows that require them.
const LIVE_PORTIONS: [(&[&str], &str, &str); 3] = [
    (
        &["T57", "T58", "T59"],
        "real engine",
        "an installed qwen binary",
    ),
    (
        &["T71"],
        "ChatGPT Desktop host",
        "the real ChatGPT Desktop host",
    ),
    (&["T82"], "second operating system", "a non-macOS machine"),
];

/// Returns the canonical host platform name used in qualification reports.
pub fn host_platform() -> String {
    let os = match std::env::consts::OS {
        "macos" => "macOS",
        other => other,
    };
    let architecture = match std::env::consts::ARCH {
        "aarch64" => "arm64",
        other => other,
    };
    format!("{os}/{architecture}")
}

/// Checks recorded evidence for `platform` and returns a human-readable report.
///
/// `root` is the repository root containing `migration/adr/A15-platforms.md`
/// and `migration/evidence`. `platform` is a canonical `os/architecture` name
/// or one of the runtime aliases (`darwin`, `aarch64`, and so on). When
/// `release` is true, a successful recorded release smoke is required in
/// addition to a successful full-suite record. This function only reads files;
/// it never creates or updates qualification evidence.
pub fn qualify(root: &Path, platform: &str, release: bool) -> Result<String, String> {
    let platform = Platform::parse(platform)?;
    let scope = read_scope(root)?;
    let evidence = read_evidence(root)?;
    if !has_platform_run(&evidence, &platform) {
        return Err(format!(
            "refusing qualification for {}: no recorded full-suite evidence proves this host platform",
            platform.display()
        ));
    }
    if release && !has_release_run(&evidence) {
        return Err(format!(
            "refusing release qualification for {}: no recorded successful release smoke",
            platform.display()
        ));
    }
    Ok(report(&platform, release, &scope))
}

/// Stores the normalized operating-system and architecture pair being checked.
#[derive(Debug, PartialEq, Eq)]
struct Platform {
    /// Canonical operating-system name, such as `macos` or `linux`.
    os: String,
    /// Canonical architecture name, such as `arm64` or `x86_64`.
    architecture: String,
}

impl Platform {
    /// Parses a platform name and rejects missing or ambiguous components.
    fn parse(value: &str) -> Result<Self, String> {
        let (os, architecture) = value
            .split_once('/')
            .ok_or_else(|| format!("invalid platform {value:?}; expected os/architecture"))?;
        if os.is_empty() || architecture.is_empty() || architecture.contains('/') {
            return Err(format!(
                "invalid platform {value:?}; expected nonblank os/architecture"
            ));
        }
        let os = match os.to_ascii_lowercase().as_str() {
            "darwin" => "macos".to_owned(),
            other => other.to_owned(),
        };
        let architecture = match architecture.to_ascii_lowercase().as_str() {
            "aarch64" => "arm64".to_owned(),
            other => other.to_owned(),
        };
        Ok(Self { os, architecture })
    }

    /// Formats this normalized pair for diagnostics.
    fn display(&self) -> String {
        let os = if self.os == "macos" {
            "macOS"
        } else {
            &self.os
        };
        format!("{os}/{}", self.architecture)
    }
}

/// Summarizes the scope rows needed by the report.
struct Scope {
    /// Number of rows whose status supplies deterministic scenario evidence.
    covered: usize,
    /// IDs whose `live` column says a native boundary remains.
    live: BTreeSet<String>,
}

/// Reads and validates the static scenario scope map.
fn read_scope(root: &Path) -> Result<Scope, String> {
    let path = root.join("migration/evidence/qualification-scope.md");
    let text = fs::read_to_string(&path)
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    let mut ids = BTreeSet::new();
    let mut covered = 0;
    let mut live = BTreeSet::new();
    for line in text.lines().filter(|line| line.starts_with("| T")) {
        let fields = line.split('|').map(str::trim).collect::<Vec<_>>();
        if fields.len() != 8 {
            return Err(format!("invalid qualification scope row: {line}"));
        }
        let id = fields[1];
        if !ids.insert(id.to_owned()) {
            return Err(format!("duplicate qualification scope row: {id}"));
        }
        match fields[4] {
            "covered" | "partial" => covered += 1,
            "not-covered" => {}
            status => return Err(format!("invalid qualification status {status:?} for {id}")),
        }
        match fields[6] {
            "yes" => {
                live.insert(id.to_owned());
            }
            "no" => {}
            value => return Err(format!("invalid live marker {value:?} for {id}")),
        }
    }
    if ids.len() != SCENARIO_COUNT {
        return Err(format!(
            "qualification scope has {} scenarios; expected {SCENARIO_COUNT}",
            ids.len()
        ));
    }
    for number in 1..=SCENARIO_COUNT {
        let id = format!("T{number:02}");
        if !ids.contains(&id) {
            return Err(format!("qualification scope is missing scenario {id}"));
        }
    }
    Ok(Scope { covered, live })
}

/// Reads the ADR and all regular evidence files without following directories via links.
fn read_evidence(root: &Path) -> Result<Vec<(PathBuf, String)>, String> {
    let mut paths = vec![root.join("migration/adr/A15-platforms.md")];
    collect_files(&root.join("migration/evidence"), &mut paths)?;
    paths.sort();
    paths.dedup();
    paths
        .into_iter()
        .map(|path| {
            fs::read_to_string(&path)
                .map(|text| (path.clone(), text))
                .map_err(|error| format!("cannot read evidence {}: {error}", path.display()))
        })
        .collect()
}

/// Recursively collects regular files below an evidence directory.
fn collect_files(directory: &Path, paths: &mut Vec<PathBuf>) -> Result<(), String> {
    for entry in fs::read_dir(directory)
        .map_err(|error| format!("cannot read {}: {error}", directory.display()))?
    {
        let path = entry
            .map_err(|error| format!("cannot read evidence directory entry: {error}"))?
            .path();
        let metadata = fs::symlink_metadata(&path).map_err(|error| error.to_string())?;
        if metadata.is_dir() {
            collect_files(&path, paths)?;
        } else if metadata.is_file() {
            paths.push(path);
        }
    }
    Ok(())
}

/// Finds a run record that states the tested platform and successful full suite.
fn has_platform_run(evidence: &[(PathBuf, String)], platform: &Platform) -> bool {
    evidence.iter().any(|(_, text)| {
        let lower = text.to_ascii_lowercase().replace('\n', " ");
        let os = if platform.os == "macos" {
            lower.contains("macos") || lower.contains("darwin")
        } else {
            lower.contains(&platform.os)
        };
        let architecture = lower.contains(&platform.architecture)
            || (platform.architecture == "arm64" && lower.contains("aarch64"));
        let run_statement =
            lower.contains("ran on") || lower.contains("runs on") || lower.contains("executed on");
        os && architecture && run_statement
    }) && evidence.iter().any(|(_, text)| {
        text.contains("Full suite") && text.contains("passed") && text.contains("0 failed")
    })
}

/// Finds a recorded successful release smoke for the `--release` gate.
fn has_release_run(evidence: &[(PathBuf, String)]) -> bool {
    evidence.iter().any(|(_, text)| {
        text.contains("Release smoke") && text.contains("--release") && text.contains("exit 0")
    })
}

/// Renders the qualification decision and all outstanding live requirements.
fn report(platform: &Platform, release: bool, scope: &Scope) -> String {
    let mut report = format!(
        "qualification: platform evidence present for {}\nrule: a platform is evidenced only by a recorded run naming that OS/architecture and a successful full suite; this follows A15's evidence bar and does not decide future support\nscenarios: {}/{} covered by the scope map (T01–T84)\n",
        platform.display(),
        scope.covered,
        SCENARIO_COUNT
    );
    if release {
        report.push_str("release: recorded release smoke required and present\n");
    } else {
        report.push_str(
            "release: debug qualification; pass --release to require release-smoke evidence\n",
        );
    }
    let live = LIVE_PORTIONS
        .iter()
        .filter(|(ids, _, _)| ids.iter().any(|id| scope.live.contains(*id)))
        .collect::<Vec<_>>();
    report.push_str(&format!("live portions retained: {}\n", live.len()));
    for (ids, name, requirement) in live {
        report.push_str(&format!(
            "- {name} ({}): pending; requires {requirement}\n",
            ids.join(", ")
        ));
    }
    report.push_str(
        "result: host platform evidenced; pending live portions are not claimed as run\n",
    );
    report
}
