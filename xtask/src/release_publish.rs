//! Staged publisher for one previously built and accepted immutable inventory.
//! Source verification code never builds a second payload or executes its bytes.
use crate::{
    delivery::{self, MANIFEST, Manifest, REPOSITORY, WORKFLOW_PATH},
    release_ops::field,
    release_wait,
};
use std::{
    fs,
    path::Path,
    time::{Duration, Instant},
};

/// Runs a bounded explicit GitHub command; failures retain the draft and scratch.
fn gh(root: &Path, args: Vec<String>, deadline: Instant) -> Result<(), String> {
    let timeout = release_wait::budget(deadline, 180).map_err(|e| e.reason)?;
    if delivery::run("gh", &args, root, timeout)?.0 != 0 {
        return Err("GitHub publication command failed; draft/evidence retained".into());
    }
    Ok(())
}

/// Enforces cryptographic producer verification without an authenticated fallback.
/// Expected repo/workflow/ref/full source are supplied by the reviewed producer.
pub fn attestation(
    root: &Path,
    path: &Path,
    manifest: &Manifest,
    deadline: Instant,
) -> Result<(), String> {
    gh(
        root,
        vec![
            "attestation".into(),
            "verify".into(),
            path.to_string_lossy().into_owned(),
            "--hostname".into(),
            "github.com".into(),
            "--repo".into(),
            REPOSITORY.into(),
            "--signer-workflow".into(),
            format!("{REPOSITORY}/{WORKFLOW_PATH}"),
            "--signer-digest".into(),
            manifest.commit.clone(),
            "--source-digest".into(),
            manifest.commit.clone(),
            "--source-ref".into(),
            format!("refs/tags/{}", manifest.tag),
            "--deny-self-hosted-runners".into(),
            "--limit".into(),
            "10".into(),
        ],
        deadline,
    )
}

/// Finds an exact release without assuming the tag endpoint exposes drafts.
/// After its allowed 404, authenticated list/ID GETs share the caller's deadline
/// and existing API byte/retry limits. At most five 100-entry pages are scanned;
/// malformed, ambiguous, inaccessible or incomplete inventories fail closed.
/// A listed candidate is reread by its positive numeric ID, with tag, ID and
/// draft state checked again. No release is created, changed or retried here.
fn lookup(root: &Path, tag: &str, deadline: Instant) -> Result<Option<serde_json::Value>, String> {
    let endpoint = format!("repos/{REPOSITORY}/releases/tags/{tag}");
    if let Some(release) =
        release_wait::api(root, &endpoint, deadline, true).map_err(|e| e.reason)?
    {
        return Ok(Some(release));
    }
    let mut candidate = None;
    for page in 1..=5 {
        let response = release_wait::api(
            root,
            &format!("repos/{REPOSITORY}/releases?per_page=100&page={page}"),
            deadline,
            false,
        )
        .map_err(|e| e.reason)?
        .ok_or("release list unavailable")?;
        let releases = response.as_array().ok_or("release list invalid")?;
        if releases.len() > 100 {
            return Err("release list page limit exceeded".into());
        }
        for release in releases {
            if release["tag_name"] == tag {
                if candidate.is_some() {
                    return Err("ambiguous releases for publication tag".into());
                }
                let id = release["id"]
                    .as_u64()
                    .filter(|id| *id > 0)
                    .ok_or("listed release ID invalid")?;
                let draft = release["draft"]
                    .as_bool()
                    .ok_or("listed release draft state invalid")?;
                candidate = Some((id, draft));
            }
        }
        if releases.len() < 100 {
            break;
        }
        if page == 5 {
            return Err("release pagination limit reached".into());
        }
    }
    let Some((id, draft)) = candidate else {
        return Ok(None);
    };
    let release = release_wait::api(
        root,
        &format!("repos/{REPOSITORY}/releases/{id}"),
        deadline,
        false,
    )
    .map_err(|e| e.reason)?
    .ok_or("listed release unavailable")?;
    if release["id"].as_u64() != Some(id) || release["tag_name"] != tag || release["draft"] != draft
    {
        return Err("listed release identity changed".into());
    }
    Ok(Some(release))
}

/// Downloads and verifies remote inventory in fresh retained scratch. Drafts and
/// published releases use identical hash/size/evidence checks; neither is overwritten.
fn remote(
    root: &Path,
    manifest: &Manifest,
    expected: &serde_json::Value,
    published: bool,
    deadline: Instant,
) -> Result<(), String> {
    release_wait::release_identity(expected, &manifest.tag, published).map_err(|e| e.reason)?;
    fs::create_dir_all(root.join("target/release-publisher"))
        .map_err(|_| "publisher scratch unavailable")?;
    let dir = tempfile::Builder::new()
        .prefix("verify-")
        .tempdir_in(root.join("target/release-publisher"))
        .map_err(|_| "publisher scratch unavailable")?
        .keep();
    release_wait::download(root, &manifest.tag, &dir, deadline, true).map_err(|e| e.reason)?;
    let bytes = delivery::read_regular(&dir.join(MANIFEST), 65536)?;
    let actual: Manifest = serde_json::from_slice(&bytes).map_err(|_| "remote manifest invalid")?;
    if &actual != manifest {
        return Err("existing release identity/attempt conflicts; no overwrite".into());
    }
    release_wait::inventory(expected, manifest, bytes.len() as u64).map_err(|e| e.reason)?;
    release_wait::download(root, &manifest.tag, &dir, deadline, false).map_err(|e| e.reason)?;
    delivery::verify_until(&dir, &manifest.commit, Some(&manifest.workflow), deadline)?;
    for artifact in &manifest.artifacts {
        attestation(root, &dir.join(&artifact.name), manifest, deadline)?;
    }
    attestation(root, &dir.join(MANIFEST), manifest, deadline)?;
    Ok(())
}

/// Explicit privileged publication command. Requires the producer's actual
/// source/run/attempt, complete inventory, nonempty notes and enforced provenance.
/// Existing drafts fail closed; a published release is a no-op only for exact bytes.
pub fn command(root: &Path, args: &[String]) -> Result<(), String> {
    if args.len() != 6
        || !args
            .chunks(2)
            .all(|pair| ["--accepted-commit", "--directory", "--notes"].contains(&pair[0].as_str()))
    {
        return Err("publication requires exactly accepted-commit, directory and notes".into());
    }
    let commit = field(args, "--accepted-commit")?;
    let identity = crate::release_ops::verify_source(root, &commit, true)?;
    let workflow = identity
        .workflow
        .clone()
        .ok_or("publication requires actual GitHub Actions identity")?;
    let directory = fs::canonicalize(field(args, "--directory")?)
        .map_err(|_| "publication inventory missing")?;
    let manifest = delivery::verify(&directory, &commit, Some(&workflow))?;
    if delivery::Manifest::from_identity(&identity, manifest.artifacts.clone())? != manifest {
        return Err("accepted-source family identity conflict".into());
    }
    let evidence: delivery::Acceptance = serde_json::from_slice(&delivery::read_regular(
        &directory.join(delivery::ACCEPTANCE),
        262144,
    )?)
    .map_err(|_| "producer evidence invalid")?;
    if evidence.identity != identity {
        return Err("accepted-source toolchain/target identity conflict".into());
    }
    let notes =
        fs::canonicalize(field(args, "--notes")?).map_err(|_| "release notes unavailable")?;
    let notes_meta = fs::symlink_metadata(&notes).map_err(|_| "release notes unavailable")?;
    if !notes_meta.is_file()
        || notes_meta.len() == 0
        || notes_meta.len() > 65536
        || fs::read_to_string(&notes)
            .map_err(|_| "release notes invalid")?
            .trim()
            .is_empty()
    {
        return Err("bounded nonempty release notes required".into());
    }
    let deadline = Instant::now() + Duration::from_secs(900);
    release_wait::preflight(root, deadline).map_err(|e| e.reason)?;
    let tag = release_wait::tag(root, &manifest.tag, &commit, deadline)
        .map_err(|e| e.reason)?
        .ok_or("publication tag absent")?;
    let run = release_wait::api(
        root,
        &format!("repos/{REPOSITORY}/actions/runs/{}", workflow.run_id),
        deadline,
        false,
    )
    .map_err(|e| e.reason)?
    .ok_or("publication run absent")?;
    release_wait::validate_run(&run, &workflow, &manifest.tag, &commit).map_err(|e| e.reason)?;
    for artifact in &manifest.artifacts {
        attestation(root, &directory.join(&artifact.name), &manifest, deadline)?;
    }
    attestation(root, &directory.join(MANIFEST), &manifest, deadline)?;
    let endpoint = format!("repos/{REPOSITORY}/releases/tags/{}", manifest.tag);
    if let Some(existing) = lookup(root, &manifest.tag, deadline)? {
        if existing["draft"] != false {
            return Err("existing draft requires manual reconciliation; no overwrite".into());
        }
        remote(root, &manifest, &existing, true, deadline)?;
        if release_wait::tag(root, &manifest.tag, &commit, deadline).map_err(|e| e.reason)?
            != Some(tag)
        {
            return Err("tag changed during no-op verification".into());
        }
        let final_release = release_wait::api(root, &endpoint, deadline, false)
            .map_err(|e| e.reason)?
            .ok_or("final existing release absent")?;
        if final_release["id"] != existing["id"]
            || final_release["assets"] != existing["assets"]
            || final_release["draft"] != false
            || final_release["prerelease"] != false
        {
            return Err("existing release changed during no-op".into());
        }
        eprintln!("Exact published release verified; no-op");
        return Ok(());
    }
    let mut create = vec![
        "release".into(),
        "create".into(),
        manifest.tag.clone(),
        "--repo".into(),
        REPOSITORY.into(),
        "--draft".into(),
        "--verify-tag".into(),
        "--title".into(),
        manifest.tag.clone(),
        "--notes-file".into(),
        notes.to_string_lossy().into_owned(),
    ];
    for name in manifest
        .artifacts
        .iter()
        .map(|a| a.name.as_str())
        .chain([MANIFEST, "SHA256SUMS"])
    {
        create.push(directory.join(name).to_string_lossy().into_owned());
    }
    gh(root, create, deadline)?;
    let draft = lookup(root, &manifest.tag, deadline)?.ok_or("created draft unavailable")?;
    remote(root, &manifest, &draft, false, deadline)?;
    if release_wait::tag(root, &manifest.tag, &commit, deadline).map_err(|e| e.reason)?
        != Some(tag.clone())
    {
        return Err("tag moved before publication".into());
    }
    gh(
        root,
        vec![
            "release".into(),
            "edit".into(),
            manifest.tag.clone(),
            "--repo".into(),
            REPOSITORY.into(),
            "--draft=false".into(),
        ],
        deadline,
    )?;
    let published = release_wait::api(root, &endpoint, deadline, false)
        .map_err(|e| e.reason)?
        .ok_or("published release unavailable")?;
    if published["id"] != draft["id"] {
        return Err("publication identity changed".into());
    }
    remote(root, &manifest, &published, true, deadline)?;
    if release_wait::tag(root, &manifest.tag, &commit, deadline).map_err(|e| e.reason)? != Some(tag)
    {
        return Err("final publication tag changed".into());
    }
    let final_run = release_wait::api(
        root,
        &format!("repos/{REPOSITORY}/actions/runs/{}", workflow.run_id),
        deadline,
        false,
    )
    .map_err(|e| e.reason)?
    .ok_or("final publication run absent")?;
    release_wait::validate_run(&final_run, &workflow, &manifest.tag, &commit)
        .map_err(|e| e.reason)?;
    Ok(())
}
