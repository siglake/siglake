use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=SIGLAKE_GIT_SHA");
    // Cargo can share a build-script fingerprint between Git worktrees when
    // they use the same target directory. Make the invoking worktree part of
    // that fingerprint before relying on the absolute Git paths below.
    println!("cargo:rerun-if-env-changed=PWD");
    track_git_head();

    let commit = supplied_commit()
        .or_else(git_commit)
        .unwrap_or_else(|| "unknown".to_string());
    let version = std::env::var("CARGO_PKG_VERSION").expect("Cargo sets CARGO_PKG_VERSION");

    println!("cargo:rustc-env=SIGLAKE_GIT_SHA={commit}");
    println!("cargo:rustc-env=SIGLAKE_BUILD_VERSION={version} ({commit})");
}

fn supplied_commit() -> Option<String> {
    std::env::var("SIGLAKE_GIT_SHA")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn git_commit() -> Option<String> {
    git_output(["rev-parse", "--short", "HEAD"])
}

fn track_git_head() {
    let Some(head) = git_path("HEAD") else {
        return;
    };
    track_path(&head);

    // The reflog changes for symbolic, detached, loose, and packed HEADs.
    if let Some(head_log) = git_path("logs/HEAD") {
        track_path(&head_log);
    }

    if let Some(reference) = git_output(["symbolic-ref", "-q", "HEAD"]) {
        if let Some(reference_path) = git_path(&reference) {
            track_path(&reference_path);
        }
    }
    if let Some(packed_refs) = git_path("packed-refs") {
        track_path(&packed_refs);
    }
}

fn track_path(path: &Path) {
    if path.is_file() {
        println!("cargo:rerun-if-changed={}", path.display());
    }
}

fn git_path(path: &str) -> Option<PathBuf> {
    let path = PathBuf::from(git_output(["rev-parse", "--git-path", path])?);
    if path.is_absolute() {
        Some(path)
    } else {
        Some(manifest_dir().join(path))
    }
}

fn git_output<const N: usize>(args: [&str; N]) -> Option<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(manifest_dir())
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?;
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn manifest_dir() -> PathBuf {
    PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").expect("Cargo sets CARGO_MANIFEST_DIR"))
}
