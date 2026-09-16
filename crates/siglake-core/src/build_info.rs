/// Version and source revision embedded when `siglake-core` was built.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BuildInfo {
    pub version: &'static str,
    pub commit: &'static str,
}

/// Version string used by the `siglake`, `siglake-query-server`, and
/// `siglake-operator` binaries' `--version` output.
pub const BUILD_VERSION: &str = env!("SIGLAKE_BUILD_VERSION");

/// Return the build provenance shared by every deployable binary.
pub const fn build_info() -> BuildInfo {
    BuildInfo {
        version: env!("CARGO_PKG_VERSION"),
        commit: match option_env!("SIGLAKE_GIT_SHA") {
            Some(commit) => commit,
            None => "unknown",
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_version_contains_the_embedded_provenance() {
        let info = build_info();
        assert_eq!(info.version, env!("CARGO_PKG_VERSION"));
        assert!(!info.commit.is_empty());
        assert_eq!(BUILD_VERSION, format!("{} ({})", info.version, info.commit));
    }
}
