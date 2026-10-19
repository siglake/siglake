# Contributing to Siglake

We welcome bug reports, documentation improvements, tests, and code changes.
Please follow our [Code of Conduct](CODE_OF_CONDUCT.md) when participating.

## Getting started

Check [existing issues](https://github.com/siglake/siglake/issues) before opening
one. For a bug, include the Siglake version, a small reproduction, and the
expected and actual behavior. Remove credentials and sensitive data from logs.
Report security vulnerabilities privately using [SECURITY.md](SECURITY.md).

For a substantial feature or architectural change, open an issue to discuss the
approach before investing in an implementation. Small fixes can go straight to
a pull request.

## Build and test

Fork the repository and create a branch from `main`. Install Rust with
[rustup](https://rustup.rs/); it selects the toolchain pinned in
[`rust-toolchain.toml`](rust-toolchain.toml).

```sh
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
```

The workspace tests need no cloud credentials or external services. To run the
local service stack with Docker Compose, follow the
[README quickstart](README.md#try-it-locally).

[`scripts/ci-local.sh`](scripts/ci-local.sh) runs the combined local checks and
reports any missing tools. Use `--all --strict` for the full integration gate;
this also requires Docker, kind, and the external-reader dependencies described
in the script. Include the checks you ran and any skips in your pull request.

The Python MCP adapter has separate tests and setup instructions in
[`integrations/mcp/README.md`](integrations/mcp/README.md).

## Submit a pull request

Keep changes focused, explain the problem and resulting behavior, and add or
update tests for behavior changes. Update documentation when an interface or
configuration changes, and record new limitations in
[`docs/LIMITATIONS.md`](docs/LIMITATIONS.md).

Follow the surrounding code's conventions. The
[architecture guide](docs/ARCHITECTURE.md) explains the main components;
[`third_party/README.md`](third_party/README.md) describes the vendored forks
and their additional checks.

Sign off each commit and open a pull request against `main`. Address review
feedback and make sure the relevant CI checks pass before merging. CI for a
fork's pull request may wait for maintainer approval, including after a new
push; this does not mean your change failed.

### Licensing and sign-off

Contributions use the project's [Apache License 2.0](LICENSE). There is no
Contributor License Agreement (CLA).

Every commit must include a `Signed-off-by:` trailer certifying the
[Developer Certificate of Origin 1.1](https://developercertificate.org/): you
wrote the change or have the right to contribute it under the project's license.
Add the trailer using your configured name and email:

```sh
git commit -s
```

To add a missing sign-off to your latest commit, use `git commit --amend -s`.
