# Vendored apache/iceberg-rust `iceberg-storage-opendal` 0.10.1

This fork is based on the published `iceberg-storage-opendal` crate 0.10.1.
Siglake retains multipart controls, upload-class permits, retry policy,
observability, and its reqsign 3 AWS credential provider.

The upstream base is commit
`04ae06bdb15a6fd7c7927d29d4e0f6a33de0f1f9`, `path_in_vcs`
`crates/storage/opendal`. The adjacent `.cargo_vcs_info.json` records the same
provenance; local differences are listed in `third_party/README.md`.
