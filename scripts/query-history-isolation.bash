# Test-only isolation; source only after env -i. No DB/URI configuration is read.
# Pinned build tools/target are set separately by the caller.
history_isolate_environment() {
  local work=$1 cache=/data/homes/gianarb/.cargo/registry part native
  # Resolve native tools before HOME changes even in runners that repeat their
  # version assertions below. Never allow rustup/home-dependent resolution.
  for part in cargo rustc rustdoc cargo-clippy clippy-driver rustfmt; do
    native=$(command -v "$part") || return 1
    [[ $native == /data/homes/gianarb/.local/state/gastone-agent-roadmap/milestone-2-auth/pinned/toolchain/bin/* && ! -L $native && $(file -b "$native") == ELF* ]] || return 1
  done
  [[ -d "$work" && $(stat -c %a "$work") == 700 ]] || return 1
  [[ -z ${DATABASE_URL+x} ]] || return 1
  # new_without_pgpass still imports PGHOST/PGUSER/PGSSL*/PGOPTIONS/etc.
  # Reject EVERY inherited PG-prefixed variable before constructing options.
  for part in "${!PG@}"; do [[ -z "$part" ]] || return 1; done
  export HOME="$work/home"
  export XDG_CONFIG_HOME="$work/xdg-config" XDG_CACHE_HOME="$work/xdg-cache"
  export XDG_DATA_HOME="$work/xdg-data" XDG_STATE_HOME="$work/xdg-state"
  mkdir -m 700 "$HOME" "$XDG_CONFIG_HOME" "$XDG_CACHE_HOME" "$XDG_DATA_HOME" "$XDG_STATE_HOME" "$work/providers"
  # SQLx 0.8.6 falls back to HOME/.pgpass even when PGPASSFILE is empty.
  export PGPASSFILE="$work/empty.pgpass"
  # Override constructor defaults BEFORE new_without_pgpass runs, including
  # SQLx's default-host socket probing and OS-username fallback.
  export PGHOST="$work" PGPORT=5432 PGUSER=fixture_admin PGDATABASE=qh_fixture
  export PGPASSWORD='' PGSSLMODE=disable PGAPPNAME=siglake-history-fixture
  export AWS_CONFIG_FILE="$work/providers/aws-config" AWS_SHARED_CREDENTIALS_FILE="$work/providers/aws-credentials"
  export AWS_EC2_METADATA_DISABLED=true AWS_EC2_METADATA_V1_DISABLED=true
  export BOTO_CONFIG="$work/providers/boto-config" GOOGLE_APPLICATION_CREDENTIALS="$work/providers/google-credentials"
  export CLOUDSDK_CONFIG="$work/providers/gcloud" AZURE_CONFIG_DIR="$work/providers/azure"
  export KUBECONFIG="$work/providers/kubeconfig" DOCKER_CONFIG="$work/providers/docker" GH_CONFIG_DIR="$work/providers/gh"
  mkdir -m 700 "$CLOUDSDK_CONFIG" "$AZURE_CONFIG_DIR" "$DOCKER_CONFIG" "$GH_CONFIG_DIR"
  for part in "$PGPASSFILE" "$HOME/.pgpass" "$AWS_CONFIG_FILE" "$AWS_SHARED_CREDENTIALS_FILE" "$BOTO_CONFIG" "$GOOGLE_APPLICATION_CREDENTIALS" "$KUBECONFIG"; do
    : > "$part"
    chmod 600 "$part"
    [[ ! -s "$part" && $(stat -c %a "$part") == 600 ]] || return 1
  done
  # Keep default provider locations empty too. All fixture runners share this
  # helper; explicit overrides alone must not leave a fallback location unclear.
  mkdir -m 700 "$HOME/.aws" "$HOME/.config" "$HOME/.config/gcloud" "$HOME/.azure" "$HOME/.kube"
  for part in "$HOME/.aws/config" "$HOME/.aws/credentials" "$HOME/.boto" "$HOME/.config/gcloud/application_default_credentials.json" "$HOME/.azure/accessTokens.json" "$HOME/.azure/azureProfile.json" "$HOME/.kube/config"; do
    : > "$part"
    chmod 600 "$part"
  done
  # Share ONLY cached public registry sources/index/archives; never Cargo config,
  # credential files, bin, Git config or an original HOME. Tool metadata is private.
  export CARGO_HOME=/data/homes/gianarb/.local/state/gastone-agent-roadmap/milestone-2-ndjson/isolated-cargo
  mkdir -p -m 700 "$CARGO_HOME/registry"
  [[ ! -e "$CARGO_HOME/config" && ! -e "$CARGO_HOME/config.toml" && ! -e "$CARGO_HOME/credentials" && ! -e "$CARGO_HOME/credentials.toml" ]] || return 1
  for part in src index cache; do
    if [[ ! -L "$CARGO_HOME/registry/$part" ]]; then
      ln -s "$cache/$part" "$CARGO_HOME/registry/$part" || return 1
    fi
    [[ $(readlink "$CARGO_HOME/registry/$part") == "$cache/$part" ]] || return 1
  done
  export CARGO_NET_OFFLINE=true
  printf '%s\n' 'Isolation: private HOME/XDG/provider dirs; empty 0600 explicit PGPASSFILE and private HOME/.pgpass; empty default/explicit AWS/provider files; AWS metadata disabled; env -i; offline locked cache-only build.'
  for part in src index cache; do
    printf 'Cargo registry %s -> %s\n' "$part" "$(readlink "$CARGO_HOME/registry/$part")"
  done
}
