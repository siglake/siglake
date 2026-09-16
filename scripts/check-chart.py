#!/usr/bin/env python3
"""Validate the rendered Helm chart for the drift class the 2026-08-29 survey found.

Two defects shipped because nothing rendered the chart in CI, and both are
visible in `helm template` output:

  - a Service whose named `targetPort` matches no container port. Kubernetes
    silently drops it: the EndpointSlice carries no port, connections are
    refused, and the pods stay Ready because probes address the container port
    directly.
  - an HPA `scaleTargetRef` naming a workload kind that does not exist. The HPA
    reports FailedGetScale on its own status and never scales; nothing else
    surfaces it.

Both fail loudly here instead.

The same script checks that every `siglake_*` series named by a PrometheusRule
expression, a KEDA trigger query or a panel in `deploy/grafana/*.json` is one
some `metrics::` macro in `crates/` actually emits, and that it is queried in
the form the exporter renders it: `_bucket` and `histogram_quantile()` only on
the histograms `siglake_core::metrics::builder()` hands buckets, a `quantile`
label only on the ones it leaves as summaries. A renamed metric otherwise blanks
a panel or silences an alert with no error anywhere, and a `_bucket` query on a
summary reads exactly the same nothing: four p99 panels and the KEDA query
trigger sat in that state until 2026-09-03 (#148). The bucket rule is parsed
from `crates/siglake-core/src/metrics.rs`, not copied here.

Every rendered PrometheusRule is also reduced to the native Prometheus rule
file shape (`groups:` without the Kubernetes CRD wrapper), passed to
`promtool check rules`, and exercised by `promtool test rules`. This catches
invalid PromQL and rule-file fields that YAML parsing and the metric-name
checks cannot, and tests alert timing against synthetic series. CI requires
promtool; a local full check reports this part as skipped when the binary is
not installed.

The same engine evaluates the drain-backlog panel's two expressions against a
fixture of two clusters, one draining through the catalog claim and one through
the local filesystem. The metric-name check above says a panel reads a series
something emits; it says nothing about what the panel computes, and this is the
panel where that is not obvious — `siglake_compactor_sealed_pending` is one
shared queue reported by every claim worker and a per-tenant local count under
the filesystem drain, and one expression has to chart both without multiplying
the backlog by the replica count (#3692).

It also holds the README to the PrometheusRule: the "No built-in UI" bullet
states how many alerts the rule ships, and that number is counted from the
template source here so it cannot drift as alerts are added.

And it holds the PrometheusRule to the pre-registration catalog. `metrics`
creates a series on its first write and Prometheus `increase()` needs two
samples, so a counter's first increment on a fresh pod is invisible to an
`increase(...) > 0` rule unless the series already existed at 0. Each binary
creates the counters its alerts read at startup from the `*_ALERTED_COUNTERS`
lists in `crates/siglake-core/src/metrics.rs`; every counter the template reads
through `increase()` must be in one of those lists or, with a reason, in
`UNREGISTERABLE_ALERTED_COUNTERS`, and every literal label set the code records
under a listed name must be a series the list creates.

And it holds the query pods to the one alert whose threshold is a chart value
rather than a constant: `SiglakeQueryWarmCycleStalled` fires after three warm
intervals with no completed cycle, and the interval it assumes must be the one
the pods RUN (`SIGLAKE_QUERY_WARM_INTERVAL_SECS`). Both come from
`query.warmIntervalSecs`, and every render asserts the rule's `for:` is three
times the container's value — or, at 0 (startup-only warm), that the rule is
not rendered at all.

`SiglakeMirrorReconciliationStalled` carries the other value-driven threshold,
`prometheusRule.mirrorRotationStallSecs`. It has no pod-side counterpart to be
held to (the compactor's page cadence is a different span), so what is checked
is that every range selector in the rendered expression IS that value rather
than a hardcoded window, that at 0 the rule is not rendered, and that both arms
survive: a bounded page counter and the durable rotation-generation gauge.
Either arm alone is not a stall — pages running is normal, and no rotation
completing is also what a deliberately disabled reconciliation looks like.

Finally, every Helm hook Job must carry the object-store credentials used by
any Deployment or StatefulSet in the same render. Hook Jobs run before the new
workloads and otherwise fail only at upgrade time, as the kind schema-migration
Job did when MinIO credentials lived under per-component `extraEnv`.

Every `SIGLAKE_*` name the chart renders, docker-compose sets, or the operator
places in a rendered pod must also occur in non-test Rust source. The source
catalog deliberately accepts every string literal, rather than only direct
`env::var` calls: clap attributes and the few closures that read a name passed
as an argument are binary reads too. This makes a code-side rename fail here
instead of leaving a dead environment variable in a deployment surface.

`--source-only` runs just the checks that read the tree — the metric catalog,
the environment-name catalog and non-Helm deployment surfaces, the dashboards,
and the README's alert count — and skips the render matrix, so a box without
helm still runs them instead of nothing. ci-local.sh reports that mode as its
own `dashboard` line; CI and the local `helm` job run the full script.
"""
import argparse
import collections
import dataclasses
import json
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile

import yaml

DASHBOARD_DIR = pathlib.Path("deploy/grafana")
METRIC_NAME = re.compile(r"\bsiglake_[a-z0-9_]+")
# A metric name and the label selector that follows it, if any, so a
# `quantile=` matcher can be tied to the series it selects.
SELECTOR = re.compile(r"\b(siglake_[a-z0-9_]+)\s*(\{[^}]*\})?")
QUANTILE_MATCHER = re.compile(r"\bquantile\s*(?:=~|!~|!=|=)")
HISTOGRAM_SUFFIX = re.compile(r"_(bucket|sum|count)$")
HISTOGRAM_QUANTILE = "histogram_quantile("

# Where the exposition form is decided. `metrics-exporter-prometheus` renders a
# `metrics::histogram!` as a Prometheus summary unless the recorder was handed
# buckets for its name; `builder()` here is the one place that hands them out.
METRICS_RS = pathlib.Path("crates/siglake-core/src/metrics.rs")
# The owned forks emit `siglake_*` metrics that ship in the same binaries as
# `crates/`: the Iceberg fork's reader owns the text-index and object-store read
# families. Read alongside `crates/` so a panel over one of them is not
# mistaken for a rename.
METRIC_FORK_ROOTS = (
    pathlib.Path("third_party/iceberg/src"),
    pathlib.Path("third_party/iceberg-catalog-sql/src"),
    pathlib.Path("third_party/iceberg-storage-opendal/src"),
)
MACRO = re.compile(r'metrics::(gauge|counter|histogram)!\(\s*"([a-z0-9_]+)"', re.S)
COUNT_HISTOGRAMS = re.compile(
    r"pub const COUNT_HISTOGRAMS:\s*&\[&str\]\s*=\s*&\[(.*?)\];", re.S
)
COUNT_HISTOGRAMS_LOOP = re.compile(r"\bfor\s+\w+\s+in\s+COUNT_HISTOGRAMS\b")
BUCKET_MATCHER = re.compile(r'Matcher::(Suffix|Prefix|Full)\(\s*"([a-z0-9_]+)"')
GLOBAL_BUCKETS = re.compile(r"\.set_buckets\(")
STR_LITERAL = re.compile(r'"([a-z0-9_]+)"')

# Deployment surfaces may set only names a binary knows. This is intentionally
# every exact SIGLAKE_* Rust string literal, not just env::var("..."): clap's
# `env = "..."`, pure resolver call sites and indirect env-reader closures are
# all real reads. Test modules and integration-test trees are not binaries we
# ship, so they cannot make a dead deployment variable look live.
ENV_NAME = re.compile(r"SIGLAKE_[A-Z0-9_]+")
ENV_NAME_LITERAL = re.compile(r'"(SIGLAKE_[A-Z0-9_]+)"')
CFG_TEST = re.compile(r"#\s*\[\s*cfg\s*\(\s*test\s*\)\s*\]")
RUST_PACKAGE_ROOTS = (pathlib.Path("crates"), pathlib.Path("third_party"))
COMPOSE_FILE = pathlib.Path("deploy/docker-compose.yml")
OPERATOR_RENDER_RS = pathlib.Path("crates/siglake-operator/src/render.rs")
CHART_DIR = pathlib.Path("deploy/helm/siglake")

# #4317: the install notes, and the one predicate that decides whether the query
# tier has any authentication at all. The note and the StatefulSet used to
# decide it separately, and the note's copy did not look at `query.oidc`.
NOTES_TEMPLATE = CHART_DIR / "templates/NOTES.txt"
QUERY_STS_TEMPLATE = CHART_DIR / "templates/statefulset-query-server.yaml"
QUERY_AUTH_HELPER = 'include "siglake.queryAuthOn"'
OPEN_QUERY_WARNING = "the query API is running open"
# The ConfigMap notes_probe_chart() renders the notes into, since a render
# cannot show them any other way.
NOTES_PROBE_NAME = "siglake-notes-probe"

# Pre-registration. The `AlertedCounter { name, series }` literals in
# metrics.rs are the catalog each binary creates at 0 on startup; `series` is
# either the UNLABELLED constant or a slice of label-pair slices. The loop in
# `preregister()` is what makes the list the rule, as with COUNT_HISTOGRAMS.
INCREASE_NAME = re.compile(r"\bincrease\(\s*(siglake_[a-z0-9_]+)")
ALERTED_COUNTER = re.compile(
    r'AlertedCounter\s*\{\s*name:\s*"([a-z0-9_]+)"\s*,\s*series:\s*'
    r"(UNLABELLED|&\[.*?\])\s*,?\s*\}",
    re.S,
)
INNER_SERIES = re.compile(r"&\[([^\[\]]*)\]")
LABEL_PAIR = re.compile(r'\(\s*"([a-z0-9_]+)"\s*,\s*"([^"]*)"\s*\)')
UNREGISTERABLE = re.compile(
    r"pub const UNREGISTERABLE_ALERTED_COUNTERS:\s*&\[\(&str,\s*&str\)\]\s*=\s*&\[(.*?)\];",
    re.S,
)
UNREGISTERABLE_NAME = re.compile(r'\(\s*"([a-z0-9_]+)"\s*,')
PREREGISTER_LOOP = re.compile(r"\bfor\s+\w+\s+in\s+\w+\.series\b")
COUNTER_MACRO = "metrics::counter!("
STR_ARG = re.compile(r'^"([^"]*)"$')

README = pathlib.Path("README.md")
RULE_TEMPLATE = pathlib.Path("deploy/helm/siglake/templates/prometheusrule.yaml")
PROMETHEUS_RULE_TESTS = pathlib.Path(
    "deploy/helm/siglake/tests/prometheusrule.test.yaml"
)
# One rule per `- alert:` line in the template SOURCE, not in a render: the
# render needs helm and `prometheusRule.enabled`, and
# SiglakeQueryWarmCycleStalled is itself wrapped in a
# `query.warmIntervalSecs > 0` conditional, so no single render sees every
# alert. The README's number is what an operator reading the file finds.
ALERT_LINE = re.compile(r"^\s*-\s+alert:\s*\S", re.M)
# The sentence in README's "No built-in UI" bullet. Anchored on the exact
# phrasing on purpose: if the wording changes the check must FAIL and be
# re-pointed, not silently stop guarding.
README_ALERT_COUNT = re.compile(r"`PrometheusRule` with (\d+) alerts")

# The warm-cadence coupling: the query container's env var and the alert whose
# `for:` (and staleness threshold) is three of those intervals.
WARM_ENV = "SIGLAKE_QUERY_WARM_INTERVAL_SECS"
WARM_ALERT = "SiglakeQueryWarmCycleStalled"
QUERY_CONTAINER = "query-server"
FILE_CACHE_BYTES_ENV = "SIGLAKE_QUERY_SCAN_FILE_CACHE_MAX_BYTES"
FILE_CACHE_ENTRIES_ENV = "SIGLAKE_QUERY_SCAN_FILE_CACHE_MAX_ENTRIES"
DEFAULT_FILE_CACHE_ENV = {
    FILE_CACHE_BYTES_ENV: "0",
    FILE_CACHE_ENTRIES_ENV: "0",
}
# The WAL mirror. Default-on in the binary since 2026-09-11, so the chart has
# to render the prefix in BOTH arms: an ingester with no
# SIGLAKE_WAL_MIRROR_PREFIX mirrors, and `wal.mirror.enabled: false` would then
# be a value that changes nothing. The reader side is the compactor's
# `--mirror-prefix`, which has to name the same string or a catalog-claim drain
# claims from a prefix nobody writes to.
INGEST_CONTAINER = "ingester"
COMPACTOR_CONTAINER = "compactor"
INDEX_REBUILD_ENV = "SIGLAKE_INDEX_REBUILD"
WAL_MIRROR_ENV = "SIGLAKE_WAL_MIRROR_PREFIX"
DEFAULT_WAL_MIRROR_PREFIX = "wal-mirror"
# #4273: the token allow-list, and the three values that can name its Secret.
# `<chart>` stands for the render's own `siglake.fullname`, so the expectation
# does not depend on the release name `render()` installs under.
QUERY_TOKENS_ENV = "SIGLAKE_QUERY_TOKENS"
CHART_QUERY_TOKENS_SECRET = "<chart>-query-tokens"
SPILL_DIR_ENV = "SIGLAKE_QUERY_SPILL_DIR"
SPILL_MAX_ENV = "SIGLAKE_QUERY_SPILL_MAX_BYTES"
# #967: the pod name the coordinator matches against the SRV answer to find its
# own worker URL — the failover target for a departed peer's shard.
PEER_SELF_NAME_ENV = "SIGLAKE_QUERY_PEER_SELF_NAME"
# The batch-job store, default-on since 2026-09-11: the catalog database, one
# table shared by every query replica. Its value is a `$(VAR)` expansion, which
# Kubernetes resolves only from entries earlier in the same env list.
JOBS_STORE_ENV = "SIGLAKE_JOBS_POSTGRES_URI"
CATALOG_URI_ENV = "SIGLAKE_CATALOG_URI"
HOOK_CREDENTIAL_ENV = re.compile(
    r"^(?:AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AWS_ENDPOINT_URL)$"
)
DEFAULT_QUERY_SPILL = {
    "directory": "/var/lib/siglake/spill",
    "max_bytes": "8589934592",
    "size_limit": "10Gi",
    "request": "10Gi",
    "limit": "12Gi",
}
# The template renders `for: {{ $stale }}s`; anything else is a format the
# check cannot hold to the pods and must fail rather than skip.
FOR_SECONDS = re.compile(r"(\d+)s")

# The stalled mirror-reconciliation alert and its window value. Both arms are
# named here so a one-armed rewrite (which would page on a cluster that has
# reconciliation switched off, or never page at all) fails instead of shipping.
MIRROR_ALERT = "SiglakeMirrorReconciliationStalled"
MIRROR_STALL_VALUE = "prometheusRule.mirrorRotationStallSecs"
DEFAULT_MIRROR_STALL_SECS = 21600
MIRROR_ALERT_ARMS = (
    "siglake_compactor_mirror_sync_total",
    "siglake_compactor_mirror_sync_rotations_completed",
)
MIRROR_PROGRESS_GAUGE = "siglake_compactor_mirror_sync_rotation_objects_examined"
RANGE_SECONDS = re.compile(r"\[\s*(\d+)([a-z]+)\s*\]")

# Gauges published by a maintenance sweep, for dashboards only. A live pod
# RETAINS the last value of each when the sweep that publishes it stops running
# — reconciliation switched off, the maintenance lease lost, the stage's
# watchdog tripping, delete-task execution disabled — and none of them exists
# before the first successful pass. An alert reading one of these therefore
# pages on stale data and reads "healthy" when the truth is "unobserved", which
# is why every one of them ships with a counter arm instead. The reason each is
# here, in the code that publishes it: metrics.rs's COMPACTOR_ALERTED_COUNTERS
# note for the mirror progress gauge, and the emitter comment in
# crates/siglake-compactor/src/lib.rs for the delete-task one.
DASHBOARD_ONLY_GAUGES = (
    MIRROR_PROGRESS_GAUGE,
    "siglake_compactor_delete_tasks_nonterminal",
)


def render(chart: str, extra: list[str]) -> list[dict]:
    out = subprocess.run(
        ["helm", "template", "ci", chart, *extra],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return [d for d in yaml.safe_load_all(out) if d]


def notes_probe_chart(chart: pathlib.Path, destination: pathlib.Path) -> pathlib.Path:
    """A throwaway copy of the chart that renders NOTES.txt as a manifest.

    NOTES.txt cannot be read from a render. `helm template` writes
    `rel.Manifest` plus the hook manifests, and `--show-only` filters that same
    string by the `# Source:` comment each manifest carries — which the notes
    do not have, because they live on the release object. `helm install
    --dry-run` does print them, but it reaches for the cluster on the way (the
    ci-local helm job, 2026-09-14: `Kubernetes cluster unreachable`, at
    `--dry-run=client`), and this check has to run where the render matrix
    runs, with no cluster anywhere.

    So the notes are rendered as a manifest instead: the same file, verbatim,
    wrapped in a `define` and emitted into a ConfigMap by one template this
    adds to the copy. Same engine, same context, same values — the only step it
    does not exercise is helm's own choice to render templates/NOTES.txt as the
    release notes, which is helm's contract rather than this chart's.
    """
    copy = destination / chart.name
    shutil.copytree(chart, copy)
    notes = (copy / "templates/NOTES.txt").read_text()
    # The notes verbatim, as an ordinary named template in the file suffix helm
    # renders no manifest from, and one ConfigMap that emits it. `quote` so the
    # text arrives as a single YAML scalar whatever it contains: the blank lines
    # and the indentation the warnings carry would otherwise have to survive a
    # block scalar. Every piece here — a `define` in a `_*.tpl`, `include`,
    # `quote` — is one the chart itself uses, so the probe cannot fail in a way
    # the render matrix would not have caught anyway.
    (copy / "templates/_notes-probe.tpl").write_text(
        '{{- define "siglake.notesProbe" -}}\n' f"{notes}" "{{- end -}}\n"
    )
    (copy / "templates/zzz-notes-probe.yaml").write_text(
        "apiVersion: v1\n"
        "kind: ConfigMap\n"
        "metadata:\n"
        f"  name: {NOTES_PROBE_NAME}\n"
        "data:\n"
        '  notes: {{ include "siglake.notesProbe" . | quote }}\n'
    )
    return copy


def render_notes(chart: str, extra: list[str]) -> str:
    """The NOTES.txt an install of these values prints, as text.

    `chart` is a notes_probe_chart() copy. Returns "" when the probe rendered
    nothing, which the caller reports as a failure rather than as an install
    with nothing to say.
    """
    for doc in render(chart, extra):
        if (
            doc.get("kind") == "ConfigMap"
            and doc.get("metadata", {}).get("name") == NOTES_PROBE_NAME
        ):
            return str((doc.get("data") or {}).get("notes", ""))
    return ""


def check_notes_open_warning(notes: str, expected: bool) -> list[str]:
    """Hold the install notes' open-API warning to what the tier enforces.

    #4317: the warning names the authentication the operator is missing, so it
    must appear for a query tier with none and stay away from one that verifies
    its callers — by token allow-list or by OIDC, which the query-server selects
    ahead of the allow-list.
    """
    if not notes.strip():
        return ["the render carried no NOTES.txt text at all"]
    warned = OPEN_QUERY_WARNING in notes
    if warned == expected:
        return []
    if expected:
        return [
            "this install authenticates nothing on the query tier, and the notes "
            f"do not say so: no {OPEN_QUERY_WARNING!r} warning in {notes.strip()!r}"
        ]
    line = next(line for line in notes.splitlines() if OPEN_QUERY_WARNING in line)
    return [
        "the notes call the query API open on an install that authenticates its "
        f"callers: {line.strip()!r}"
    ]


def check_notes_auth_predicate(
    notes_path: pathlib.Path = NOTES_TEMPLATE,
    sts_path: pathlib.Path = QUERY_STS_TEMPLATE,
) -> list[str]:
    """The note and the query pods must read ONE authentication predicate.

    The render arms prove what a given install prints. This proves the two
    templates cannot start answering the question separately again — which is
    how an OIDC-only install came to be told its API was open and pointed at
    the token allow-list OIDC takes precedence over (#4317) — and it needs no
    helm, so it runs wherever the source checks do.
    """
    problems = []
    lines = notes_path.read_text().splitlines()
    if QUERY_AUTH_HELPER not in sts_path.read_text():
        problems.append(
            f"{sts_path} no longer decides query authentication through "
            f"`{QUERY_AUTH_HELPER}`, so {notes_path} can describe an install the "
            "pods do not run"
        )
    warnings = [i for i, line in enumerate(lines) if OPEN_QUERY_WARNING in line]
    if not warnings:
        problems.append(
            f"{notes_path} prints no {OPEN_QUERY_WARNING!r} warning: an install "
            "with no query authentication would say nothing about it"
        )
    for index in warnings:
        guard = next(
            (lines[i] for i in range(index, -1, -1) if "{{- if" in lines[i]), None
        )
        if guard is None or QUERY_AUTH_HELPER not in guard:
            problems.append(
                f"{notes_path}:{index + 1} warns that the query API is open under "
                f"`{guard.strip() if guard else 'no condition'}`, not under "
                f"`{QUERY_AUTH_HELPER}`"
            )
    return problems


def check_render_refusal(
    chart: str, extra: list[str], expected: str, allowed: str
) -> str | None:
    """Require Helm to refuse a configuration for the expected reason.

    `allowed` names what a successful render would have left installed, so the
    failure line says what the missing refusal costs rather than only that one
    is missing.
    """
    try:
        render(chart, extra)
    except subprocess.CalledProcessError as error:
        if expected in error.stderr:
            return None
        return (
            "render failed, but not for the expected reason; expected stderr to "
            f"contain {expected!r}, got {error.stderr.strip()!r}"
        )
    return f"render succeeded, {allowed}"


def hpa_metric_shapes(docs: list[dict], component: str) -> list[str]:
    """`type[:metric name]` of each metric on the component's HPA, in order.

    Empty when the component renders no HPA. A `Pods` entry is the shape that
    makes the reading per-pod: the autoscaler averages the metric across the
    running pods and multiplies the target ratio by that count, which is only
    correct for a value each pod owns (#3718).
    """
    shapes: list[str] = []
    for d in docs:
        if d.get("kind") != "HorizontalPodAutoscaler":
            continue
        labels = d["metadata"].get("labels", {}) or {}
        if labels.get("app.kubernetes.io/component") != component:
            continue
        for metric in d["spec"].get("metrics") or []:
            kind = metric.get("type")
            if kind == "Resource":
                shapes.append(f"Resource:{metric['resource']['name']}")
            elif kind == "Pods":
                shapes.append(f"Pods:{metric['pods']['metric']['name']}")
            else:
                shapes.append(str(kind))
    return shapes


def check_hpa_metrics(docs: list[dict], expected: dict[str, list[str]]) -> list[str]:
    """Hold each component's rendered HPA metrics to the expected shapes."""
    problems = []
    for component, want in sorted(expected.items()):
        got = hpa_metric_shapes(docs, component)
        if got != want:
            problems.append(
                f"the {component} HPA renders metrics {got}, expected {want}"
            )
    return problems


def container_env_names(pod_spec: dict) -> set[str]:
    """Environment-variable names across the pod's regular containers."""
    names: set[str] = set()
    for container in pod_spec.get("containers") or []:
        for env in container.get("env") or []:
            name = env.get("name")
            if isinstance(name, str):
                names.add(name)
    return names


class EnvCatalogError(Exception):
    """The source catalog or a non-Helm deployment surface was unreadable."""


def rust_code_mask(source: str) -> str:
    """Rust source with comments and literals blanked, preserving positions."""
    out = list(source)

    def blank(start: int, end: int) -> None:
        for pos in range(start, end):
            if out[pos] != "\n":
                out[pos] = " "

    i = 0
    while i < len(source):
        if source.startswith("//", i):
            end = source.find("\n", i + 2)
            end = len(source) if end < 0 else end
            blank(i, end)
            i = end
            continue
        if source.startswith("/*", i):
            depth = 1
            end = i + 2
            while end < len(source) and depth:
                if source.startswith("/*", end):
                    depth += 1
                    end += 2
                elif source.startswith("*/", end):
                    depth -= 1
                    end += 2
                else:
                    end += 1
            blank(i, end)
            i = end
            continue

        # Raw string and raw byte-string literals. Their hashes make ordinary
        # quote scanning insufficient, and their contents may contain braces.
        raw = None
        if source[i] == "r":
            raw = i + 1
        elif source.startswith("br", i):
            raw = i + 2
        if raw is not None:
            quote = raw
            while quote < len(source) and source[quote] == "#":
                quote += 1
            if quote < len(source) and source[quote] == '"':
                close = '"' + source[raw:quote]
                end = source.find(close, quote + 1)
                end = len(source) if end < 0 else end + len(close)
                blank(i, end)
                i = end
                continue

        if source[i] == '"':
            end = i + 1
            while end < len(source):
                if source[end] == "\\":
                    end += 2
                elif source[end] == '"':
                    end += 1
                    break
                else:
                    end += 1
            blank(i, min(end, len(source)))
            i = end
            continue

        # A lifetime starts with the same character as a char literal. Mask
        # only the compact forms that can contain a delimiter relevant below.
        if source[i] == "'":
            if i + 2 < len(source) and source[i + 2] == "'":
                blank(i, i + 3)
                i += 3
                continue
            if i + 2 < len(source) and source[i + 1] == "\\":
                end = source.find("'", i + 3)
                line_end = source.find("\n", i + 1)
                if end >= 0 and (line_end < 0 or end < line_end):
                    blank(i, end + 1)
                    i = end + 1
                    continue
        i += 1
    return "".join(out)


def non_test_rust_source(source: str) -> str:
    """Blank cfg(test) items while preserving production source and line numbers."""
    mask = rust_code_mask(source)
    out = list(source)
    search_from = 0
    while match := CFG_TEST.search(mask, search_from):
        item_start = match.end()
        first_brace = mask.find("{", item_start)
        first_semi = mask.find(";", item_start)
        delimiters = [p for p in (first_brace, first_semi) if p >= 0]
        if not delimiters:
            item_end = len(source)
        else:
            first_delimiter = min(delimiters)
            prefix_words = re.findall(r"[A-Za-z_]+", mask[item_start:first_delimiter])
            semicolon_item = (
                any(word in {"use", "static", "type"} for word in prefix_words)
                or ("const" in prefix_words and "fn" not in prefix_words)
            )
            parens = brackets = braces = 0
            item_end = len(source)
            for pos in range(item_start, len(mask)):
                char = mask[pos]
                if char == "(":
                    parens += 1
                elif char == ")":
                    parens -= 1
                elif char == "[":
                    brackets += 1
                elif char == "]":
                    brackets -= 1
                elif char == "{":
                    braces += 1
                elif char == "}":
                    braces -= 1
                    if not semicolon_item and parens == brackets == braces == 0:
                        item_end = pos + 1
                        break
                elif char == ";" and parens == brackets == braces == 0:
                    item_end = pos + 1
                    break
        for pos in range(match.start(), item_end):
            if out[pos] != "\n":
                out[pos] = " "
        search_from = item_end
    return "".join(out)


def env_name_catalog() -> frozenset[str]:
    """Every SIGLAKE_* string literal in crates/**/src and third_party/**/src."""
    names: set[str] = set()
    source_count = 0
    for package_root in RUST_PACKAGE_ROOTS:
        if not package_root.is_dir():
            continue
        for src_dir in package_root.glob("**/src"):
            for path in src_dir.rglob("*.rs"):
                if "tests" in path.relative_to(src_dir).parts:
                    continue
                source_count += 1
                try:
                    source = path.read_text()
                except OSError as e:
                    raise EnvCatalogError(
                        f"cannot read {path}: {e.strerror}"
                    ) from e
                names.update(ENV_NAME_LITERAL.findall(non_test_rust_source(source)))
    if source_count == 0:
        raise EnvCatalogError(
            "found no Rust source under crates/**/src or third_party/**/src"
        )
    return frozenset(names)


def compose_env_locations(path: pathlib.Path = COMPOSE_FILE) -> list[tuple[str, str]]:
    """(file/service, name) for SIGLAKE_* env set by docker-compose."""
    try:
        doc = yaml.safe_load(path.read_text())
    except OSError as e:
        raise EnvCatalogError(f"cannot read {path}: {e.strerror}") from e
    except yaml.YAMLError as e:
        raise EnvCatalogError(f"cannot parse {path}: {e}") from e
    services = (doc or {}).get("services")
    if not isinstance(services, dict):
        raise EnvCatalogError(f"{path} has no services mapping")
    found: list[tuple[str, str]] = []
    for service_name, service in services.items():
        environment = (service or {}).get("environment")
        if environment is None:
            continue
        if isinstance(environment, dict):
            names = environment
        elif isinstance(environment, list):
            names = (
                entry.split("=", 1)[0]
                for entry in environment
                if isinstance(entry, str)
            )
        else:
            raise EnvCatalogError(
                f"{path} service/{service_name} environment is neither a mapping nor a list"
            )
        found.extend(
            (f"{path} service/{service_name}", name)
            for name in names
            if isinstance(name, str) and ENV_NAME.fullmatch(name)
        )
    return found


def compose_query_file_cache_problems(
    path: pathlib.Path = COMPOSE_FILE,
) -> list[str]:
    """The standalone compose query server carries the packaged cache default."""
    try:
        doc = yaml.safe_load(path.read_text())
    except OSError as e:
        raise EnvCatalogError(f"cannot read {path}: {e.strerror}") from e
    except yaml.YAMLError as e:
        raise EnvCatalogError(f"cannot parse {path}: {e}") from e
    environment = (
        (((doc or {}).get("services") or {}).get(QUERY_CONTAINER) or {}).get(
            "environment"
        )
        or {}
    )
    if not isinstance(environment, dict):
        return [
            f"{path} service/{QUERY_CONTAINER} environment must be a mapping "
            "to assert the source-file cache default"
        ]
    return [
        f"{path} service/{QUERY_CONTAINER} sets {name}={environment.get(name)!r}; "
        f"expected packaged default {want!r}"
        for name, want in DEFAULT_FILE_CACHE_ENV.items()
        if str(environment.get(name)) != want
    ]


def rust_env_locations(path: pathlib.Path) -> list[tuple[str, str]]:
    """(file:line, name) for production SIGLAKE_* literals in one Rust file."""
    try:
        source = non_test_rust_source(path.read_text())
    except OSError as e:
        raise EnvCatalogError(f"cannot read {path}: {e.strerror}") from e
    return [
        (f"{path}:{source.count(chr(10), 0, match.start()) + 1}", match.group(1))
        for match in ENV_NAME_LITERAL.finditer(source)
    ]


def unknown_env_problems(
    locations: list[tuple[str, str]], catalog: frozenset[str]
) -> list[str]:
    """One file-and-name diagnostic for each deployment name code does not know."""
    return [
        f"{where} sets '{name}', which no non-test Rust source under "
        f"crates/**/src or third_party/**/src names"
        for where, name in locations
        if name not in catalog
    ]


def rendered_env_problems(
    docs: list[dict], catalog: frozenset[str]
) -> list[str]:
    """Every SIGLAKE_* env name in every rendered regular container is known."""
    locations: list[tuple[str, str]] = []
    for doc in docs:
        pod_spec = ((doc.get("spec") or {}).get("template") or {}).get("spec")
        if not isinstance(pod_spec, dict):
            continue
        where = (
            f"{CHART_DIR} rendered {doc.get('kind', '<unknown>')}/"
            f"{(doc.get('metadata') or {}).get('name', '<unnamed>')}"
        )
        locations.extend(
            (where, name)
            for name in container_env_names(pod_spec)
            if name.startswith("SIGLAKE_")
        )
    return unknown_env_problems(locations, catalog)


def check_hook_credentials(docs: list[dict]) -> list[str]:
    """Helm hook Jobs must receive every object-store credential workloads use."""
    workload_credentials: set[str] = set()
    for doc in docs:
        if doc.get("kind") not in ("Deployment", "StatefulSet"):
            continue
        pod_spec = doc["spec"]["template"]["spec"]
        workload_credentials.update(
            name
            for name in container_env_names(pod_spec)
            if HOOK_CREDENTIAL_ENV.fullmatch(name)
        )

    problems: list[str] = []
    for doc in docs:
        if doc.get("kind") != "Job":
            continue
        annotations = doc.get("metadata", {}).get("annotations") or {}
        if "helm.sh/hook" not in annotations:
            continue
        hook_env = container_env_names(doc["spec"]["template"]["spec"])
        missing = sorted(workload_credentials - hook_env)
        if missing:
            problems.append(
                f"Job/{doc['metadata']['name']} is a Helm hook but is missing "
                f"credential env used by rendered workloads: {missing}"
            )
    return problems


def check(
    docs: list[dict],
    exported: "Exported | None" = None,
    expected_query_env: dict[str, str] | None = None,
    expected_query_spill: dict[str, str] | None = None,
    env_catalog: frozenset[str] | None = None,
    expected_mirror_stall: int = DEFAULT_MIRROR_STALL_SECS,
    expected_wal_mirror_prefix: str | None = None,
    expected_otlp_grpc_port: int | None = 4317,
    persistent_jobs: bool = True,
    expected_index_rebuild: str = "0",
    expected_query_tokens: tuple[str, str] | None = None,
) -> list[str]:
    problems: list[str] = []
    if exported is None:
        exported = Exported.load()
    if env_catalog is None:
        env_catalog = env_name_catalog()

    workloads = {
        (d["kind"], d["metadata"]["name"]): d
        for d in docs
        if d.get("kind") in ("Deployment", "StatefulSet", "DaemonSet")
    }
    problems.extend(rendered_env_problems(docs, env_catalog))
    problems.extend(check_hook_credentials(docs))
    # Container port names, per workload, plus a flat set for Service checks.
    ports_by_workload: dict[tuple[str, str], set[str]] = {}
    for key, w in workloads.items():
        names = set()
        for c in w["spec"]["template"]["spec"].get("containers", []):
            for p in c.get("ports", []) or []:
                if p.get("name"):
                    names.add(p["name"])
        ports_by_workload[key] = names
    all_port_names = set().union(*ports_by_workload.values()) if ports_by_workload else set()

    for d in docs:
        if d.get("kind") == "Service":
            for p in d["spec"].get("ports", []) or []:
                tp = p.get("targetPort")
                if isinstance(tp, str) and tp not in all_port_names:
                    problems.append(
                        f"Service/{d['metadata']['name']} port {p.get('name')} targets "
                        f"'{tp}', which no container declares "
                        f"(declared: {sorted(all_port_names)})"
                    )
        if d.get("kind") == "HorizontalPodAutoscaler":
            ref = d["spec"]["scaleTargetRef"]
            key = (ref["kind"], ref["name"])
            if key not in workloads:
                problems.append(
                    f"HPA/{d['metadata']['name']} targets {ref['kind']}/{ref['name']}, "
                    f"which the chart does not render "
                    f"(rendered: {sorted(f'{k}/{n}' for k, n in workloads)})"
                )

    # Two Services carrying the same component label and both exposing a port
    # named `metrics` make Prometheus build one target per (Endpoints, pod)
    # pair: every metric on that tier reads DOUBLE. That is not a cosmetic
    # error — `siglake_query_in_flight` drives both the KEDA query trigger and
    # the operator's autoscaler, so the effective per-pod target silently
    # became half the configured one, on a signal nobody would think to
    # distrust. Exactly one Service per tier may be the scrape target, and the
    # ServiceMonitor selects it by an explicit label.
    scrape_targets: dict[str, list[str]] = {}
    for d in docs:
        if d.get("kind") != "Service":
            continue
        labels = d["metadata"].get("labels", {}) or {}
        component = labels.get("app.kubernetes.io/component")
        if component is None:
            continue
        has_metrics = any(
            (p.get("name") == "metrics") for p in (d["spec"].get("ports") or [])
        )
        if has_metrics:
            scrape_targets.setdefault(component, []).append(d["metadata"]["name"])
    for component, names in sorted(scrape_targets.items()):
        if len(names) > 1:
            problems.append(
                f"component '{component}' has {len(names)} Services exposing a port named "
                f"'metrics' ({sorted(names)}); the ServiceMonitor matches on component "
                f"labels, so every metric on this tier would be scraped once per Service"
            )

    # And the ServiceMonitor must actually discriminate, or the check above is
    # the only thing standing between a new Service and doubled metrics.
    for d in docs:
        if d.get("kind") != "ServiceMonitor":
            continue
        match = (d["spec"].get("selector") or {}).get("matchLabels") or {}
        if "siglake.limnion.ai/scrape" not in match:
            problems.append(
                f"ServiceMonitor/{d['metadata']['name']} selects on component labels alone; "
                f"it must also require siglake.limnion.ai/scrape so a second Service on the "
                f"same tier cannot double every metric"
            )

    # An alert on a metric nothing emits is indistinguishable from a healthy
    # system — it simply never fires. The repo has hit this class before: a
    # diagnostic report listed `siglake_side_agg_cache_total`, which nothing
    # exports, and duly reported "never incremented" for a family that was live
    # under a different name. So every `siglake_*` series named in a rule
    # expression must be emitted somewhere in the source, in the form it is
    # emitted. The KEDA triggers are held to the same rule, and for them the
    # failure is worse than silence: the prometheus scaler reads an empty
    # result as 0 (ignoreNullValues defaults to true), so a trigger on a
    # series that does not exist pins the tier at minReplicaCount under any
    # load — which is what the p95 queue-wait trigger did while
    # `siglake_query_exec_pool_queue_seconds` was still a summary (#148).
    if exported:
        for d in docs:
            for where, expr in promql_exprs(d):
                for problem in metric_problems(expr, exported):
                    problems.append(f"{where} {problem}")

    problems.extend(check_warm_interval(docs))
    problems.extend(check_dashboard_only_gauges(docs))
    problems.extend(check_mirror_rotation_window(docs, expected_mirror_stall))
    if expected_query_env is not None:
        problems.extend(check_query_env(docs, expected_query_env))
    problems.extend(
        check_query_spill(docs, expected_query_spill or DEFAULT_QUERY_SPILL)
    )
    problems.extend(check_query_peer_discovery(docs))
    problems.extend(check_query_token_source(docs, expected_query_tokens))
    problems.extend(
        check_wal_mirror_env(
            docs,
            DEFAULT_WAL_MIRROR_PREFIX
            if expected_wal_mirror_prefix is None
            else expected_wal_mirror_prefix,
        )
    )
    problems.extend(check_otlp_grpc(docs, expected_otlp_grpc_port))
    problems.extend(check_jobs_store(docs, persistent_jobs))
    problems.extend(check_compactor_index_rebuild(docs, expected_index_rebuild))

    return problems


def check_compactor_index_rebuild(docs: list[dict], expected: str) -> list[str]:
    """The chart states both arms of the binary's default-off rebuild switch."""
    rendered = False
    values: list[str] = []
    for doc in docs:
        if doc.get("kind") != "Deployment":
            continue
        for container in doc["spec"]["template"]["spec"].get("containers", []):
            if container.get("name") != COMPACTOR_CONTAINER:
                continue
            rendered = True
            values = [
                str(entry.get("value"))
                for entry in container.get("env") or []
                if entry.get("name") == INDEX_REBUILD_ENV
            ]
    if not rendered:
        return []
    if not values:
        return [
            f"Deployment container '{COMPACTOR_CONTAINER}' renders no "
            f"{INDEX_REBUILD_ENV}; true must be an explicit binary opt-in"
        ]
    if values[-1] != expected:
        return [
            f"Deployment container '{COMPACTOR_CONTAINER}' renders "
            f"{INDEX_REBUILD_ENV}={values[-1]!r}; expected {expected!r}"
        ]
    return []


def promql_exprs(doc: dict) -> list[tuple[str, str]]:
    """Every (location, PromQL) pair a rendered object hands to Prometheus.

    PrometheusRule alert (or recording) rule exprs, and the `query` of every
    prometheus-type trigger on a KEDA ScaledObject.
    """
    out: list[tuple[str, str]] = []
    kind = doc.get("kind")
    if kind == "PrometheusRule":
        for group in doc["spec"].get("groups") or []:
            for rule in group.get("rules") or []:
                label = rule.get("alert") or rule.get("record")
                out.append((f"PrometheusRule alert '{label}'", str(rule.get("expr", ""))))
    elif kind == "ScaledObject":
        for i, trigger in enumerate(doc["spec"].get("triggers") or []):
            if trigger.get("type") != "prometheus":
                continue
            query = (trigger.get("metadata") or {}).get("query")
            if isinstance(query, str):
                out.append((f"ScaledObject/{doc['metadata']['name']} trigger {i}", query))
    return out


def check_dashboard_only_gauges(docs: list[dict]) -> list[str]:
    """No rendered PromQL that DECIDES anything may read a sweep-published
    gauge.

    `promql_exprs` covers PrometheusRule alert and recording rules and KEDA
    prometheus triggers — every rendered expression whose result changes
    behaviour rather than a chart. Each name in DASHBOARD_ONLY_GAUGES is
    retained at its last value by a live pod once the sweep publishing it stops
    running, so any of them reads stale-healthy exactly when the truth is
    "unobserved". The counter arm beside each one is the signal to read
    instead.
    """
    problems: list[str] = []
    for doc in docs:
        for where, expr in promql_exprs(doc):
            for gauge in DASHBOARD_ONLY_GAUGES:
                if gauge not in expr:
                    continue
                # check_mirror_rotation_window already reports this one pair,
                # with the reasoning specific to that rule's activity arm.
                if gauge == MIRROR_PROGRESS_GAUGE and MIRROR_ALERT in where:
                    continue
                problems.append(
                    f"{where} reads '{gauge}', a gauge published only by a maintenance "
                    f"sweep: a live pod retains its last value when that sweep stops "
                    f"running and it does not exist before the first pass, so this "
                    f"expression is true or false on stale data. Read the counter arm "
                    f"beside it with increase() instead, and keep the gauge on the "
                    f"dashboard"
                )
    return problems


def check_prometheus_rules(
    docs: list[dict], require_promtool: bool = False
) -> tuple[int, list[str], bool]:
    """Validate and unit-test rendered PrometheusRules with Prometheus's tools.

    `promtool check rules` reads Prometheus rule files, not PrometheusRule
    custom resources, so write only each object's `spec.groups`. The temporary
    file also avoids depending on promtool's stdin conventions. The unit-test
    fixture names that temporary rule file, so its assertions exercise the
    rendered chart rule rather than a duplicated expression.
    """
    checked = 0
    problems: list[str] = []
    for doc in docs:
        if doc.get("kind") != "PrometheusRule":
            continue
        name = doc.get("metadata", {}).get("name", "<unnamed>")
        rule_file = {"groups": doc.get("spec", {}).get("groups", [])}
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = pathlib.Path(tmp_dir)
            rule_path = tmp_path / "prometheusrule.rules.yaml"
            test_path = tmp_path / PROMETHEUS_RULE_TESTS.name
            rule_path.write_text(yaml.safe_dump(rule_file, sort_keys=False))
            try:
                test_path.write_text(PROMETHEUS_RULE_TESTS.read_text())
            except OSError as e:
                problems.append(
                    f"cannot read {PROMETHEUS_RULE_TESTS}: {e.strerror}"
                )
                continue
            try:
                subprocess.run(
                    ["promtool", "check", "rules", rule_path.name],
                    capture_output=True,
                    text=True,
                    check=True,
                    cwd=tmp_path,
                )
            except FileNotFoundError:
                problem = (
                    "promtool not installed; rendered PrometheusRule expressions "
                    "were not parsed"
                )
                return checked, [problem] if require_promtool else [], True
            except subprocess.CalledProcessError as e:
                output = "\n".join(s for s in (e.stdout.strip(), e.stderr.strip()) if s)
                problems.append(
                    f"PrometheusRule/{name} failed `promtool check rules`:"
                    + (f"\n{output}" if output else " no diagnostic output")
                )
            else:
                try:
                    subprocess.run(
                        ["promtool", "test", "rules", test_path.name],
                        capture_output=True,
                        text=True,
                        check=True,
                        cwd=tmp_path,
                    )
                except subprocess.CalledProcessError as e:
                    output = "\n".join(
                        s for s in (e.stdout.strip(), e.stderr.strip()) if s
                    )
                    problems.append(
                        f"PrometheusRule/{name} failed `promtool test rules`:"
                        + (f"\n{output}" if output else " no diagnostic output")
                    )
                else:
                    checked += 1
    return checked, problems, False


def query_warm_interval(docs: list[dict]) -> tuple[bool, str | None]:
    """(query StatefulSet rendered?, its SIGLAKE_QUERY_WARM_INTERVAL_SECS).

    The LAST entry by that name wins, as it does on the kubelet, so a
    `query.extraEnv` override is read the way the pod would read it.
    """
    rendered = False
    value: str | None = None
    for d in docs:
        if d.get("kind") != "StatefulSet":
            continue
        for c in d["spec"]["template"]["spec"].get("containers", []):
            if c.get("name") != QUERY_CONTAINER:
                continue
            rendered = True
            for env in c.get("env") or []:
                if env.get("name") == WARM_ENV:
                    value = str(env.get("value"))
    return rendered, value


def check_query_env(docs: list[dict], expected: dict[str, str]) -> list[str]:
    """The query container must receive each expected value as a string."""
    rendered = False
    values: dict[str, str] = {}
    for d in docs:
        if d.get("kind") != "StatefulSet":
            continue
        for c in d["spec"]["template"]["spec"].get("containers", []):
            if c.get("name") != QUERY_CONTAINER:
                continue
            rendered = True
            for env in c.get("env") or []:
                name = env.get("name")
                if name in expected:
                    values[name] = str(env.get("value"))

    if not rendered:
        return [f"StatefulSet container '{QUERY_CONTAINER}' was not rendered"]

    problems: list[str] = []
    for name, want in expected.items():
        if name not in values:
            problems.append(
                f"StatefulSet container '{QUERY_CONTAINER}' renders no {name}; "
                f"expected value {want!r}"
            )
        elif values[name] != want:
            problems.append(
                f"StatefulSet container '{QUERY_CONTAINER}' renders {name}="
                f"{values[name]!r}; expected {want!r}"
            )
    return problems


def check_wal_mirror_env(docs: list[dict], expected_prefix: str) -> list[str]:
    """The ingester states the mirror prefix, and the drain claims from it.

    Two failures this catches, both silent:

    * The ingester renders no ``SIGLAKE_WAL_MIRROR_PREFIX``. The binary
      defaults the mirror ON wherever a warehouse URL is set, so an omitted
      variable is not "off" — ``wal.mirror.enabled: false`` would upload
      anyway, and the operator who set it would never know.
    * The compactor's ``--mirror-prefix`` names a different string. In
      catalog-claim mode the drain reads that prefix and local ``sealed/``
      never, so a mismatch stops compaction with no error.

    The claim-with-no-mirror arm below is now a backstop: since #2955 the
    chart refuses that pair at render time (``siglake.compactorScaleGuard``),
    so no matrix scenario can reach it. It stays because it is the check that
    would catch a guard that stopped firing.
    """
    rendered = False
    values: list[str] = []
    claim_prefixes: list[str] = []
    for d in docs:
        if d.get("kind") != "Deployment":
            continue
        for c in d["spec"]["template"]["spec"].get("containers", []):
            if c.get("name") == INGEST_CONTAINER:
                rendered = True
                # Kubernetes resolves a duplicate name to the last entry, and
                # `ingester.extraEnv` is rendered last on purpose.
                values = [
                    str(e.get("value"))
                    for e in c.get("env") or []
                    if e.get("name") == WAL_MIRROR_ENV
                ]
            if c.get("name") == COMPACTOR_CONTAINER:
                args = c.get("args") or []
                claim_prefixes = [
                    args[i + 1]
                    for i, a in enumerate(args)
                    if a == "--mirror-prefix" and i + 1 < len(args)
                ]

    if not rendered:
        return [f"Deployment container '{INGEST_CONTAINER}' was not rendered"]

    problems: list[str] = []
    if not values:
        problems.append(
            f"Deployment container '{INGEST_CONTAINER}' renders no {WAL_MIRROR_ENV}; "
            f"the binary mirrors by default, so the chart has to state "
            f"{expected_prefix!r} — an empty value is the opt-out"
        )
    elif values[-1] != expected_prefix:
        problems.append(
            f"Deployment container '{INGEST_CONTAINER}' renders {WAL_MIRROR_ENV}="
            f"{values[-1]!r}; expected {expected_prefix!r}"
        )
    for got in claim_prefixes:
        if not expected_prefix:
            problems.append(
                f"Deployment container '{COMPACTOR_CONTAINER}' claims from "
                f"--mirror-prefix {got!r} while wal.mirror.enabled is off: the "
                f"drain reads the mirror and never local sealed/, so nothing "
                f"would ever be compacted"
            )
        elif got != expected_prefix:
            problems.append(
                f"Deployment container '{COMPACTOR_CONTAINER}' claims from "
                f"--mirror-prefix {got!r} but the ingester writes to "
                f"{expected_prefix!r}"
            )
    return problems


def check_otlp_grpc(docs: list[dict], expected_port: int | None) -> list[str]:
    """Hold the chart's listener argument, container port and Service together."""
    args: list[str] | None = None
    container_ports: dict[str, int] = {}
    service_ports: dict[str, int] = {}
    for d in docs:
        if d.get("kind") == "Deployment":
            for c in d["spec"]["template"]["spec"].get("containers", []):
                if c.get("name") != INGEST_CONTAINER:
                    continue
                args = [str(a) for a in c.get("args") or []]
                container_ports = {
                    str(p.get("name")): int(p.get("containerPort"))
                    for p in c.get("ports") or []
                    if p.get("name") and p.get("containerPort") is not None
                }
        if d.get("kind") == "Service" and (
            d.get("metadata", {}).get("labels") or {}
        ).get("app.kubernetes.io/component") == "ingester":
            service_ports = {
                str(p.get("name")): int(p.get("port"))
                for p in d.get("spec", {}).get("ports") or []
                if p.get("name") and p.get("port") is not None
            }

    if args is None:
        return [f"Deployment container '{INGEST_CONTAINER}' was not rendered"]

    problems: list[str] = []
    listener_values = [
        args[i + 1]
        for i, arg in enumerate(args)
        if arg == "--otlp-grpc-listen" and i + 1 < len(args)
    ]
    if expected_port is None:
        if "--disable-otlp-grpc" not in args:
            problems.append(
                "ingester.otlpGrpc.enabled=false omits --disable-otlp-grpc; "
                "the default-on binary would still bind 4317"
            )
        if listener_values:
            problems.append(
                f"disabled OTLP/gRPC render still sets listener {listener_values!r}"
            )
        if "otlp-grpc" in container_ports or "otlp-grpc" in service_ports:
            problems.append("disabled OTLP/gRPC render still exposes an otlp-grpc port")
        return problems

    expected_addr = f"0.0.0.0:{expected_port}"
    if listener_values != [expected_addr]:
        problems.append(
            f"enabled OTLP/gRPC listener is {listener_values!r}; expected [{expected_addr!r}]"
        )
    if "--disable-otlp-grpc" in args:
        problems.append("enabled OTLP/gRPC render also passes --disable-otlp-grpc")
    if container_ports.get("otlp-grpc") != expected_port:
        problems.append(
            f"ingester container otlp-grpc port is {container_ports.get('otlp-grpc')!r}; "
            f"expected {expected_port}"
        )
    if service_ports.get("otlp-grpc") != expected_port:
        problems.append(
            f"ingester Service otlp-grpc port is {service_ports.get('otlp-grpc')!r}; "
            f"expected {expected_port}"
        )
    return problems


def check_jobs_store(docs: list[dict], persistent: bool) -> list[str]:
    """The batch-job store is shared by the query replicas, or the tier is one pod.

    `query.jobs.persistent` is ON by default since 2026-09-11. The value has to
    be the catalog URI expanded from an env entry rendered EARLIER in the same
    list, since that is the only thing Kubernetes substitutes `$(VAR)` from; a
    reordering leaves the literal string `$(SIGLAKE_CATALOG_URI)` as the
    connection string and crash-loops the tier. With the opt-out the variable
    must be absent rather than blank-but-present, and the replica count is then
    the whole correctness argument: two pods with two in-memory stores answer
    404 for each other's jobs.
    """
    env: list[dict] = []
    replicas: int | None = None
    for d in docs:
        if d.get("kind") != "StatefulSet":
            continue
        for c in d["spec"]["template"]["spec"].get("containers", []):
            if c.get("name") != QUERY_CONTAINER:
                continue
            env = list(c.get("env") or [])
            replicas = d["spec"].get("replicas")

    if not env:
        return [f"StatefulSet container '{QUERY_CONTAINER}' was not rendered"]

    names = [e.get("name") for e in env]
    problems: list[str] = []
    if JOBS_STORE_ENV not in names:
        if persistent:
            problems.append(
                f"query.jobs.persistent is on but the container renders no "
                f"{JOBS_STORE_ENV}; batch jobs would be per-pod and in-memory"
            )
        return problems

    if not persistent:
        return [
            f"query.jobs.persistent=false still renders {JOBS_STORE_ENV}; the "
            f"opt-out is an absent variable, not a blank one"
        ]

    index = names.index(JOBS_STORE_ENV)
    value = str(env[index].get("value"))
    if value != f"$({CATALOG_URI_ENV})":
        problems.append(
            f"{JOBS_STORE_ENV} is {value!r}; the store is the catalog database, so it "
            f"must be $({CATALOG_URI_ENV})"
        )
    elif CATALOG_URI_ENV not in names[:index]:
        problems.append(
            f"{JOBS_STORE_ENV} expands $({CATALOG_URI_ENV}), which is rendered at "
            f"position {names.index(CATALOG_URI_ENV) if CATALOG_URI_ENV in names else None} "
            f"and must come before position {index}; Kubernetes substitutes only from "
            f"EARLIER entries and passes the rest through literally"
        )
    if replicas is not None and int(replicas) < 1:
        problems.append(f"query StatefulSet renders {replicas} replicas")
    return problems


def check_query_token_source(docs: list[dict], expected: tuple[str, str] | None) -> list[str]:
    """#4273: the query pods read the token Secret the values asked for.

    `expected` is the `(Secret name, key)` the query container must source
    ``SIGLAKE_QUERY_TOKENS`` from — with ``CHART_QUERY_TOKENS_SECRET`` standing
    for the chart's own ``<fullname>-query-tokens`` — or None for a deliberately
    open install, where no such variable may be rendered at all.

    Two failures, in both directions, and neither shows up in a render anyone
    reads by eye:

    * a Secret the chart materializes that nothing consumes. The ESO arm did
      exactly this: `externalSecrets.queryTokens.remoteKey` created an
      ExternalSecret while the StatefulSet resolved the token Secret from
      `tokens.existingSecret`/`tokens.list` only, so a cluster whose tokens came
      from a secret backend served `/api/v1/sql` open with one warning line
      (`AuthConfig::open()`, crates/siglake-query-server/src/main.rs).
    * the inverse: a `secretKeyRef` naming the chart's own token Secret when no
      arm renders it, which holds the pods in `CreateContainerConfigError`.

    A Secret the customer owns (`tokens.existingSecret`) is outside the render,
    so only the name and key are held to the values.
    """
    query: dict | None = None
    fullname: str | None = None
    for doc in docs:
        if doc.get("kind") != "StatefulSet":
            continue
        for container in doc["spec"]["template"]["spec"].get("containers", []):
            if container.get("name") == QUERY_CONTAINER:
                query = container
                fullname = re.sub(r"-query$", "", doc["metadata"]["name"])
    if query is None or fullname is None:
        return [] if expected is None else [
            f"StatefulSet container '{QUERY_CONTAINER}' was not rendered, so no "
            f"{QUERY_TOKENS_ENV} could come from {expected[0]!r}"
        ]

    chart_secret = CHART_QUERY_TOKENS_SECRET.replace("<chart>", fullname)
    source: tuple[str, str] | None = None
    for env in query.get("env") or []:
        if env.get("name") != QUERY_TOKENS_ENV:
            continue
        ref = (env.get("valueFrom") or {}).get("secretKeyRef") or {}
        source = (str(ref.get("name")), str(ref.get("key")))

    # Who renders the chart-owned Secret on this arm: the inline `tokens.list`
    # Secret, the ESO target, or neither.
    creators = [
        f"{doc['kind']}/{doc['metadata']['name']}"
        for doc in docs
        if (doc.get("kind") == "Secret" and doc["metadata"]["name"] == chart_secret)
        or (
            doc.get("kind") == "ExternalSecret"
            and ((doc["spec"].get("target") or {}).get("name") == chart_secret)
        )
    ]

    problems: list[str] = []
    if expected is None:
        if source is not None:
            problems.append(
                f"query container renders {QUERY_TOKENS_ENV} from {source[0]!r}; this "
                f"arm sets no token source and must run open"
            )
    else:
        want = (expected[0].replace("<chart>", fullname), expected[1])
        if source is None:
            problems.append(
                f"query container renders no {QUERY_TOKENS_ENV}; expected it from "
                f"Secret {want[0]!r} key {want[1]!r}, so this install answers "
                f"/api/v1/sql open"
            )
        elif source != want:
            problems.append(
                f"query container sources {QUERY_TOKENS_ENV} from {source}; expected "
                f"{want}"
            )

    if creators and (source is None or source[0] != chart_secret):
        read = (
            f"{QUERY_TOKENS_ENV} comes from {source[0]!r} instead"
            if source
            else f"{QUERY_TOKENS_ENV} is not rendered at all, so the tier runs open"
        )
        problems.append(
            f"{', '.join(creators)} materializes the query token Secret "
            f"{chart_secret!r}, which the query container does not read ({read})"
        )
    if not creators and source is not None and source[0] == chart_secret:
        problems.append(
            f"query container sources {QUERY_TOKENS_ENV} from {chart_secret!r}, which "
            f"no rendered object creates; the pods would not start"
        )
    return problems


def check_oidc_env(docs: list[dict], expected: dict[str, dict[str, str]]) -> list[str]:
    """#4303: each tier renders exactly the OIDC variables its values asked for.

    `expected` maps a tier (``ingester``/``query``) to the ``SIGLAKE_OIDC_*``
    names and values its container must carry; a tier left out of it must carry
    none. Both directions matter, and neither is visible in a render read by
    eye:

    * a requested variable that is not rendered. The templates emitted the
      whole block only under `if and .issuer .audience`, so an incomplete block
      dropped out of the render and the tier came up on its remaining
      authentication — or open — with nothing saying so. That path is now a
      render refusal (`siglake.oidcGuard`), and these arms are the other half:
      a complete block must still reach the pods.
    * a variable rendered where the values set none, which would turn a
      deliberately token-authenticated or open tier into one that rejects every
      request its provider does not sign.
    """
    containers = {
        "ingester": (INGEST_CONTAINER, "Deployment"),
        "query": (QUERY_CONTAINER, "StatefulSet"),
    }
    problems: list[str] = []
    for tier, (container_name, kind) in containers.items():
        want = expected.get(tier, {})
        container: dict | None = None
        for doc in docs:
            if doc.get("kind") != kind:
                continue
            for candidate in doc["spec"]["template"]["spec"].get("containers", []):
                if candidate.get("name") == container_name:
                    container = candidate
        if container is None:
            if want:
                problems.append(
                    f"container '{container_name}' was not rendered, so none of "
                    f"{sorted(want)} reached the {tier} tier"
                )
            continue
        got = {
            env["name"]: env.get("value")
            for env in container.get("env") or []
            if env.get("name", "").startswith("SIGLAKE_OIDC_")
        }
        if got != want:
            problems.append(
                f"{tier} container renders OIDC variables {got}; expected {want}"
            )
    return problems


def check_query_peer_discovery(docs: list[dict]) -> list[str]:
    """#967: hold the four halves of runtime peer discovery together.

    The chart's KEDA ceiling was removed on the strength of discovery being
    rendered, so each of these silently un-does that removal on its own:

    * a rendered static ``--query-peers`` list caps fan-out at the count that
      was rendered, which is exactly what the refusal existed to prevent;
    * a missing ``--query-peer-discovery-srv`` leaves every pod single-pod;
    * an SRV name that does not match the headless Service resolves to
      nothing, so the tier never fans out at all; and
    * a headless Service with ``publishNotReadyAddresses`` puts pods that
      cannot serve a shard into the membership.
    """
    problems: list[str] = []
    query: dict | None = None
    for doc in docs:
        if doc.get("kind") != "StatefulSet":
            continue
        for container in doc["spec"]["template"]["spec"].get("containers", []):
            if container.get("name") == QUERY_CONTAINER:
                query = container
    if query is None:
        return []

    args = [str(a) for a in query.get("args") or []]
    if "--query-peers" in args:
        problems.append(
            "query StatefulSet still renders a static --query-peers list; "
            "fan-out would be capped at the rendered replica count, which is "
            "what removing the KEDA ceiling assumed was no longer true"
        )
    headless = {
        doc["metadata"]["name"]: doc
        for doc in docs
        if doc.get("kind") == "Service"
        and (doc.get("spec") or {}).get("clusterIP") == "None"
    }
    for name, doc in headless.items():
        if (doc.get("spec") or {}).get("publishNotReadyAddresses"):
            problems.append(
                f"Service/{name} is the peer directory and sets "
                "publishNotReadyAddresses: an unready pod would be handed "
                "shard work"
            )
    if "--query-peer-discovery-srv" not in args:
        # Discovery off is a valid render (query.distributed.enabled=false),
        # but then nothing must claim a scaled fan-out either.
        return problems
    srv = args[args.index("--query-peer-discovery-srv") + 1]
    if not srv.startswith("_http._tcp."):
        problems.append(
            f"--query-peer-discovery-srv={srv!r} does not name the headless "
            "Service's `http` port (_http._tcp.<service>.<ns>.svc...)"
        )
    else:
        service = srv[len("_http._tcp.") :].split(".", 1)[0]
        if service not in headless:
            problems.append(
                f"--query-peer-discovery-srv={srv!r} names {service!r}, which "
                f"is not a rendered headless Service (have {sorted(headless)})"
            )
    self_name = next(
        (e for e in query.get("env") or [] if e.get("name") == PEER_SELF_NAME_ENV),
        None,
    )
    field = ((self_name or {}).get("valueFrom") or {}).get("fieldRef") or {}
    if field.get("fieldPath") != "metadata.name":
        problems.append(
            f"query StatefulSet must set {PEER_SELF_NAME_ENV} from the "
            "downward API's metadata.name; without its own pod name the "
            f"coordinator cannot find its failover URL (got {self_name!r})"
        )
    return problems


def check_query_spill(docs: list[dict], expected: dict[str, str]) -> list[str]:
    """Hold the spill env, mount, emptyDir, and resource budget together."""
    query_pod: dict | None = None
    query_container: dict | None = None
    for doc in docs:
        if doc.get("kind") != "StatefulSet":
            continue
        pod = doc["spec"]["template"]["spec"]
        for container in pod.get("containers", []):
            if container.get("name") == QUERY_CONTAINER:
                query_pod = pod
                query_container = container

    if query_pod is None or query_container is None:
        return [f"StatefulSet container '{QUERY_CONTAINER}' was not rendered"]

    problems: list[str] = []
    env = {
        entry.get("name"): str(entry.get("value"))
        for entry in query_container.get("env") or []
    }
    for name, want in [
        (SPILL_DIR_ENV, expected["directory"]),
        (SPILL_MAX_ENV, expected["max_bytes"]),
    ]:
        if env.get(name) != want:
            problems.append(
                f"StatefulSet container '{QUERY_CONTAINER}' renders {name}="
                f"{env.get(name)!r}; expected {want!r}"
            )

    mounts = {
        mount.get("name"): mount for mount in query_container.get("volumeMounts") or []
    }
    spill_mount = mounts.get("query-spill")
    if spill_mount is None:
        problems.append("query StatefulSet has no query-spill volumeMount")
    elif str(spill_mount.get("mountPath")) != expected["directory"]:
        problems.append(
            "query-spill mountPath "
            f"{spill_mount.get('mountPath')!r}; expected {expected['directory']!r}"
        )

    volumes = {volume.get("name"): volume for volume in query_pod.get("volumes") or []}
    spill_volume = volumes.get("query-spill")
    if spill_volume is None:
        problems.append("query StatefulSet has no query-spill volume")
    else:
        empty_dir = spill_volume.get("emptyDir")
        if not isinstance(empty_dir, dict):
            problems.append("query-spill volume is not an emptyDir")
        elif str(empty_dir.get("sizeLimit")) != expected["size_limit"]:
            problems.append(
                "query-spill emptyDir sizeLimit "
                f"{empty_dir.get('sizeLimit')!r}; expected {expected['size_limit']!r}"
            )

    resources = query_container.get("resources") or {}
    for side, want in [("requests", expected["request"]), ("limits", expected["limit"])]:
        actual = (resources.get(side) or {}).get("ephemeral-storage")
        if str(actual) != want:
            problems.append(
                f"query resources.{side}.ephemeral-storage={actual!r}; expected {want!r}"
            )
    return problems


def warm_stalled_rules(docs: list[dict]) -> list[dict]:
    """Every rendered `SiglakeQueryWarmCycleStalled` rule (0 or 1 expected)."""
    rules: list[dict] = []
    for d in docs:
        if d.get("kind") != "PrometheusRule":
            continue
        for group in d["spec"].get("groups") or []:
            for rule in group.get("rules") or []:
                if rule.get("alert") == WARM_ALERT:
                    rules.append(rule)
    return rules


def check_warm_interval(docs: list[dict]) -> list[str]:
    """The stalled-warmer alert must assume the cadence the query pods run.

    `SiglakeQueryWarmCycleStalled` is the earliest signal for the query
    degradation that only a restart cures, and it is the one alert whose
    threshold is a chart value: `for:` is three warm intervals, so a rule built
    for a 300s cadence on pods running 30s stays quiet for 15 minutes of
    stall, and one built for 30s on pods running 300s pages on every healthy
    cycle. Both sides read `query.warmIntervalSecs`, and nothing but this
    check notices if one of them stops. At 0 (startup-only warm) there is no
    cycle to miss and the rule must be absent, not rendered as `for: 0s` on an
    always-true clause.

    `prometheusRule.queryWarmIntervalSecs` exists to break the coupling on
    purpose, for pods whose cadence comes from `query.extraEnv`; a render with
    it set fails here by design, so the matrix does not set it.
    """
    problems: list[str] = []
    rendered, value = query_warm_interval(docs)
    if not rendered:
        # No query tier in this render: nothing for the rule to be held to.
        return problems
    if value is None:
        problems.append(
            f"StatefulSet container '{QUERY_CONTAINER}' renders no {WARM_ENV}; the "
            f"chart renders it unconditionally so the pods and {WARM_ALERT} read "
            f"the same value"
        )
        return problems
    try:
        warm = int(value)
    except ValueError:
        problems.append(f"{WARM_ENV}={value!r} on '{QUERY_CONTAINER}' is not an integer")
        return problems

    if not any(d.get("kind") == "PrometheusRule" for d in docs):
        # prometheusRule.enabled=false: there is no alert to hold to the pods.
        return problems
    rules = warm_stalled_rules(docs)
    if warm == 0:
        if rules:
            problems.append(
                f"{WARM_ALERT} is rendered although {WARM_ENV}=0 (startup-only warm): "
                f"there is no cycle to miss, and the rule degenerates to `for: 0s` on "
                f"an always-true clause"
            )
        return problems
    if not rules:
        problems.append(
            f"{WARM_ALERT} is absent although the query pods run {WARM_ENV}={warm}: "
            f"a warmer that stalls on that cadence would alert nothing"
        )
    for rule in rules:
        for_ = str(rule.get("for", ""))
        m = FOR_SECONDS.fullmatch(for_)
        if not m:
            problems.append(
                f"{WARM_ALERT} has for: {for_!r}; expected plain seconds ('<n>s') so it "
                f"can be held to {WARM_ENV}"
            )
            continue
        stale = int(m.group(1))
        if stale != 3 * warm:
            problems.append(
                f"{WARM_ALERT} holds for {stale}s (three intervals of {stale / 3:g}s), but "
                f"the query pods run {WARM_ENV}={warm}; both must derive from "
                f"query.warmIntervalSecs"
            )
        # The staleness clause is the same `$stale`; `for:` alone would leave a
        # threshold that disagrees with its own hold time.
        expr = str(rule.get("expr", ""))
        if not re.search(rf">\s*{stale}(?!\d)", expr):
            problems.append(
                f"{WARM_ALERT} holds for {stale}s but its expr does not compare "
                f"staleness against {stale}: {expr.strip()!r}"
            )
    return problems


def mirror_stalled_rules(docs: list[dict]) -> list[dict]:
    """Every rendered `SiglakeMirrorReconciliationStalled` rule (0 or 1 expected)."""
    rules: list[dict] = []
    for d in docs:
        if d.get("kind") != "PrometheusRule":
            continue
        for group in d["spec"].get("groups") or []:
            for rule in group.get("rules") or []:
                if rule.get("alert") == MIRROR_ALERT:
                    rules.append(rule)
    return rules


def check_mirror_rotation_window(docs: list[dict], expected: int) -> list[str]:
    """The stalled-reconciliation alert must read the window the values file sets.

    Mirror-to-catalog reconciliation is the only owner of the "object in the
    mirror, no catalog row" window, and since it became a bounded, cursor-durable
    walk it can stall at full page throughput with every other compactor signal
    healthy. The one thing that says it is converging is a rotation completing,
    and how long to wait for one depends on the retained prefix, so the window is
    `prometheusRule.mirrorRotationStallSecs`. A hardcoded range in the template
    would silently stop following that value — the same coupling defect
    `check_warm_interval` exists for — so every range selector in the rendered
    expression must BE the configured window. At 0 the rule must be absent rather
    than rendered with `[0s]`, which is not a valid range at all.

    Both arms are asserted by name. Dropping the page counter leaves a rule that
    fires on any cluster with reconciliation switched off; dropping the rotation
    gauge leaves one that fires whenever reconciliation runs.
    """
    if not any(d.get("kind") == "PrometheusRule" for d in docs):
        # prometheusRule.enabled=false: there is no rule to hold to the value.
        return []

    problems: list[str] = []
    rules = mirror_stalled_rules(docs)
    if expected <= 0:
        if rules:
            problems.append(
                f"{MIRROR_ALERT} is rendered although {MIRROR_STALL_VALUE}={expected}: "
                f"0 disables the alert, and the range selectors would render as [0s]"
            )
        return problems
    if not rules:
        problems.append(
            f"{MIRROR_ALERT} is absent although {MIRROR_STALL_VALUE}={expected}: "
            f"a reconciliation walk that never wraps around would alert nothing"
        )
    for rule in rules:
        expr = str(rule.get("expr", ""))
        ranges = RANGE_SECONDS.findall(expr)
        if not ranges:
            problems.append(
                f"{MIRROR_ALERT} has no `[<n>s]` range selector in its expr; the window "
                f"must be {MIRROR_STALL_VALUE} in plain seconds so it can be held to the "
                f"value: {expr.strip()!r}"
            )
        for value, unit in ranges:
            if unit != "s" or int(value) != expected:
                problems.append(
                    f"{MIRROR_ALERT} reads over [{value}{unit}], but "
                    f"{MIRROR_STALL_VALUE}={expected}; the alert's window is that value, "
                    f"not a constant"
                )
        for arm in MIRROR_ALERT_ARMS:
            if arm not in expr:
                problems.append(
                    f"{MIRROR_ALERT} no longer reads '{arm}': the rule is only a stall "
                    f"signal with both a bounded-page arm and the durable "
                    f"rotation-generation arm — one alone fires on a healthy or a "
                    f"deliberately disabled reconciliation"
                )
        if MIRROR_PROGRESS_GAUGE in expr:
            problems.append(
                f"{MIRROR_ALERT} reads '{MIRROR_PROGRESS_GAUGE}': a live pod retains "
                f"that value after reconciliation is disabled or it loses the lease, "
                f"so stale nonzero progress must not make the activity arm true"
            )
    return problems


def quantile_selected(expr: str) -> set[str]:
    """Metric names whose label selector matches on `quantile`."""
    return {
        name
        for name, selector in SELECTOR.findall(expr)
        if selector and QUANTILE_MATCHER.search(selector)
    }


def histogram_quantile_args(expr: str) -> list[str]:
    """The argument text of every `histogram_quantile(...)` call, parens balanced."""
    out: list[str] = []
    start = 0
    while True:
        i = expr.find(HISTOGRAM_QUANTILE, start)
        if i < 0:
            return out
        j = i + len(HISTOGRAM_QUANTILE)
        depth = 1
        k = j
        while k < len(expr) and depth:
            depth += {"(": 1, ")": -1}.get(expr[k], 0)
            k += 1
        out.append(expr[j : k - 1] if depth == 0 else expr[j:])
        start = k


def metric_problems(expr: str, exported: "Exported") -> list[str]:
    """Every way a PromQL expression names a `siglake_*` series that does not
    exist as written, one message per offending name.

    - no `metrics::` macro emits the base name (a rename);
    - `_bucket` / `_sum` / `_count` on a gauge or counter, which have neither;
    - `_bucket`, or `histogram_quantile()`, on a histogram siglake-core exports
      as a summary: there is no `_bucket` series and the quantile reads nothing;
    - the bare name of a histogram exported in histogram form, with or without
      a `quantile=` matcher: only its `_bucket` / `_sum` / `_count` exist.

    `_sum` and `_count` exist in both forms and are never flagged. Counters
    already carry `_total` in the name.
    """
    problems: list[str] = []
    by_quantile = quantile_selected(expr)
    under_histogram_quantile: set[str] = set()
    for arg in histogram_quantile_args(expr):
        under_histogram_quantile.update(METRIC_NAME.findall(arg))

    for name in sorted(set(METRIC_NAME.findall(expr))):
        if name in exported.kinds:
            base, suffix = name, None
        else:
            base = HISTOGRAM_SUFFIX.sub("", name)
            suffix = name[len(base) + 1 :] or None
            if base not in exported.kinds:
                problems.append(
                    f"references '{name}', which no metrics:: macro in crates/ or the owned "
                    f"forks emits"
                )
                continue
        form = exported.form(base)
        if form is None:
            if suffix:
                kinds = "/".join(sorted(exported.kinds[base]))
                problems.append(
                    f"references '{name}', but {base} is a {kinds}: only histograms "
                    f"expose _{suffix}"
                )
            continue
        if form == "summary":
            if suffix == "bucket":
                problems.append(
                    f"references '{name}', but siglake-core exports {base} as a "
                    f"summary (quantile label, no _bucket series): give it buckets in "
                    f"{METRICS_RS} builder() before querying _bucket"
                )
            elif name in under_histogram_quantile:
                problems.append(
                    f"applies histogram_quantile() to '{name}', but siglake-core "
                    f"exports {base} as a summary (quantile label, no _bucket series): "
                    f"give it buckets in {METRICS_RS} builder(), or select "
                    f"{base}{{quantile=...}} directly"
                )
        elif suffix is None:
            if name in by_quantile:
                problems.append(
                    f"selects '{name}' by quantile, but siglake-core exports it as a "
                    f"histogram (_bucket/_sum/_count, no quantile label): use "
                    f"histogram_quantile() over {name}_bucket"
                )
            else:
                problems.append(
                    f"references '{name}' bare, but siglake-core exports it as a "
                    f"histogram: only {name}_bucket, _sum and _count exist"
                )
    return problems


def dashboard_exprs(dashboard: dict) -> list[tuple[str, str]]:
    """Every (location, PromQL) pair a Grafana dashboard evaluates.

    Panel targets, including panels nested under a collapsed row, plus the
    templating variables — the `namespace` dropdown is a `label_values(...)`
    over a metric too, and a rename there empties every panel at once.
    """
    exprs: list[tuple[str, str]] = []
    for var in (dashboard.get("templating") or {}).get("list") or []:
        query = var.get("query")
        if isinstance(query, dict):
            query = query.get("query")
        if isinstance(query, str):
            exprs.append((f"variable '{var.get('name')}'", query))

    def walk(panels: list[dict]) -> None:
        for panel in panels:
            for target in panel.get("targets") or []:
                expr = target.get("expr")
                if isinstance(expr, str):
                    exprs.append(
                        (f"panel '{panel.get('title')}' target {target.get('refId')}", expr)
                    )
            walk(panel.get("panels") or [])

    walk(dashboard.get("panels") or [])
    return exprs


def dashboard_panels(dashboard: dict) -> list[dict]:
    """Every panel, flattened, including those nested under a collapsed row."""
    out: list[dict] = []

    def walk(panels: list[dict]) -> None:
        for panel in panels:
            out.append(panel)
            walk(panel.get("panels") or [])

    walk(dashboard.get("panels") or [])
    return out


def check_dashboard(path: pathlib.Path, exported: "Exported") -> list[str]:
    """A dashboard panel on a metric nothing emits renders as an empty chart,
    which reads as "nothing happening" — the same shape as an alert that never
    fires. The 2026-09-01 side-aggregate pin regression sat on a hit/miss
    counter for two months with no panel; the panel is only worth adding if a
    rename cannot silently blank it again."""
    problems: list[str] = []
    try:
        dashboard = json.loads(path.read_text())
    except (OSError, ValueError) as e:
        return [f"{path}: not valid JSON ({e})"]

    # Grafana keys panels by id within a dashboard; a duplicate id makes the
    # import keep one panel and drop the other without complaint.
    ids = collections.Counter(p.get("id") for p in dashboard_panels(dashboard))
    for panel_id, n in sorted(ids.items(), key=lambda kv: str(kv[0])):
        if panel_id is None:
            problems.append(f"{path}: {n} panel(s) have no id")
        elif n > 1:
            problems.append(f"{path}: panel id {panel_id} is used {n} times")

    if exported:
        for where, expr in dashboard_exprs(dashboard):
            for problem in metric_problems(expr, exported):
                problems.append(f"{path}: {where} {problem}")

    # The breaker panel is how an operator sees WHICH limit refused a request,
    # so a new refusal reason must reach it without anyone editing the
    # dashboard: no `breaker=` matcher, and `breaker` in the grouping. #2184
    # added four series to this counter (the Jaeger read ceilings) and this is
    # what says they are readable rather than merely emitted.
    for where, expr in dashboard_exprs(dashboard):
        if "siglake_query_breaker_trips_total" not in expr:
            continue
        if re.search(
            r"siglake_query_breaker_trips_total\s*\{[^}]*\bbreaker\s*(=|=~|!=|!~)", expr
        ):
            problems.append(
                f"{path}: {where} filters siglake_query_breaker_trips_total on `breaker`, "
                f"so a breaker added later is invisible until someone edits the "
                f"dashboard; group `by (breaker)` instead"
            )
        elif not re.search(r"by\s*\(\s*[^)]*\bbreaker\b[^)]*\)", expr):
            problems.append(
                f"{path}: {where} reads siglake_query_breaker_trips_total without "
                f"grouping by `breaker`, so the panel cannot say WHICH limit refused"
            )

    # The same rule for the text-index startup families (#3969): each one exists
    # to say WHICH stage, outcome or bound is responsible, and a panel that
    # matched on the dimension instead of grouping by it would answer a question
    # the operator already knew the answer to — and would go silent when the
    # reader's vocabulary grows.
    for metric, dimension in TEXT_INDEX_GROUPINGS.items():
        for where, expr in dashboard_exprs(dashboard):
            if metric not in expr:
                continue
            if re.search(rf"{metric}[a-z_]*\s*\{{[^}}]*\b{dimension}\s*(=|=~|!=|!~)", expr):
                problems.append(
                    f"{path}: {where} filters {metric} on `{dimension}`, so a new "
                    f"`{dimension}` value is invisible until someone edits the "
                    f"dashboard; group `by ({dimension})` instead"
                )
            elif not re.search(rf"by\s*\(\s*[^)]*\b{dimension}\b[^)]*\)", expr):
                problems.append(
                    f"{path}: {where} reads {metric} without grouping by "
                    f"`{dimension}`, so the panel cannot say WHICH `{dimension}` the "
                    f"number belongs to"
                )
    return problems


OVERVIEW_DASHBOARD = DASHBOARD_DIR / "siglake-overview.json"
# #3969's text-index families and the dimension each panel must GROUP BY rather
# than match on: the stage of a per-file index load, the parsed-index cache's
# lookup outcome, and which bound dropped an entry. The vocabularies are the
# reader's, exported from the Iceberg fork
# (`TEXT_INDEX_STARTUP_STAGES`, `PARSED_INDEX_CACHE_OUTCOMES`,
# `PARSED_INDEX_CACHE_DROP_REASONS`) and held to these panels by
# `text_index_startup_series_are_preregistered` in siglake-storage.
TEXT_INDEX_GROUPINGS = {
    "siglake_iceberg_text_index_startup_seconds": "stage",
    "siglake_iceberg_parsed_index_cache_lookups_total": "outcome",
    "siglake_iceberg_parsed_index_cache_evictions_total": "reason",
}
# "Text-index startup by stage (p50 / p99)".
TEXT_INDEX_STARTUP_PANEL = 159
# A two-stage classic histogram, one stage recorded under both storage forms.
# `(stage, storage) -> [(le, observations)]`, cumulative as Prometheus expects.
# The quantiles below are the linear interpolation within the bucket that holds
# them (`lower + (upper - lower) * q`, with the lowest bucket's lower bound at
# 0), which is what `histogram_quantile` computes for a classic histogram:
# `decode` sits in (0.05, 0.5] and `permit_wait` in (0, 0.0025].
TEXT_INDEX_STARTUP_SERIES = {
    ("decode", "puffin"): [("0.05", 0), ("0.5", 20), ("+Inf", 20)],
    ("decode", "footer_kv"): [("0.05", 0), ("0.5", 10), ("+Inf", 10)],
    ("permit_wait", "puffin"): [("0.0025", 30), ("0.05", 30), ("+Inf", 30)],
}
TEXT_INDEX_STARTUP_EXPECTED = {
    "0.50": {"decode": 0.05 + (0.5 - 0.05) * 0.5, "permit_wait": 0.0025 * 0.5},
    "0.99": {"decode": 0.05 + (0.5 - 0.05) * 0.99, "permit_wait": 0.0025 * 0.99},
}
# "Drain backlog (segments + bytes)".
DRAIN_BACKLOG_PANEL = 111
# Two clusters' worth of `siglake_compactor_sealed_pending*` series, as
# `namespace -> [(pod, tenant, value)]`.
#
# `logs` is a catalog-claim fleet: `peek_pending` counts the whole sealed table
# with no worker filter and labels it `tenant="default"`, so all three workers
# publish the same total and the panel must chart that total, not three times
# it. `logs-staging` is a filesystem drain — one pod, one series per tenant,
# which do add up. Both shapes have to read correctly under ONE expression,
# and the two namespaces have to stay two lines.
DRAIN_BACKLOG_SERIES = {
    "siglake_compactor_sealed_pending": {
        "logs": [
            ("compactor-0", "default", 8),
            ("compactor-1", "default", 8),
            ("compactor-2", "default", 8),
        ],
        "logs-staging": [
            ("compactor-0", "acme", 2),
            ("compactor-0", "globex", 1),
        ],
    },
    "siglake_compactor_sealed_pending_bytes": {
        "logs": [
            ("compactor-0", "default", 4096),
            ("compactor-1", "default", 4096),
            ("compactor-2", "default", 4096),
        ],
        "logs-staging": [
            ("compactor-0", "acme", 1024),
            ("compactor-0", "globex", 2048),
        ],
    },
}


def drain_backlog_exprs(dashboard: dict) -> dict[str, str]:
    """Panel 111's expressions, keyed by the metric each one reads.

    Read from the shipped panel rather than copied, so the fixture below tests
    what an operator imports. A panel or metric that has moved returns short and
    is reported: this check must be re-pointed, not left passing on nothing.
    """
    out: dict[str, str] = {}
    for panel in dashboard_panels(dashboard):
        if panel.get("id") != DRAIN_BACKLOG_PANEL:
            continue
        for target in panel.get("targets") or []:
            expr = target.get("expr")
            if not isinstance(expr, str):
                continue
            for metric in DRAIN_BACKLOG_SERIES:
                # The selector brace is what separates `…_pending` from
                # `…_pending_bytes`, which has the shorter name as a prefix.
                if re.search(rf"\b{metric}\{{", expr):
                    out.setdefault(metric, expr)
    return out


def deduplicated_totals(series: dict[str, list]) -> dict[str, int]:
    """Each namespace's queue depth: a tenant's deepest copy, summed."""
    totals = {}
    for namespace, entries in series.items():
        by_tenant: dict[str, int] = {}
        for _pod, tenant, value in entries:
            by_tenant[tenant] = max(by_tenant.get(tenant, 0), value)
        totals[namespace] = sum(by_tenant.values())
    return totals


def drain_backlog_fixture(exprs: dict[str, str]) -> str:
    """A `promtool test rules` file over panel 111's two expressions.

    Each gets the fleet sum beside it as the control arm — the reading the panel
    charted before #3692, which counts one shared queue once per worker and
    collapses every selected namespace into one line. A fixture both expressions
    satisfy would be no evidence, so the caller checks they differ.
    """
    out = "evaluation_interval: 1m\ntests:\n"
    for metric, expr in sorted(exprs.items()):
        series = DRAIN_BACKLOG_SERIES[metric]
        out += f"  - name: {metric} across two clusters\n    interval: 5m\n"
        out += "    input_series:\n"
        for namespace, entries in sorted(series.items()):
            for pod, tenant, value in entries:
                out += (
                    f"      - series: '{metric}{{namespace=\"{namespace}\","
                    f'app_kubernetes_io_instance="siglake",'
                    f'app_kubernetes_io_component="compactor",'
                    f"pod=\"{pod}\",tenant=\"{tenant}\"}}'\n"
                    f"        values: '_ {value}'\n"
                )
        out += "    promql_expr_test:\n"
        out += f"      - expr: '{expr.replace('$namespace', '.*')}'\n"
        out += "        eval_time: 5m\n        exp_samples:\n"
        for namespace, total in sorted(deduplicated_totals(series).items()):
            out += (
                f"          - labels: '{{namespace=\"{namespace}\"}}'\n"
                f"            value: {total}\n"
            )
        fleet = sum(v for entries in series.values() for _p, _t, v in entries)
        out += f"      - expr: 'sum({metric})'\n"
        out += "        eval_time: 5m\n        exp_samples:\n"
        out += f"          - labels: '{{}}'\n            value: {fleet}\n"
    return out


def check_drain_backlog_panel(require_promtool: bool = False) -> tuple[list[str], bool]:
    """Evaluate panel 111's expressions with Prometheus' own engine.

    `check_dashboard` holds every panel to a metric something emits; it says
    nothing about what the expression computes from it. This one does, for the
    panel where the arithmetic is not obvious: the backlog gauge is a shared
    queue under the catalog claim and per-tenant local counts under the
    filesystem drain, and one expression charts both.

    Returns (problems, skipped); a box without promtool skips unless
    `require_promtool`.
    """
    try:
        dashboard = json.loads(OVERVIEW_DASHBOARD.read_text())
    except (OSError, ValueError) as e:
        return [f"{OVERVIEW_DASHBOARD}: not readable as JSON ({e})"], False
    exprs = drain_backlog_exprs(dashboard)
    missing = sorted(set(DRAIN_BACKLOG_SERIES) - set(exprs))
    if missing:
        return [
            f"{OVERVIEW_DASHBOARD}: panel {DRAIN_BACKLOG_PANEL} no longer reads "
            f"{', '.join(missing)}; re-point DRAIN_BACKLOG_SERIES rather than "
            f"leaving this check evaluating nothing"
        ], False
    for metric, expr in exprs.items():
        if "'" in expr:
            return [f"{OVERVIEW_DASHBOARD}: {metric} expression needs YAML escaping"], False
        totals = deduplicated_totals(DRAIN_BACKLOG_SERIES[metric])
        fleet = sum(
            v for entries in DRAIN_BACKLOG_SERIES[metric].values() for _p, _t, v in entries
        )
        if sum(totals.values()) == fleet:
            return [
                f"{OVERVIEW_DASHBOARD}: the {metric} fixture reads the same under the "
                f"panel and under a bare sum, so it cannot tell them apart"
            ], False
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = pathlib.Path(tmp_dir)
        name = "drain-backlog.test.yaml"
        (tmp_path / name).write_text(drain_backlog_fixture(exprs))
        try:
            subprocess.run(
                ["promtool", "test", "rules", name],
                capture_output=True,
                text=True,
                check=True,
                cwd=tmp_path,
            )
        except FileNotFoundError:
            problem = (
                "promtool not installed; the drain-backlog panel expressions were "
                "not evaluated"
            )
            return ([problem] if require_promtool else []), True
        except subprocess.CalledProcessError as e:
            output = "\n".join(s for s in (e.stdout.strip(), e.stderr.strip()) if s)
            return [
                f"{OVERVIEW_DASHBOARD}: panel {DRAIN_BACKLOG_PANEL} does not read one "
                f"queue depth per namespace:" + (f"\n{output}" if output else "")
            ], False
    return [], False


def text_index_startup_exprs(dashboard: dict) -> dict[str, str]:
    """Panel 159's expressions, keyed by the quantile each one takes.

    Read from the shipped panel rather than copied, so the fixture below
    evaluates what an operator imports.
    """
    out: dict[str, str] = {}
    for panel in dashboard_panels(dashboard):
        if panel.get("id") != TEXT_INDEX_STARTUP_PANEL:
            continue
        for target in panel.get("targets") or []:
            expr = target.get("expr")
            if not isinstance(expr, str):
                continue
            if "siglake_iceberg_text_index_startup_seconds_bucket" not in expr:
                continue
            found = re.search(r"histogram_quantile\(\s*([0-9.]+)", expr)
            if found:
                out.setdefault(f"{float(found.group(1)):.2f}", expr)
    return out


def text_index_startup_fixture(exprs: dict[str, str]) -> str:
    """A `promtool test rules` file over panel 159's quantile expressions.

    The fixture records `decode` under both storage forms and `permit_wait`
    under one, in different buckets. One sample per STAGE with the storage forms
    summed is the reading the panel is for: an expression that dropped `le` from
    the grouping returns nothing, one that kept `storage` returns three samples
    with a `storage` label, and one that grouped by neither collapses two very
    different stages into one line.
    """
    out = "evaluation_interval: 1m\ntests:\n"
    for quantile, expr in sorted(exprs.items()):
        out += f"  - name: text-index startup p{quantile}\n    interval: 1m\n"
        out += "    input_series:\n"
        for (stage, storage), buckets in sorted(TEXT_INDEX_STARTUP_SERIES.items()):
            for le, observations in buckets:
                out += (
                    f"      - series: 'siglake_iceberg_text_index_startup_seconds_bucket"
                    f'{{namespace="logs",app_kubernetes_io_instance="siglake",'
                    f'app_kubernetes_io_component="query-server",pod="query-server-0",'
                    f'stage="{stage}",storage="{storage}",le="{le}"}}\'\n'
                    f"        values: '0+{observations}x6'\n"
                )
        out += "    promql_expr_test:\n"
        out += f"      - expr: '{expr.replace('$namespace', '.*')}'\n"
        out += "        eval_time: 6m\n        exp_samples:\n"
        for stage, value in sorted(TEXT_INDEX_STARTUP_EXPECTED[quantile].items()):
            out += (
                f"          - labels: '{{stage=\"{stage}\"}}'\n"
                f"            value: {value}\n"
            )
    return out


def check_text_index_startup_panel(require_promtool: bool = False) -> tuple[list[str], bool]:
    """Evaluate panel 159's quantile expressions with Prometheus' own engine.

    `check_dashboard` holds the panel to a metric the source emits and to
    grouping by `stage`; this one says the quantile arithmetic reads one number
    per stage out of a fleet's buckets. It is the half of #3969's acceptance
    that a structural check cannot cover: the whole point of the panel is to
    separate `decode` from `permit_wait`, and an expression that quietly folds
    them together looks like a working chart.

    Returns (problems, skipped); a box without promtool skips unless
    `require_promtool`.
    """
    try:
        dashboard = json.loads(OVERVIEW_DASHBOARD.read_text())
    except (OSError, ValueError) as e:
        return [f"{OVERVIEW_DASHBOARD}: not readable as JSON ({e})"], False
    exprs = text_index_startup_exprs(dashboard)
    missing = sorted(set(TEXT_INDEX_STARTUP_EXPECTED) - set(exprs))
    if missing:
        return [
            f"{OVERVIEW_DASHBOARD}: panel {TEXT_INDEX_STARTUP_PANEL} no longer takes "
            f"quantile(s) {', '.join(missing)} over "
            f"siglake_iceberg_text_index_startup_seconds_bucket; re-point "
            f"TEXT_INDEX_STARTUP_EXPECTED rather than leaving this check evaluating "
            f"nothing"
        ], False
    for quantile, expr in exprs.items():
        if "'" in expr:
            return [
                f"{OVERVIEW_DASHBOARD}: the p{quantile} expression needs YAML escaping"
            ], False
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = pathlib.Path(tmp_dir)
        name = "text-index-startup.test.yaml"
        (tmp_path / name).write_text(text_index_startup_fixture(exprs))
        try:
            subprocess.run(
                ["promtool", "test", "rules", name],
                capture_output=True,
                text=True,
                check=True,
                cwd=tmp_path,
            )
        except FileNotFoundError:
            problem = (
                "promtool not installed; the text-index startup panel expressions "
                "were not evaluated"
            )
            return ([problem] if require_promtool else []), True
        except subprocess.CalledProcessError as e:
            output = "\n".join(s for s in (e.stdout.strip(), e.stderr.strip()) if s)
            return [
                f"{OVERVIEW_DASHBOARD}: panel {TEXT_INDEX_STARTUP_PANEL} does not read "
                f"one latency per stage:" + (f"\n{output}" if output else "")
            ], False
    return [], False


def check_dashboards(exported: "Exported | None" = None) -> tuple[int, list[str]]:
    """Check every dashboard under deploy/grafana; returns (count, problems)."""
    if exported is None:
        exported = Exported.load()
    paths = sorted(DASHBOARD_DIR.glob("*.json"))
    problems: list[str] = []
    for path in paths:
        problems.extend(check_dashboard(path, exported))
    return len(paths), problems


def alert_count(template: str) -> int:
    """Number of `- alert:` rules in the PrometheusRule template source."""
    return len(ALERT_LINE.findall(template))


def check_alert_count(template: str, readme: str) -> list[str]:
    """README's "`PrometheusRule` with N alerts" must equal the template's count.

    The number was typed by hand when the rule shipped and nothing has read it
    since; every alert added after that silently makes the README wrong. It is
    checked here because this script already parses the rule file, and from
    the source rather than a render so it reports where helm is unavailable.
    """
    problems: list[str] = []
    n = alert_count(template)
    claimed = [int(m) for m in README_ALERT_COUNT.findall(readme)]
    if not claimed:
        problems.append(
            f"{README}: no sentence matching /{README_ALERT_COUNT.pattern}/; "
            f"the alert count in the 'No built-in UI' bullet is unguarded"
        )
    for c in claimed:
        if c != n:
            problems.append(
                f"{README} says the PrometheusRule ships {c} alerts, but "
                f"{RULE_TEMPLATE} defines {n} `- alert:` rules"
            )
    return problems


def check_alert_count_files() -> tuple[int, list[str]]:
    """Run check_alert_count on the checked-out files; returns (count, problems)."""
    try:
        template = RULE_TEMPLATE.read_text()
        readme = README.read_text()
    except OSError as e:
        return 0, [f"cannot read {e.filename}: {e.strerror}"]
    return alert_count(template), check_alert_count(template, readme)


class ExpositionRuleError(Exception):
    """metrics.rs no longer carries its bucket configuration in a shape this
    script can read. The check must be re-pointed, not left to guess: a
    catalog that quietly called every histogram a summary would fail every
    `_bucket` panel, and one that called them all histograms would pass a
    `_bucket` query on a summary — the defect this exists to catch."""


class PreregistrationRuleError(Exception):
    """metrics.rs no longer carries the pre-registration catalog in a shape
    this script can read. Same rule as ExpositionRuleError: re-point, do not
    guess. An empty catalog would pass every `increase()` rule while every
    first increment on a fresh pod went unseen — the defect this exists to
    catch."""


@dataclasses.dataclass(frozen=True)
class Exported:
    """Every metric the shipped Rust source emits, and the form the exporter
    renders it in.

    `kinds` maps a name to the macro(s) that record it. A `metrics::histogram!`
    renders as a true Prometheus histogram (`_bucket` / `_sum` / `_count`, no
    bare series) only when `builder()` in metrics.rs hands the recorder buckets
    for its name — by suffix, by prefix, by full name, or globally. Anything
    else renders as a summary: the bare name with a `quantile` label, plus
    `_sum` / `_count`. The matchers are read from metrics.rs so this file never
    carries a copy of the list that can drift from it.

    `counter_series` maps a counter name to every label set some
    `metrics::counter!` records it under with string literals for both key and
    value — the series that exist before the first increment and so can be
    pre-registered. `counter_dynamic` names the counters with at least one
    site whose label value is an expression, known only at the increment.
    """

    kinds: dict[str, frozenset[str]]
    bucketed_suffixes: tuple[str, ...] = ()
    bucketed_prefixes: tuple[str, ...] = ()
    bucketed_names: frozenset[str] = frozenset()
    all_bucketed: bool = False
    counter_series: dict[str, frozenset[frozenset[tuple[str, str]]]] = dataclasses.field(
        default_factory=dict
    )
    counter_dynamic: frozenset[str] = frozenset()

    def __bool__(self) -> bool:
        return bool(self.kinds)

    def form(self, name: str) -> str | None:
        """'histogram' or 'summary' for an emitted histogram; None otherwise."""
        if "histogram" not in self.kinds.get(name, ()):
            return None
        if (
            self.all_bucketed
            or name in self.bucketed_names
            or name.endswith(self.bucketed_suffixes)
            or name.startswith(self.bucketed_prefixes)
        ):
            return "histogram"
        return "summary"

    def histograms(self) -> dict[str, list[str]]:
        """Emitted histogram names grouped by form."""
        out: dict[str, list[str]] = {"histogram": [], "summary": []}
        for name in sorted(self.kinds):
            form = self.form(name)
            if form:
                out[form].append(name)
        return out

    @classmethod
    def load(
        cls,
        root: pathlib.Path = pathlib.Path("crates"),
        extra_roots: tuple[pathlib.Path, ...] = METRIC_FORK_ROOTS,
    ) -> "Exported":
        """Read the macros under `root` and the bucket rule from metrics.rs.

        No `root` at all (run from another directory) yields an empty catalog
        and the metric checks skip, as they always have. A `root` whose
        metrics.rs cannot be read the way this script reads it raises
        ExpositionRuleError instead.

        `extra_roots` are the owned forks under `third_party/`, which emit
        `siglake_*` metrics of their own — the whole text-index and
        object-store read families live in the Iceberg fork's reader. They ship
        in the same binaries as `crates/`, so a dashboard may read them and a
        binary may pre-register them; leaving them out made every one of those
        series look like a rename to `check_dashboard` (#3969).
        """
        if not root.is_dir():
            return cls({})
        kinds: dict[str, set[str]] = {}
        series: dict[str, set[frozenset[tuple[str, str]]]] = {}
        dynamic: set[str] = set()
        sources = [root, *(extra for extra in extra_roots if extra.is_dir())]
        for path in [p for source in sources for p in source.rglob("*.rs")]:
            if "/tests/" in str(path):
                continue
            try:
                text = path.read_text()
            except OSError:
                continue
            for kind, name in MACRO.findall(text):
                kinds.setdefault(name, set()).add(kind)
            for name, labels in counter_sites(text):
                if labels is None:
                    dynamic.add(name)
                else:
                    series.setdefault(name, set()).add(labels)
        try:
            source = METRICS_RS.read_text()
        except OSError as e:
            raise ExpositionRuleError(f"cannot read {METRICS_RS}: {e.strerror}") from e
        suffixes, prefixes, names, everything = exposition_rule(source)
        return cls(
            {k: frozenset(v) for k, v in kinds.items()},
            suffixes,
            prefixes,
            names,
            everything,
            {k: frozenset(v) for k, v in series.items()},
            frozenset(dynamic),
        )


def split_args(text: str) -> list[str]:
    """Split macro argument text at the commas outside brackets and strings."""
    parts: list[str] = []
    cur: list[str] = []
    depth = 0
    in_str = False
    i = 0
    while i < len(text):
        c = text[i]
        if in_str:
            cur.append(c)
            if c == "\\" and i + 1 < len(text):
                cur.append(text[i + 1])
                i += 1
            elif c == '"':
                in_str = False
        elif c == '"':
            in_str = True
            cur.append(c)
        elif c in "([{":
            depth += 1
            cur.append(c)
        elif c in ")]}":
            depth -= 1
            cur.append(c)
        elif c == "," and depth == 0:
            parts.append("".join(cur).strip())
            cur = []
        else:
            cur.append(c)
        i += 1
    tail = "".join(cur).strip()
    if tail:
        parts.append(tail)
    return parts


def counter_sites(text: str) -> list[tuple[str, "frozenset[tuple[str, str]] | None"]]:
    """(name, label set) for every `metrics::counter!("literal", ...)` in `text`.

    The label set is the `"key" => "value"` pairs when every key and value is a
    string literal, and None when any is an expression (a column, a table),
    since that series is only known at the increment. A site whose NAME is not
    a literal is skipped: no rule can spell it either.
    """
    out: list[tuple[str, frozenset[tuple[str, str]] | None]] = []
    start = 0
    while True:
        i = text.find(COUNTER_MACRO, start)
        if i < 0:
            return out
        j = i + len(COUNTER_MACRO)
        depth = 1
        k = j
        in_str = False
        while k < len(text) and depth:
            c = text[k]
            if in_str:
                if c == "\\":
                    k += 1
                elif c == '"':
                    in_str = False
            elif c == '"':
                in_str = True
            else:
                depth += {"(": 1, ")": -1}.get(c, 0)
            k += 1
        start = k
        args = split_args(text[j : k - 1] if depth == 0 else text[j:])
        if not args:
            continue
        name = STR_ARG.match(args[0])
        if not name or not name.group(1).startswith("siglake_"):
            continue
        labels: set[tuple[str, str]] = set()
        static = True
        for arg in args[1:]:
            key, sep, value = arg.partition("=>")
            k_lit = STR_ARG.match(key.strip())
            v_lit = STR_ARG.match(value.strip())
            if not (sep and k_lit and v_lit):
                static = False
                break
            labels.add((k_lit.group(1), v_lit.group(1)))
        out.append((name.group(1), frozenset(labels) if static else None))


@dataclasses.dataclass(frozen=True)
class Preregistered:
    """The catalog metrics.rs pre-registers at startup: for each counter name,
    every label set created at 0 (the empty set for the unlabelled series),
    and the names an `increase()` alert reads that no binary can pre-register,
    each with a reason in the source."""

    series: dict[str, frozenset[frozenset[tuple[str, str]]]]
    unregisterable: frozenset[str]


def preregistration_rule(source: str) -> Preregistered:
    """Parse the `AlertedCounter` literals and UNREGISTERABLE_ALERTED_COUNTERS
    from the non-test part of metrics.rs, and insist the `preregister()` loop
    that turns the list into series is still there."""
    body = source.split("#[cfg(test)]", 1)[0]
    entries = ALERTED_COUNTER.findall(body)
    if not entries:
        raise PreregistrationRuleError(
            f"{METRICS_RS} has no `AlertedCounter {{ name: \"...\", series: ... }}` "
            f"literals; re-point this check at the pre-registration catalog"
        )
    if not PREREGISTER_LOOP.search(body):
        raise PreregistrationRuleError(
            f"{METRICS_RS} lists AlertedCounters but no `for _ in _.series` loop "
            f"registers them; the list is not the rule"
        )
    series: dict[str, set[frozenset[tuple[str, str]]]] = {}
    for name, text in entries:
        sets = series.setdefault(name, set())
        if text == "UNLABELLED":
            sets.add(frozenset())
            continue
        for inner in INNER_SERIES.findall(text):
            sets.add(frozenset(LABEL_PAIR.findall(inner)))
    m = UNREGISTERABLE.search(body)
    if not m:
        raise PreregistrationRuleError(
            f"{METRICS_RS} has no `pub const UNREGISTERABLE_ALERTED_COUNTERS: "
            f"&[(&str, &str)] = &[...]`; re-point this check at the exemption list"
        )
    return Preregistered(
        {k: frozenset(v) for k, v in series.items()},
        frozenset(UNREGISTERABLE_NAME.findall(m.group(1))),
    )


def check_preregistered(exported: "Exported", catalog: Preregistered) -> list[str]:
    """Every way the PrometheusRule's `increase()` counters and the catalog
    disagree, one message each.

    - an `increase()`d counter in neither list: its first increment on a fresh
      pod is invisible, the defect the catalog exists to close;
    - a name in both lists, or a listed name no `metrics::counter!` emits (a
      rename), or an unregisterable name every site records with literal
      labels (it could be pre-registered after all);
    - a literal label set the code records under a pre-registered name that
      the catalog does not create: that series still starts at its first
      increment.
    """
    problems: list[str] = []
    try:
        template = RULE_TEMPLATE.read_text()
    except OSError as e:
        return [f"cannot read {e.filename}: {e.strerror}"]
    increased = set(INCREASE_NAME.findall(template))
    registered = set(catalog.series)
    listed = registered | catalog.unregisterable
    for name in sorted(increased - listed):
        problems.append(
            f"increase({name}) in {RULE_TEMPLATE} reads a counter no binary "
            f"pre-registers, so its first increment on a fresh pod is invisible: add "
            f"it to a *_ALERTED_COUNTERS list in {METRICS_RS}, or to "
            f"UNREGISTERABLE_ALERTED_COUNTERS with the reason"
        )
    for name in sorted(registered & catalog.unregisterable):
        problems.append(
            f"{METRICS_RS} lists '{name}' both as pre-registered and as unregisterable"
        )
    for name in sorted(listed):
        if "counter" not in exported.kinds.get(name, ()):
            problems.append(
                f"{METRICS_RS} lists '{name}' for pre-registration, but no "
                f"metrics::counter! in crates/ or the owned forks emits it"
            )
    for name in sorted(catalog.unregisterable):
        if name in exported.kinds and name not in exported.counter_dynamic:
            problems.append(
                f"{METRICS_RS} calls '{name}' unregisterable, but every site records "
                f"it with literal labels: pre-register it instead"
            )
    for name in sorted(registered):
        for labels in sorted(exported.counter_series.get(name, ()), key=sorted):
            if labels in catalog.series[name]:
                continue
            shown = ",".join(f'{k}="{v}"' for k, v in sorted(labels))
            problems.append(
                f"the source records {name}{{{shown}}}, a series {METRICS_RS} does not "
                f"pre-register: add that label set to its AlertedCounter entry"
            )
    return problems


def exposition_rule(
    source: str,
) -> tuple[tuple[str, ...], tuple[str, ...], frozenset[str], bool]:
    """(suffixes, prefixes, full names, every histogram?) that get buckets.

    Parsed from the non-test part of metrics.rs: each `Matcher::{Suffix,
    Prefix,Full}` literal handed to `set_buckets_for_metric`, the
    `COUNT_HISTOGRAMS` list `builder()` loops over, and whether a global
    `set_buckets` call buckets every histogram. The test module is cut off
    first: it deliberately records `_seconds_max` names to prove the suffix
    matcher does NOT fire on them.
    """
    body = source.split("#[cfg(test)]", 1)[0]
    everything = bool(GLOBAL_BUCKETS.search(body))
    if "set_buckets_for_metric" not in body and not everything:
        raise ExpositionRuleError(
            f"{METRICS_RS} calls neither set_buckets_for_metric nor set_buckets; "
            f"every histogram would render as a summary — re-point this check at "
            f"wherever buckets are configured now"
        )
    m = COUNT_HISTOGRAMS.search(body)
    if not m:
        raise ExpositionRuleError(
            f"{METRICS_RS} has no `pub const COUNT_HISTOGRAMS: &[&str] = &[...]`; "
            f"re-point this check at the list of bucketed count histograms"
        )
    if not COUNT_HISTOGRAMS_LOOP.search(body):
        raise ExpositionRuleError(
            f"{METRICS_RS} defines COUNT_HISTOGRAMS but no `for _ in COUNT_HISTOGRAMS` "
            f"loop hands them to set_buckets_for_metric; the list is not the rule"
        )
    names = set(STR_LITERAL.findall(m.group(1)))
    suffixes: list[str] = []
    prefixes: list[str] = []
    for kind, literal in BUCKET_MATCHER.findall(body):
        if kind == "Suffix":
            suffixes.append(literal)
        elif kind == "Prefix":
            prefixes.append(literal)
        else:
            names.add(literal)
    if not (suffixes or prefixes or names or everything):
        raise ExpositionRuleError(
            f"{METRICS_RS} configures buckets but this script found no "
            f"Matcher::Suffix/Prefix/Full literal to apply; re-point the parser"
        )
    return tuple(suffixes), tuple(prefixes), frozenset(names), everything


def source_checks(
    require_promtool: bool = False,
) -> tuple[bool, "Exported", "frozenset[str] | None"]:
    """The checks that need no render, in the order they report.

    Returns (failed, exported, env_catalog): the two source catalogs are read
    once here and shared with the render matrix.
    """
    failed = False
    # A metrics.rs this script can no longer parse is a FAIL of its own, not a
    # reason to skip: without the rule the form checks cannot run at all.
    try:
        exported = Exported.load()
    except ExpositionRuleError as e:
        print(f"FAIL [metrics] {e}", file=sys.stderr)
        failed = True
        exported = Exported({})
    else:
        if exported:
            forms = exported.histograms()
            print(
                f"ok   [metrics] {len(exported.kinds)} names emitted under crates/ and "
                f"the owned forks; "
                f"{len(forms['histogram'])} histograms bucketed, "
                f"{len(forms['summary'])} rendered as summaries, per {METRICS_RS}",
                flush=True,
            )
        else:
            print(
                "skip [metrics] no crates/ directory; metric-name checks not run",
                flush=True,
            )
    # The env-name catalog and its two deployment surfaces need no helm. The
    # rendered chart joins this check in check() for every matrix entry below.
    try:
        env_catalog = env_name_catalog()
        deployment_locations = compose_env_locations()
        deployment_locations.extend(rust_env_locations(OPERATOR_RENDER_RS))
    except EnvCatalogError as e:
        print(f"FAIL [env] {e}", file=sys.stderr)
        failed = True
        env_catalog = None
    else:
        problems = unknown_env_problems(deployment_locations, env_catalog)
        problems.extend(compose_query_file_cache_problems())
        if problems:
            failed = True
            for p in problems:
                print(f"FAIL [env] {p}", file=sys.stderr)
        else:
            compose_names = {
                name
                for where, name in deployment_locations
                if where.startswith(str(COMPOSE_FILE))
            }
            render_names = {
                name
                for where, name in deployment_locations
                if where.startswith(str(OPERATOR_RENDER_RS))
            }
            print(
                f"ok   [env] {len(env_catalog)} names in non-test Rust source; "
                f"{len(compose_names)} set by {COMPOSE_FILE}, "
                f"{len(render_names)} named by {OPERATOR_RENDER_RS}",
                flush=True,
            )
    # The pre-registration catalog, held to the PrometheusRule's `increase()`
    # counters and to the label sets the code records. Same skip rule as the
    # catalog above: no crates/, nothing to hold.
    if exported:
        try:
            catalog = preregistration_rule(METRICS_RS.read_text())
        except (OSError, PreregistrationRuleError) as e:
            print(f"FAIL [preregistered] {e}", file=sys.stderr)
            failed = True
        else:
            problems = check_preregistered(exported, catalog)
            if problems:
                failed = True
                for p in problems:
                    print(f"FAIL [preregistered] {p}", file=sys.stderr)
            else:
                increased = set(INCREASE_NAME.findall(RULE_TEMPLATE.read_text()))
                print(
                    f"ok   [preregistered] {len(increased)} counters read through "
                    f"increase() in {RULE_TEMPLATE}: "
                    f"{len(increased & set(catalog.series))} pre-registered at startup, "
                    f"{len(increased & catalog.unregisterable)} documented as "
                    f"unregisterable, per {METRICS_RS}",
                    flush=True,
                )
    # The dashboard needs no render, so it is checked before any and still
    # reports when helm itself is unavailable.
    count, problems = check_dashboards(exported)
    panel_problems, panel_skipped = check_drain_backlog_panel(require_promtool)
    problems.extend(panel_problems)
    startup_problems, startup_skipped = check_text_index_startup_panel(require_promtool)
    problems.extend(startup_problems)
    panel_skipped = panel_skipped or startup_skipped
    if problems:
        failed = True
        for p in problems:
            print(f"FAIL [dashboard] {p}", file=sys.stderr)
    else:
        panel_result = (
            "; panel expressions skipped (promtool not installed)"
            if panel_skipped
            else f"; panels {DRAIN_BACKLOG_PANEL} and {TEXT_INDEX_STARTUP_PANEL} "
            f"passed promtool"
        )
        print(
            f"ok   [dashboard] {count} dashboard(s) under {DASHBOARD_DIR}{panel_result}",
            flush=True,
        )
    # The install notes' authentication predicate is template source too; the
    # arms that read the rendered notes are in check_query_notes below.
    problems = check_notes_auth_predicate()
    if problems:
        failed = True
        for p in problems:
            print(f"FAIL [notes] {p}", file=sys.stderr)
    else:
        print(
            f"ok   [notes] {NOTES_TEMPLATE} warns about an open query API under the "
            f"same `{QUERY_AUTH_HELPER}` {QUERY_STS_TEMPLATE} runs on",
            flush=True,
        )
    # Nor does the README's alert count, which is read from the template source.
    count, problems = check_alert_count_files()
    if problems:
        failed = True
        for p in problems:
            print(f"FAIL [alerts] {p}", file=sys.stderr)
    else:
        print(
            f"ok   [alerts] {count} `- alert:` rules in {RULE_TEMPLATE}; {README} agrees",
            flush=True,
        )
    return failed, exported, env_catalog


def check_query_notes(chart: str, base: list[str], oidc_issuer: str) -> bool:
    """Hold the install notes to the query tier's real authentication.

    Returns True when an arm failed. Separate from the render matrix because
    the notes reach this script through a probe copy of the chart
    (notes_probe_chart says why), and separate from main() so the arms can be
    exercised without a render.
    """
    failed = False
    # #4317: what the operator is TOLD about the query tier's authentication.
    # One install with nothing authenticating the query API, which must warn,
    # and each way of turning authentication on, which must not — OIDC
    # included, since that is the arm the warning used to ignore. The disabled
    # tier has no API to call open. Each authenticated arm carries a
    # coordinator token for the same reason the matrix arms above do.
    notes_matrix = [
        ("notes-open", [], True),
        ("notes-oidc",
         ["--set", f"query.oidc.issuer={oidc_issuer}",
          "--set", "query.oidc.audience=siglake-query",
          "--set", "query.distributed.coordinatorToken.value=coord"], False),
        ("notes-oidc-tenant-claim",
         ["--set", f"query.oidc.issuer={oidc_issuer}",
          "--set", "query.oidc.audience=siglake-query",
          "--set", "query.oidc.tenantClaim=org_id",
          "--set", "query.distributed.coordinatorToken.value=coord"], False),
        ("notes-tokens-inline",
         ["--set", "query.tokens.list={dev-1}",
          "--set", "query.distributed.coordinatorToken.value=coord"], False),
        ("notes-tokens-existing-secret",
         ["--set", "query.tokens.existingSecret=byo-query-tokens",
          "--set", "query.distributed.coordinatorToken.value=coord"], False),
        ("notes-tokens-eso",
         ["--set", "externalSecrets.enabled=true",
          "--set", "externalSecrets.queryTokens.remoteKey=siglake/query-tokens",
          "--set", "query.distributed.coordinatorToken.value=coord"], False),
        ("notes-query-disabled", ["--set", "query.enabled=false"], False),
    ]
    with tempfile.TemporaryDirectory() as tmp_dir:
        probe = notes_probe_chart(pathlib.Path(chart), pathlib.Path(tmp_dir))
        for label, extra, expect_warning in notes_matrix:
            try:
                notes = render_notes(str(probe), base + extra)
            except subprocess.CalledProcessError as error:
                failed = True
                print(
                    f"FAIL [{label}] the render that carries NOTES.txt failed:"
                    f"\n{error.stderr}",
                    file=sys.stderr,
                )
                continue
            problems = check_notes_open_warning(notes, expect_warning)
            if problems:
                failed = True
                for p in problems:
                    print(f"FAIL [{label}] {p}", file=sys.stderr)
            else:
                said = "warns the query API is open" if expect_warning else "does not"
                print(f"ok   [{label}] the install notes {said}", flush=True)
    return failed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate the Helm chart render matrix, deployment env names, "
        "Grafana dashboards and the README's alert count against the code."
    )
    parser.add_argument(
        "--source-only",
        action="store_true",
        help="run only the checks that need no `helm template` (the metric and "
        "environment catalogs, non-Helm deployment surfaces, dashboards and "
        "README alert count) and skip the render matrix; for a box without helm",
    )
    parser.add_argument(
        "--require-promtool",
        action="store_true",
        help="fail instead of reporting a skip when promtool is not installed; CI uses "
        "this after installing the binary",
    )
    args = parser.parse_args(argv)

    failed, exported, env_catalog = source_checks(args.require_promtool)
    if args.source_only or env_catalog is None:
        return 1 if failed else 0

    base = ["--set", "s3.region=us-west-2", "--set", "s3.bucket=ci"]
    # Every toggle that changes what is rendered, since a defect can hide behind
    # a default-off flag — the now-removed query HPA did exactly that.
    matrix = [
        ("defaults", []),
        ("kind", ["-f", "deploy/kind/values.kind.yaml"]),
        # The compactor HPA carries a ceiling of 4, and since #2955 a ceiling
        # above one pod is refused without the claim — so this arm enables the
        # claim, and the refusal itself is a guard in ci-local.sh / ci.yml.
        ("autoscaling", ["--set", "autoscaling.ingester.enabled=true",
                         "--set", "autoscaling.compactor.enabled=true",
                         "--set", "compactor.catalogClaim.enabled=true"]),
        # #3718's three supported neighbours of the backlog-metric refusal.
        # The claim's HPA keeps scaling on CPU (the `autoscaling` arm above);
        # the ingester's custom metric is a genuine per-pod rate and is not
        # touched by a compactor-side refusal; and the compactor's own metric
        # still renders in filesystem mode, where the sealed count is one pod's
        # own — at the single pod the claim refusal holds that mode to.
        ("compactor-backlog-metric-filesystem",
         ["--set", "autoscaling.compactor.enabled=true",
          "--set", "autoscaling.compactor.maxReplicas=1",
          "--set", "autoscaling.compactor.customMetric.enabled=true"]),
        ("ingester-custom-metric-claimed",
         ["--set", "autoscaling.ingester.enabled=true",
          "--set", "autoscaling.ingester.customMetric.enabled=true",
          "--set", "compactor.catalogClaim.enabled=true"]),
        # A custom metric under an autoscaler that is OFF is inert: no HPA is
        # rendered, so there is nothing to refuse.
        ("compactor-backlog-metric-inert",
         ["--set", "autoscaling.compactor.enabled=false",
          "--set", "autoscaling.compactor.customMetric.enabled=true",
          "--set", "compactor.catalogClaim.enabled=true"]),
        # The supported compactor scale-out: claim on, mirror at its default,
        # two pods. The other half of #2955's guard — this render must keep
        # working, or the refusal has swallowed the configuration it exists to
        # steer people towards.
        ("compactor-scale-out", ["--set", "compactor.replicas=2",
                                 "--set", "compactor.catalogClaim.enabled=true"]),
        ("single-pod", ["--set", "query.replicas=1",
                        "--set", "query.distributed.enabled=false"]),
        # #967: the render the chart used to REFUSE outright. A KEDA ceiling
        # above query.replicas is the whole point of runtime peer discovery,
        # and check_query_peer_discovery holds the four halves that make it
        # safe — so this scenario is what stops the refusal from being removed
        # without the discovery that replaced it.
        ("query-scale-out", ["--set", "keda.enabled=true",
                             "--set", "keda.query.maxReplicas=8",
                             "--set", "prometheusRule.enabled=true"]),
        # ServiceMonitor is default-OFF, and it is the object that decides
        # whether every metric on a tier is scraped once or twice.
        ("monitoring", ["--set", "serviceMonitor.enabled=true",
                        "--set", "keda.enabled=true",
                        "--set", "prometheusRule.enabled=true"]),
        # The warm cadence is read twice — by the query pods as
        # SIGLAKE_QUERY_WARM_INTERVAL_SECS and by SiglakeQueryWarmCycleStalled
        # as its `for:` — and check() holds them equal on every render. With the
        # rule on: at the default; at a non-default value, so a pair of
        # hardcoded 30/90 could not pass; and at 0, where the rule must be
        # absent rather than rendered as `for: 0s`.
        ("warm-interval", ["--set", "prometheusRule.enabled=true"]),
        ("warm-interval-45", ["--set", "prometheusRule.enabled=true",
                              "--set", "query.warmIntervalSecs=45"]),
        ("warm-startup-only", ["--set", "prometheusRule.enabled=true",
                               "--set", "query.warmIntervalSecs=0"]),
        # The mirror-rotation window is the other threshold that is a chart
        # value: at a non-default value, so a hardcoded range cannot pass, and
        # at 0, where the rule must be absent rather than rendered as `[0s]`.
        ("mirror-stall-window", ["--set", "prometheusRule.enabled=true",
                                 "--set", "prometheusRule.mirrorRotationStallSecs=7200"]),
        ("mirror-stall-disabled", ["--set", "prometheusRule.enabled=true",
                                   "--set", "prometheusRule.mirrorRotationStallSecs=0"]),
        ("file-cache-disabled", ["--set", "query.scan.fileCacheMaxBytes=0",
                                 "--set", "query.scan.fileCacheMaxEntries=0"]),
        ("file-cache-bounded", ["--set", "query.scan.fileCacheMaxBytes=1048576",
                                "--set", "query.scan.fileCacheMaxEntries=64"]),
        # Rebuild defaults off in the binary, so the opt-in arm is the one that
        # can rot: `indexRebuild: true` has to render "1" out loud.
        ("index-rebuild-enabled", ["--set", "compactor.indexRebuild=true"]),
        # The WAL mirror is default-on, so its opt-out is the arm that can
        # rot: `enabled: false` has to render an EMPTY prefix, not no variable
        # at all. Renamed prefix in the third arm so a hardcoded "wal-mirror"
        # on either side of the writer/reader pair cannot pass.
        ("mirror-disabled", ["--set", "wal.mirror.enabled=false"]),
        ("mirror-claimed", ["--set", "compactor.catalogClaim.enabled=true"]),
        ("mirror-renamed", ["--set", "compactor.catalogClaim.enabled=true",
                            "--set", "wal.mirror.prefix=wal-dr"]),
        # The binary defaults gRPC on, so false must pass its explicit opt-out
        # and remove both Kubernetes ports rather than merely omitting the
        # listener argument.
        ("otlp-grpc-disabled", ["--set", "ingester.otlpGrpc.enabled=false"]),
        # The batch-job store is default-on, so the arm that can rot is its
        # opt-out: `persistent: false` must drop the variable entirely — a
        # blank one crash-loops the tier on `connect Postgres `.
        ("jobs-in-memory", ["--set", "query.jobs.persistent=false",
                            "--set", "query.replicas=1"]),
        # Global KEDA can remain on while query autoscaling is off. Its dormant
        # ceiling must not turn the supported one-pod in-memory tier into a
        # refusal.
        ("jobs-in-memory-query-keda-disabled",
         ["--set", "query.jobs.persistent=false",
          "--set", "query.replicas=1",
          "--set", "keda.enabled=true",
          "--set", "keda.query.enabled=false",
          "--set", "keda.query.maxReplicas=8"]),
        # #4273: the three ways a token allow-list reaches the pods. Every
        # other arm leaves all three unset and is checked for the open install,
        # so a hardcoded secretKeyRef cannot pass either. Each needs a
        # coordinator token because auth plus the default two-pod fan-out is
        # refused (the refusal list below holds that door from the other side).
        ("query-tokens-eso",
         ["--set", "externalSecrets.enabled=true",
          "--set", "externalSecrets.queryTokens.remoteKey=siglake/query-tokens",
          "--set", "query.tokens.secretKey=allowlist",
          "--set", "query.distributed.coordinatorToken.value=coord"]),
        ("query-tokens-existing-secret",
         ["--set", "query.tokens.existingSecret=byo-query-tokens",
          "--set", "query.distributed.coordinatorToken.value=coord"]),
        ("query-tokens-inline",
         ["--set", "query.tokens.list={dev-1,dev-2}",
          "--set", "query.distributed.coordinatorToken.value=coord"]),
        # ESO naming a Secret the customer already owns: the ExternalSecret
        # fills that name instead of the chart's, and the pods must follow it
        # there rather than to `<fullname>-query-tokens`.
        ("query-tokens-eso-existing-secret",
         ["--set", "externalSecrets.enabled=true",
          "--set", "externalSecrets.queryTokens.remoteKey=siglake/query-tokens",
          "--set", "query.tokens.existingSecret=byo-query-tokens",
          "--set", "query.distributed.coordinatorToken.value=coord"]),
        # ESO on with no query remoteKey (the Postgres-only setup): the token
        # path is untouched and the tier is still open.
        ("query-tokens-eso-postgres-only",
         ["--set", "externalSecrets.enabled=true"]),
        # #4303: a COMPLETE OIDC block per tier — the half the refusal list
        # below would otherwise be free to swallow. Every other arm leaves both
        # blocks empty and is checked for no SIGLAKE_OIDC_* at all, so a
        # hardcoded issuer cannot pass either. The query arms carry a
        # coordinator token because OIDC IS authentication here: it feeds the
        # same `$authOn` as a token list, so the default two-pod fan-out needs
        # a credential to present to its peers.
        ("oidc-ingester",
         ["--set", "ingester.oidc.issuer=https://idp.example/realms/siglake",
          "--set", "ingester.oidc.audience=siglake-ingest",
          "--set", "ingester.oidc.tenantClaim=org_id"]),
        ("oidc-query",
         ["--set", "query.oidc.issuer=https://idp.example/realms/siglake",
          "--set", "query.oidc.audience=siglake-query",
          "--set", "query.oidc.tenantClaim=org_id",
          "--set", "query.distributed.coordinatorToken.value=coord"]),
        # The single-tenant OIDC configuration on both tiers: issuer and
        # audience with no claim, where SIGLAKE_OIDC_TENANT_CLAIM must be
        # ABSENT rather than rendered blank — the binaries read a blank claim
        # as off, so a rendered empty one would mean the two disagree about
        # whether tenancy is on.
        ("oidc-no-tenant-claim",
         ["--set", "ingester.oidc.issuer=https://idp.example/realms/siglake",
          "--set", "ingester.oidc.audience=siglake-ingest",
          "--set", "query.oidc.issuer=https://idp.example/realms/siglake",
          "--set", "query.oidc.audience=siglake-query",
          "--set", "query.distributed.coordinatorToken.value=coord"]),
        ("query-perf", ["-f", "deploy/aws/config/values.query-perf.yaml"]),
        ("spill-sized", ["--set", "query.spill.directory=/scratch/query",
                          "--set-string", "query.spill.maxBytes=1073741824",
                          "--set", "query.spill.sizeLimit=2Gi",
                          "--set", "query.resources.requests.ephemeral-storage=2Gi",
                          "--set", "query.resources.limits.ephemeral-storage=3Gi"]),
    ]
    expected_file_cache_env = {
        "defaults": DEFAULT_FILE_CACHE_ENV,
        "file-cache-disabled": {
            FILE_CACHE_BYTES_ENV: "0",
            FILE_CACHE_ENTRIES_ENV: "0",
        },
        "file-cache-bounded": {
            FILE_CACHE_BYTES_ENV: "1048576",
            FILE_CACHE_ENTRIES_ENV: "64",
        },
        "query-perf": {
            FILE_CACHE_BYTES_ENV: "536870912",
            FILE_CACHE_ENTRIES_ENV: "512",
        },
    }
    expected_mirror_stall = {
        "mirror-stall-window": 7200,
        "mirror-stall-disabled": 0,
    }
    expected_wal_mirror_prefix = {
        "mirror-disabled": "",
        "mirror-renamed": "wal-dr",
    }
    expected_otlp_grpc_port = {
        "otlp-grpc-disabled": None,
    }
    expected_persistent_jobs = {
        "jobs-in-memory": False,
        "jobs-in-memory-query-keda-disabled": False,
    }
    expected_index_rebuild = {
        "index-rebuild-enabled": "1",
    }
    # #4273: the Secret and key SIGLAKE_QUERY_TOKENS must come from, per arm.
    # Absent = the open install, which is what every other arm renders and what
    # check_query_token_source holds them to.
    expected_query_tokens = {
        "query-tokens-eso": (CHART_QUERY_TOKENS_SECRET, "allowlist"),
        "query-tokens-existing-secret": ("byo-query-tokens", "tokens"),
        "query-tokens-inline": (CHART_QUERY_TOKENS_SECRET, "tokens"),
        "query-tokens-eso-existing-secret": ("byo-query-tokens", "tokens"),
    }
    # #4303: the SIGLAKE_OIDC_* variables each tier must carry, per arm. A tier
    # absent from an arm's entry must render none at all, which is what every
    # other arm asserts.
    oidc_issuer = "https://idp.example/realms/siglake"
    expected_oidc = {
        "oidc-ingester": {
            "ingester": {
                "SIGLAKE_OIDC_ISSUER": oidc_issuer,
                "SIGLAKE_OIDC_AUDIENCE": "siglake-ingest",
                "SIGLAKE_OIDC_TENANT_CLAIM": "org_id",
            },
        },
        "oidc-query": {
            "query": {
                "SIGLAKE_OIDC_ISSUER": oidc_issuer,
                "SIGLAKE_OIDC_AUDIENCE": "siglake-query",
                "SIGLAKE_OIDC_TENANT_CLAIM": "org_id",
            },
        },
        "oidc-no-tenant-claim": {
            "ingester": {
                "SIGLAKE_OIDC_ISSUER": oidc_issuer,
                "SIGLAKE_OIDC_AUDIENCE": "siglake-ingest",
            },
            "query": {
                "SIGLAKE_OIDC_ISSUER": oidc_issuer,
                "SIGLAKE_OIDC_AUDIENCE": "siglake-query",
            },
        },
    }
    # #3718: which metrics each HPA may carry, per arm. The compactor's backlog
    # gauge is a per-pod (`Pods`) metric to the autoscaler, so it belongs only
    # on an arm where the reading is one pod's own.
    expected_hpa_metrics = {
        "autoscaling": {
            "compactor": ["Resource:cpu"],
            "ingester": ["Resource:cpu"],
        },
        "compactor-backlog-metric-filesystem": {
            "compactor": ["Resource:cpu", "Pods:siglake_compactor_sealed_pending"],
        },
        "ingester-custom-metric-claimed": {
            "compactor": [],
            "ingester": [
                "Resource:cpu",
                "Pods:siglake_ingest_requests_per_second",
            ],
        },
        "compactor-backlog-metric-inert": {"compactor": []},
        "defaults": {"compactor": [], "ingester": []},
    }
    expected_query_spill = {
        "spill-sized": {
            "directory": "/scratch/query",
            "max_bytes": "1073741824",
            "size_limit": "2Gi",
            "request": "2Gi",
            "limit": "3Gi",
        }
    }
    helm_available = True
    for label, extra in matrix:
        try:
            docs = render("deploy/helm/siglake", base + extra)
        except FileNotFoundError:
            # No helm binary at all. One FAIL line, not one per render: every
            # entry would fail the same way, and a traceback here used to bury
            # the source-check lines that did report.
            print(
                f"FAIL [{label}] helm not installed; the render matrix did not run "
                f"(--source-only runs just the checks above)",
                file=sys.stderr,
            )
            failed = True
            helm_available = False
            break
        except subprocess.CalledProcessError as e:
            print(f"FAIL [{label}] helm template failed:\n{e.stderr}", file=sys.stderr)
            failed = True
            continue
        problems = check(
            docs,
            exported,
            expected_file_cache_env.get(label),
            expected_query_spill.get(label),
            env_catalog,
            expected_mirror_stall.get(label, DEFAULT_MIRROR_STALL_SECS),
            expected_wal_mirror_prefix.get(label),
            expected_otlp_grpc_port.get(label, 4317),
            expected_persistent_jobs.get(label, True),
            expected_index_rebuild.get(label, "0"),
            expected_query_tokens.get(label),
        )
        problems.extend(check_hpa_metrics(docs, expected_hpa_metrics.get(label, {})))
        problems.extend(check_oidc_env(docs, expected_oidc.get(label, {})))
        checked_rules, promtool_problems, promtool_skipped = check_prometheus_rules(
            docs, require_promtool=args.require_promtool
        )
        problems.extend(promtool_problems)
        if problems:
            failed = True
            for p in problems:
                print(f"FAIL [{label}] {p}", file=sys.stderr)
        else:
            if promtool_skipped:
                promtool_result = "; PrometheusRule syntax skipped (promtool not installed)"
            elif checked_rules:
                promtool_result = f"; {checked_rules} PrometheusRule(s) passed promtool"
            else:
                promtool_result = ""
            print(f"ok   [{label}] {len(docs)} objects{promtool_result}", flush=True)

    if not helm_available:
        return 1 if failed else 0

    failed = check_query_notes("deploy/helm/siglake", base, oidc_issuer) or failed

    # These are correctness refusals, not lint failures, and each one is paired
    # with must-render arms in the matrix above so it cannot grow past its
    # intent. #3115: both ways the enabled query tier can reach a second pod
    # (the matrix covers the default shared store, shared-store KEDA scale-out,
    # the one-pod opt-out and disabled KEDA query settings). #3718: the
    # compactor backlog metric under the claim (the matrix covers CPU-only
    # claim-mode scaling, filesystem mode at one pod, the ingester's own custom
    # metric and the inert disabled-HPA settings).
    jobs_message = "query.jobs.persistent is false"
    jobs_allowed = "leaving per-pod batch-job stores behind one Service"
    # #3718: the compactor's backlog gauge is the whole shared queue under the
    # claim, and the HPA can only ask for it per pod. Both doors: the tier this
    # chart renders, and one it does not (`compactor.enabled: false`), since the
    # HPA scales whatever Deployment carries the name.
    backlog_message = "autoscaling.compactor.customMetric.enabled is true"
    backlog_allowed = (
        "leaving an HPA that multiplies the shared sealed queue by the pod count"
    )
    backlog_metric = [
        "--set", "autoscaling.compactor.enabled=true",
        "--set", "autoscaling.compactor.customMetric.enabled=true",
        "--set", "compactor.catalogClaim.enabled=true",
    ]
    # #4273: the coordinator-token refusal reads the SAME resolved token source
    # as the env var, so turning auth on through ESO has to reach it too. The
    # `query-tokens-*` arms above are the must-render half.
    coord_message = "query.distributed.coordinatorToken is unset"
    coord_allowed = (
        "leaving a coordinator that presents no credential to its peers, so every "
        "fanned-out query 401s"
    )
    eso_tokens = [
        "--set", "externalSecrets.enabled=true",
        "--set", "externalSecrets.queryTokens.remoteKey=siglake/query-tokens",
    ]
    token_source_message = (
        "query.tokens.list and externalSecrets.queryTokens.remoteKey cannot both be set"
    )
    token_source_allowed = (
        "installing a Secret and an ExternalSecret that claim the same query-token "
        "Secret name"
    )
    # #4303: an incomplete OIDC block on an enabled tier. Each half-pair and the
    # claim on its own, per tier, plus the claim beside bearer tokens — the
    # combination that made the omission hardest to see, because the tier came
    # up authenticated, just not the way the values asked for. The
    # `oidc-*` matrix arms above are the must-render half.
    oidc_pair_allowed = (
        "installing a tier that verifies no token at all while its values ask for "
        "OIDC"
    )
    oidc_claim_allowed = (
        "installing a tier whose tenancy was supposed to come from a verified "
        "token, with no verifier to take it from"
    )
    refusals = [
        ("ingester-oidc-issuer-only",
         ["--set", "ingester.oidc.issuer=https://idp.example/realms/siglake"],
         "ingester.oidc.issuer is set but ingester.oidc.audience is empty",
         oidc_pair_allowed),
        ("ingester-oidc-audience-only",
         ["--set", "ingester.oidc.audience=siglake-ingest"],
         "ingester.oidc.audience is set but ingester.oidc.issuer is empty",
         oidc_pair_allowed),
        ("ingester-oidc-claim-only",
         ["--set", "ingester.oidc.tenantClaim=org_id"],
         "ingester.oidc.tenantClaim is set but ingester.oidc.issuer and "
         "ingester.oidc.audience are empty",
         oidc_claim_allowed),
        # Bearer tokens beside the claim: they authenticate the caller, they do
        # not carry the tenant, so this install would still route every write to
        # `default` while the values said the tenant came from the token.
        ("ingester-oidc-claim-only-with-tokens",
         ["--set", "ingester.oidc.tenantClaim=org_id",
          "--set", "ingester.auth.list={write-1}"],
         "ingester.oidc.tenantClaim is set but ingester.oidc.issuer and "
         "ingester.oidc.audience are empty",
         oidc_claim_allowed),
        ("query-oidc-issuer-only",
         ["--set", "query.oidc.issuer=https://idp.example/realms/siglake"],
         "query.oidc.issuer is set but query.oidc.audience is empty",
         oidc_pair_allowed),
        ("query-oidc-audience-only",
         ["--set", "query.oidc.audience=siglake-query"],
         "query.oidc.audience is set but query.oidc.issuer is empty",
         oidc_pair_allowed),
        ("query-oidc-claim-only",
         ["--set", "query.oidc.tenantClaim=org_id"],
         "query.oidc.tenantClaim is set but query.oidc.issuer and "
         "query.oidc.audience are empty",
         oidc_claim_allowed),
        # Token auth on and a coordinator token set, so the coordinator-token
        # guard has nothing to say and the OIDC refusal is the only one that can
        # fire: the partial block has to be refused on its own terms, not as a
        # side effect of another guard.
        ("query-oidc-claim-only-with-tokens",
         ["--set", "query.oidc.tenantClaim=org_id",
          "--set", "query.tokens.list={dev-1}",
          "--set", "query.distributed.coordinatorToken.value=coord"],
         "query.oidc.tenantClaim is set but query.oidc.issuer and "
         "query.oidc.audience are empty",
         oidc_claim_allowed),
        # A complete OIDC block IS auth, so it reaches the coordinator-token
        # guard exactly as a token list does — the `oidc-query` arm above is the
        # same values with the token set.
        ("query-oidc-no-coordinator",
         ["--set", "query.oidc.issuer=https://idp.example/realms/siglake",
          "--set", "query.oidc.audience=siglake-query"],
         coord_message, coord_allowed),
        ("query-tokens-eso-plus-inline",
         eso_tokens + ["--set", "query.tokens.list={dev-1,dev-2}"],
         token_source_message, token_source_allowed),
        ("query-tokens-eso-no-coordinator", eso_tokens, coord_message, coord_allowed),
        ("jobs-in-memory-static-scale-out",
         ["--set", "query.jobs.persistent=false",
          "--set", "query.replicas=2"],
         jobs_message, jobs_allowed),
        ("jobs-in-memory-keda-scale-out",
         ["--set", "query.jobs.persistent=false",
          "--set", "query.replicas=1",
          "--set", "keda.enabled=true",
          "--set", "keda.query.enabled=true",
          "--set", "keda.query.maxReplicas=2"],
         jobs_message, jobs_allowed),
        ("compactor-backlog-metric-claimed",
         backlog_metric, backlog_message, backlog_allowed),
        ("compactor-backlog-metric-claimed-tier-disabled",
         backlog_metric + ["--set", "compactor.enabled=false"],
         backlog_message, backlog_allowed),
    ]
    for label, extra, refusal_message, allowed in refusals:
        problem = check_render_refusal(
            "deploy/helm/siglake", base + extra, refusal_message, allowed
        )
        if problem:
            failed = True
            print(f"FAIL [{label}] {problem}", file=sys.stderr)
        else:
            print(f"ok   [{label}] render refused", flush=True)

    # Values under a DISABLED tier are inert: the tier renders no workload, so
    # there is no pod for the setting to be wrong on and nothing to refuse.
    # These do not enter the full object checks because the workload the checks
    # read does not exist.
    inert = [
        ("jobs-in-memory-query-disabled",
         ["--set", "query.enabled=false",
          "--set", "query.jobs.persistent=false",
          "--set", "query.replicas=2",
          "--set", "keda.enabled=true",
          "--set", "keda.query.enabled=true",
          "--set", "keda.query.maxReplicas=8"]),
        # #4303: each refused OIDC shape, under the tier that is switched off.
        ("oidc-partial-query-disabled",
         ["--set", "query.enabled=false",
          "--set", "query.oidc.issuer=https://idp.example/realms/siglake",
          "--set", "query.oidc.tenantClaim=org_id"]),
        ("oidc-partial-ingester-disabled",
         ["--set", "ingester.enabled=false",
          "--set", "ingester.oidc.audience=siglake-ingest",
          "--set", "ingester.oidc.tenantClaim=org_id"]),
    ]
    for label, extra in inert:
        try:
            docs = render("deploy/helm/siglake", base + extra)
        except subprocess.CalledProcessError as error:
            failed = True
            print(
                f"FAIL [{label}] helm template failed:\n{error.stderr}",
                file=sys.stderr,
            )
            continue
        # The disabled tier's values must stay off the OTHER tier's pods too:
        # both binaries read the same SIGLAKE_OIDC_* names, so a block leaking
        # across would turn OIDC on for a tier that never asked for it.
        problems = check_oidc_env(docs, {})
        if problems:
            failed = True
            for p in problems:
                print(f"FAIL [{label}] {p}", file=sys.stderr)
        else:
            print(f"ok   [{label}] render allowed", flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
