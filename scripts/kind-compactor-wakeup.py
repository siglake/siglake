#!/usr/bin/env python3
"""Isolated operator-managed kind qualification for #6012/#6470.

Only kind-round.sh's validated COMPACTOR_WAKEUP_CAPTURE opt-in invokes this.
No existing cluster is reused. Every kubectl/helm command uses a private
kubeconfig; a failed proof keeps its evidence and still removes our cluster.
"""
from __future__ import annotations

import importlib.util
import datetime
import json
import math
import os
from pathlib import Path
import re
import signal
import shutil
import subprocess
import tempfile
import threading
import time
import urllib.parse
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parent.parent
BATCH = 500
PROM_RELEASE = "kube-prometheus-stack"
PROM_VERSION = "77.11.1"  # Same pin as the ordinary kind round.
SEALED_DEPTH = "siglake_wal_segments_sealed"
SEALED_AGE = "siglake_wal_segments_sealed_sample_age_seconds"
SAMPLE_MAX_AGE = 120
OBSERVER_MAX_BRACKET_SECONDS = 1


def metric_series(text: str, name: str) -> list[dict]:
    """Parse one metric family from a Prometheus text exposition."""
    rows = []
    pattern = re.compile(
        rf"^{re.escape(name)}(?P<labels>\{{[^}}]*\}})?[ \t]+"
        r"(?P<value>[-+]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][-+]?[0-9]+)?|[-+]?Inf|NaN)"
        r"(?:[ \t]+[0-9]+)?$"
    )
    for line in text.splitlines():
        match = pattern.fullmatch(line.strip())
        if not match:
            continue
        try:
            value = float(match.group("value"))
        except ValueError:
            continue
        rows.append({"labels": match.group("labels") or "", "value": value})
    return rows


def direct_signal(direct: dict, allowance: float) -> tuple[float, float] | None:
    """Return total depth and maximum age for one complete direct scrape."""
    depth = direct.get("published_depth") or []
    age = direct.get("sample_age") or []
    depth_by_labels = {row.get("labels"): row.get("value") for row in depth}
    age_by_labels = {row.get("labels"): row.get("value") for row in age}
    if (not depth_by_labels or len(depth_by_labels) != len(depth) or
            set(depth_by_labels) != set(age_by_labels) or len(age_by_labels) != len(age)):
        return None
    values = [*depth_by_labels.values(), *age_by_labels.values()]
    if not all(isinstance(value, (int, float)) and not isinstance(value, bool) and
               value >= 0 and math.isfinite(value) for value in values):
        return None
    maximum_age = max(age_by_labels.values())
    if maximum_age > allowance:
        return None
    return sum(depth_by_labels.values()), maximum_age


def parked_state(state: dict) -> bool:
    return state.get("replicas") == 0 and state.get("pods") == []


def direct_observation_is_positive(before: dict, observation: dict,
                                   allowance: float = SAMPLE_MAX_AGE,
                                   max_bracket: float = OBSERVER_MAX_BRACKET_SECONDS) -> bool:
    """The direct signal follows a nearby parked observation and has a following state."""
    after = observation.get("state") or {}
    direct = observation.get("direct") or {}
    signal = direct_signal(direct, allowance)
    try:
        bracket = after["at"] - before["at"]
        ordered = before["at"] <= direct["time"] <= after["at"]
    except (KeyError, TypeError):
        return False
    return (parked_state(before) and isinstance(after.get("replicas"), int) and
            isinstance(after.get("pods"), list) and ordered and
            0 <= bracket <= max_bracket and signal is not None and signal[0] > 0)


def prom_signal_is_positive(queries: dict, allowance: float = SAMPLE_MAX_AGE) -> bool:
    """Apply the operator expression's depth/age arithmetic to captured queries."""
    def rows(name):
        response = (queries.get(name) or {}).get("response") or {}
        data = response.get("data") or {}
        if response.get("status") != "success" or data.get("resultType") != "vector":
            return []
        return data.get("result") or []

    def per_pod(values, combine):
        result = {}
        for row in values:
            pod = (row.get("metric") or {}).get("pod")
            try:
                value = float(row["value"][1])
            except (KeyError, IndexError, TypeError, ValueError):
                return {}
            if not pod or not math.isfinite(value):
                return {}
            result[pod] = result.get(pod, 0.0) + value if combine == "sum" else max(result.get(pod, value), value)
        return result

    depth = per_pod(rows("published_depth"), "sum")
    age = per_pod(rows("sample_age"), "max")
    fresh = {pod: value for pod, value in depth.items() if age.get(pod, allowance + 1) <= allowance}
    operator = rows("operator_expression")
    if not fresh or min(fresh.values()) <= 0 or len(operator) != 1:
        return False
    try:
        operator_value = float(operator[0]["value"][1])
    except (KeyError, IndexError, TypeError, ValueError):
        return False
    expected = sum(fresh.values()) / len(fresh)
    return math.isfinite(operator_value) and operator_value > 0 and math.isclose(operator_value, expected, rel_tol=1e-9)


def activation_expression(cluster: str) -> str:
    ing = f'namespace="default",app_kubernetes_io_instance="{cluster}",app_kubernetes_io_component="ingester"'
    comp = f'namespace="default",app_kubernetes_io_instance="{cluster}",app_kubernetes_io_component="compactor"'
    return (f"avg(sum by (pod) (siglake_wal_segments_sealed{{{ing}}}) and on (pod) "
            f"(max by (pod) (siglake_wal_segments_sealed_sample_age_seconds{{{ing}}}) <= 120)) "
            f"or avg(sum by (pod) (siglake_compactor_sealed_pending{{{comp}}}))")


def cluster_manifest(name: str, image: str) -> dict:
    return {"apiVersion": "siglake.limnion.ai/v1alpha1", "kind": "SiglakeCluster",
            "metadata": {"name": name, "namespace": "default"}, "spec": {
                "image": image, "warehouseUrl": "s3://siglake-warehouse/warehouse",
                "catalogUri": "postgres://siglake:siglake@postgres/siglake", "awsRegion": "us-east-1",
                "autoscaling": {"ewmaHalfLifeSecs": 5,
                    "ingester": {"min": 1, "max": 1, "target": 1000},
                    "compactor": {"min": 0, "max": 2, "target": 5},
                    "query": {"min": 1, "max": 1, "target": 4}},
                "storage": {"walAccessMode": "ReadWriteOnce", "walSize": "1Gi",
                            "walStorageClassName": "standard"},
                "extraEnv": [{"name": k, "value": v} for k, v in {
                    "AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin",
                    "AWS_ENDPOINT_URL": "http://minio:9000", "SIGLAKE_WAL_SEALED_PUBLISH_SECS": "15",
                    "SIGLAKE_QUERY_RESULT_CACHE": "off"}.items()]}}


def rejected_log_records(response: dict) -> int:
    """Rejected-record count carried by an OTLP/HTTP logs reply.

    Siglake sends `"partialSuccess": null` instead of omitting the field, and
    `dict.get(key, default)` returns that null rather than the default, so each
    lookup here falls back on a missing key and on an explicit null alike.
    """
    partial = response.get("partialSuccess") or response.get("partial_success") or {}
    rejected = partial.get("rejectedLogRecords") or partial.get("rejected_log_records") or 0
    return int(rejected)


def pod_monitor(name: str) -> dict:
    # PodMonitor avoids scraping the query pod twice via its two Services.
    return {"apiVersion": "monitoring.coreos.com/v1", "kind": "PodMonitor",
            "metadata": {"name": "wakeup", "namespace": "default", "labels": {"release": PROM_RELEASE}},
            "spec": {"selector": {"matchLabels": {"app.kubernetes.io/instance": name}},
                     "podTargetLabels": ["app.kubernetes.io/instance", "app.kubernetes.io/component"],
                     "podMetricsEndpoints": [{"port": "metrics", "interval": "2s", "scrapeTimeout": "2s"}]}}


class Round:
    def __init__(self):
        self.name = os.environ.get("COMPACTOR_WAKEUP_CLUSTER", "")
        self.kind = os.environ.get("KIND_CLUSTER_NAME", "siglake")
        for value in (self.name, self.kind):
            if not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", value):
                raise ValueError("cluster names must be Kubernetes DNS labels")
        self.kind = self.kind[:54] + "-" + uuid.uuid4().hex[:8]
        self.out = Path(os.environ.get("RESULTS_DIR", str(ROOT / "results"))).resolve() / "compactor-wakeup"
        self.out.mkdir(parents=True, exist_ok=False)
        self.tmp = tempfile.TemporaryDirectory(prefix="siglake-wakeup-")
        self.env = dict(os.environ, KUBECONFIG=str(Path(self.tmp.name) / "kubeconfig"))
        self.pfs = []
        self.owned_id = None
        self.commands = open(self.out / "commands.log", "w", encoding="utf-8")
        self.sha = os.environ.get("SIGLAKE_SOURCE_COMMIT") or self.run("git", "rev-parse", "HEAD").strip()
        if not re.fullmatch(r"[0-9a-f]{40}", self.sha):
            raise ValueError("full source commit required")
        self.image = f"siglake-wakeup:{self.sha[:12]}"
        self.operator_image = f"siglake-wakeup-operator:{self.sha[:12]}"
        self.cr = cluster_manifest(self.name, self.image)
        self.doc = {"schema_version": 4, "revisions": {"repository_commit": self.sha,
                    "repository_commit_source": "siglake_source_commit_env" if os.environ.get("SIGLAKE_SOURCE_COMMIT") else "git_rev_parse_head"},
                    "settings": {"cluster": self.name, "namespace": "default", "kind_cluster": self.kind,
                                 "sample_max_age_seconds": SAMPLE_MAX_AGE,
                                 "observer_max_bracket_seconds": OBSERVER_MAX_BRACKET_SECONDS,
                                 "ingest_batch": BATCH, "pods_while_parked": []},
                    "replica_history": [], "queries": {}, "acceptance": {}, "grade": "running"}
        self.save()

    def save(self):
        (self.out / "capture.json").write_text(json.dumps(self.doc, indent=2) + "\n")

    def run(self, *args, input=None, timeout=120):
        print("==>", " ".join(map(str, args)), flush=True)
        self.commands.write("$ " + " ".join(map(str, args)) + "\n")
        self.commands.flush()
        result = subprocess.run(list(map(str, args)), input=input, text=True, stdout=subprocess.PIPE,
                                stderr=self.commands, cwd=ROOT, env=self.env, timeout=timeout)
        if result.returncode:
            raise RuntimeError(f"command exited {result.returncode}: {args[0:3]}; see commands.log")
        return result.stdout

    def kube(self, *args, **kwargs):
        return self.run("kubectl", "--context", f"kind-{self.kind}", *args, **kwargs)

    def apply(self, manifest, filename):
        text = json.dumps(manifest, indent=2)
        (self.out / filename).write_text(text + "\n")
        self.kube("apply", "-f", "-", input=text)

    def wait(self, label, predicate, seconds=300, interval=2):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            value = predicate()
            if value:
                return value
            time.sleep(interval)
        raise RuntimeError(f"timed out after {seconds}s: {label}")

    def forward(self, resource, port, namespace="default"):
        log = open(self.out / f"port-forward-{namespace}-{resource.replace('/', '-')}-{port}.log", "w+")
        proc = subprocess.Popen(["kubectl", "--context", f"kind-{self.kind}", "-n", namespace,
                                 "port-forward", resource, f":{port}", "--address", "127.0.0.1"],
                                stdout=log, stderr=subprocess.STDOUT, env=self.env)
        self.pfs.append((proc, log))
        def ready():
            if proc.poll() is not None:
                raise RuntimeError(f"port-forward failed: {resource}")
            log.seek(0)
            match = re.search(r"Forwarding from 127\.0\.0\.1:(\d+)", log.read())
            return f"http://127.0.0.1:{match[1]}" if match else None
        return self.wait("port-forward " + resource, ready, 30, .2)

    def http(self, url, payload=None):
        request = urllib.request.Request(url, data=json.dumps(payload).encode() if payload is not None else None,
                                         headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)

    def text(self, url):
        with urllib.request.urlopen(url, timeout=30) as response:
            return response.read().decode("utf-8")

    def prom(self, expression):
        at = time.time()
        response = self.http(self.prom_url + "/api/v1/query?" + urllib.parse.urlencode({"query": expression, "time": at}))
        if response.get("status") != "success":
            raise RuntimeError("Prometheus query failed")
        return {"expression": expression, "time": at, "response": response}

    def sample(self, phase):
        row = self.compactor_state(phase)
        self.doc["replica_history"].append(row)
        self.save()
        return row

    def compactor_state(self, phase):
        selector = f"app.kubernetes.io/instance={self.name},app.kubernetes.io/component=compactor"
        items = json.loads(self.kube("get", "deployment,pods", "-l", selector, "-o", "json"))["items"]
        deployments = [item for item in items if item["kind"] == "Deployment"]
        if len(deployments) != 1:
            raise RuntimeError(f"expected one compactor Deployment, found {len(deployments)}")
        return {"at": time.time(), "phase": phase, "replicas": deployments[0]["spec"]["replicas"],
                "pods": [item["metadata"]["name"] for item in items if item["kind"] == "Pod"]}

    def record_observation(self, observation):
        with open(self.out / "positive-observations.jsonl", "a", encoding="utf-8") as stream:
            stream.write(json.dumps(observation, separators=(",", ":")) + "\n")

    def direct_scrape(self):
        sampled_at = time.time()
        text = self.text(self.ingester_metrics_url + "/metrics")
        return {
            "time": sampled_at,
            "source": "operator-managed ingester /metrics",
            "published_depth": metric_series(text, SEALED_DEPTH),
            "sample_age": metric_series(text, SEALED_AGE),
        }

    def direct_observation(self, phase):
        observation = {"kind": "direct-signal", "direct": self.direct_scrape(),
                       "state": self.compactor_state(phase)}
        self.record_observation(observation)
        return observation

    def prometheus_observation(self, selector, expression):
        queries = {
            "published_depth": self.prom(f"{SEALED_DEPTH}{{{selector}}}"),
            "sample_age": self.prom(f"{SEALED_AGE}{{{selector}}}"),
            "operator_expression": self.prom(expression),
        }
        observation = {"kind": "prometheus-signal", "queries": queries,
                       "state": self.compactor_state("waiting-for-prometheus")}
        self.record_observation(observation)
        return observation

    def ledger(self):
        sql = "SELECT COALESCE(json_agg(t), '[]') FROM (SELECT id, rows, status, committed_at_ms FROM wal_segments WHERE tenant='default' ORDER BY id) t"
        return json.loads(self.kube("exec", "postgres-0", "--", "psql", "-U", "siglake", "-d", "siglake", "-Atc", sql))

    def ingest(self, prefix):
        payload = {"resourceLogs": [{"resource": {"attributes": [{"key": "host.name", "value": {"stringValue": f"{prefix}-{i:04d}"}}]},
                    "scopeLogs": [{"logRecords": [{"body": {"stringValue": f"wakeup {prefix} event={i}"}}]}]} for i in range(BATCH)]}
        response = self.http(self.ingest_url + "/v1/logs", payload)
        (self.out / f"ingest-{prefix}.json").write_text(json.dumps({"request": payload, "response": response}, indent=2))
        if rejected_log_records(response):
            raise RuntimeError("ingest partially rejected")

    def query(self, prefix):
        return self.http(self.query_url + "/api/v1/sql", {"query": f"SELECT host FROM events WHERE host LIKE '{prefix}-%' ORDER BY host", "format": "records"})

    def bootstrap(self):
        for tool in ("kind", "kubectl", "helm", "docker"):
            if shutil.which(tool) is None:
                raise RuntimeError("missing required tool: " + tool)
        if self.kind in self.run("kind", "get", "clusters").splitlines():
            raise RuntimeError("refusing to reuse an existing kind cluster")
        # No mapped host ports: independent rounds and local release burns can coexist.
        try:
            self.run("kind", "create", "cluster", "--name", self.kind, "--kubeconfig", self.env["KUBECONFIG"], "--wait", "120s", timeout=240)
        finally:
            # Record a control plane created by THIS attempt, even if readiness failed.
            raw = self.run("docker", "ps", "-a", "--no-trunc", "--filter", f"label=io.x-k8s.kind.cluster={self.kind}",
                           "--filter", "label=io.x-k8s.kind.role=control-plane", "--format", "{{.ID}}").splitlines()
            if len(raw) == 1:
                self.owned_id = raw[0]
        if not self.owned_id:
            raise RuntimeError("could not identify owned control plane")
        for image, dockerfile in [(self.image, "deploy/Dockerfile"), (self.operator_image, "deploy/Dockerfile.operator")]:
            self.run("docker", "build", "--build-arg", f"SIGLAKE_GIT_SHA={self.sha}", "-t", image, "-f", dockerfile, ".", timeout=7200)
            self.run("kind", "load", "docker-image", image, "--name", self.kind, timeout=300)
        self.doc["build_versions"] = {}
        for image in (self.image, self.operator_image):
            version = self.run("docker", "run", "--rm", image, "--version").strip()
            if self.sha not in version:
                raise RuntimeError("built image does not identify the pinned source: " + image)
            self.doc["build_versions"][image] = version
        self.doc["images"] = {image: json.loads(self.run("docker", "image", "inspect", image))[0]["Id"] for image in (self.image, self.operator_image)}
        self.save()
        for manifest in ("postgres", "minio"):
            self.kube("apply", "-f", ROOT / f"deploy/kind/manifests/{manifest}.yaml")
        self.kube("rollout", "status", "statefulset/postgres", "--timeout=180s", timeout=200)
        self.kube("rollout", "status", "deployment/minio", "--timeout=180s", timeout=200)
        self.kube("wait", "--for=condition=complete", "job/minio-bucket-init", "--timeout=120s", timeout=140)
        self.run("helm", "repo", "add", "prometheus-community", "https://prometheus-community.github.io/helm-charts", "--force-update")
        self.run("helm", "repo", "update", "prometheus-community")
        self.run("helm", "upgrade", "--install", PROM_RELEASE, "prometheus-community/kube-prometheus-stack",
                 "--version", PROM_VERSION, "--namespace", "monitoring", "--create-namespace", "--set", "grafana.enabled=false",
                 "--set", "alertmanager.enabled=false", "--wait", "--timeout", "10m", timeout=650)
        self.run("helm", "upgrade", "--install", "wakeup-operator", "deploy/helm/siglake-operator",
                 "--set", "image.repository=siglake-wakeup-operator", "--set-string", f"image.tag={self.sha[:12]}",
                 "--set-string", f"prometheus.url=http://{PROM_RELEASE}-prometheus.monitoring.svc.cluster.local:9090",
                 "--wait", "--timeout", "5m", timeout=330)
        self.apply(pod_monitor(self.name), "pod-monitor.json")
        self.apply(self.cr, "cluster.json")
        # The CR does not exist at bootstrap: wait for its rendered resources before rollout status.
        def rendered():
            deployments = json.loads(self.kube("get", "deployments", "-o", "json"))["items"]
            return {self.name + "-ingester", self.name + "-compactor"} <= {d["metadata"]["name"] for d in deployments}
        self.wait("operator-rendered deployments", rendered)
        self.kube("rollout", "status", "deployment/" + self.name + "-ingester", "--timeout=300s", timeout=320)
        self.kube("rollout", "status", "statefulset/" + self.name + "-query", "--timeout=300s", timeout=320)
        self.prom_url = self.forward("svc/" + PROM_RELEASE + "-prometheus", 9090, "monitoring")
        self.ingest_url = self.forward("svc/" + self.name + "-ingester", 8088)
        self.ingester_metrics_url = self.forward("svc/" + self.name + "-ingester", 9100)
        self.query_url = self.forward("svc/" + self.name + "-query", 8089)
        self.save()

    def park(self, phase):
        def parked():
            row = self.sample(phase)
            return row if row["replicas"] == 0 and not row["pods"] else None
        return self.wait("compactor park", parked, 900)

    def exercise(self):
        self.ingest("initial")
        def initial_committed():
            rows = self.ledger()
            return rows if sum(r["rows"] for r in rows if r["status"] == "committed") == BATCH else None
        before = self.wait("initial batch committed", initial_committed, 300)
        self.doc["acceptance"]["initial_ledger"] = before
        self.park("parking")
        expr = activation_expression(self.name)
        def idle():
            q = self.prom(expr)
            rows = q["response"]["data"]["result"]
            return q if len(rows) == 1 and float(rows[0]["value"][1]) == 0 else None
        self.doc["queries"]["parked_operator_expression"] = self.wait("zero activation control", idle, 120)
        self.sample("parked")
        selector = f'namespace="default",app_kubernetes_io_instance="{self.name}",app_kubernetes_io_component="ingester"'
        # Observe the publisher directly before triggering ingest. Prometheus and
        # the operator read the same scrape: on run 176 the operator's 0→1 patch
        # landed 43 ms after the last zero sample, before the next observer poll.
        # The direct endpoint exposes the depth first, so it retains the last
        # zero, the positive reading and the following state without delaying
        # or changing the operator. The operator's timestamped 0→1 decision
        # closes the other side of that interval in the offline grade.
        def pre_trigger():
            observation = self.direct_observation("pre-trigger")
            signal = direct_signal(observation["direct"], SAMPLE_MAX_AGE)
            return observation if parked_state(observation["state"]) and signal and signal[0] == 0 else None
        baseline = self.wait("fresh direct zero before trigger", pre_trigger, 120, .05)
        self.doc["acceptance"]["positive_observer"] = {"pre_trigger": baseline}
        self.doc["acceptance"]["positive_started_at"] = time.time()
        self.save()
        ingest_error = []
        def drive_ingest():
            try:
                self.ingest("wake")
            except Exception as exc:
                ingest_error.append(exc)
        ingest_thread = threading.Thread(target=drive_ingest, name="wakeup-ingest", daemon=True)
        ingest_thread.start()

        deadline = time.monotonic() + 300
        previous = baseline["state"]
        state_sampled_at = time.monotonic()
        positive_observation = None
        while time.monotonic() < deadline:
            direct = self.direct_scrape()
            signal = direct_signal(direct, SAMPLE_MAX_AGE)
            if signal is not None and signal[0] > 0:
                observation = {"kind": "direct-signal", "direct": direct,
                               "state": self.compactor_state("after-direct-signal")}
                self.record_observation(observation)
            elif time.monotonic() - state_sampled_at >= .2:
                observation = {"kind": "direct-signal", "direct": direct,
                               "state": self.compactor_state("waiting-for-direct-signal")}
                self.record_observation(observation)
                state_sampled_at = time.monotonic()
            else:
                self.record_observation({"kind": "direct-signal", "direct": direct})
                time.sleep(.01)
                continue
            if direct_observation_is_positive(previous, observation):
                positive_observation = {"before": previous, "direct": observation["direct"],
                                        "after": observation["state"]}
                break
            if not parked_state(observation["state"]):
                raise RuntimeError("compactor woke before the direct positive signal was retained")
            if ingest_error:
                raise ingest_error[0]
            previous = observation["state"]
            time.sleep(.01)
        ingest_thread.join(timeout=30)
        if ingest_thread.is_alive():
            raise RuntimeError("wake ingest did not finish within 30 seconds")
        if ingest_error:
            raise ingest_error[0]
        if positive_observation is None:
            raise RuntimeError("no fresh direct positive signal was retained at zero replicas")
        self.doc["acceptance"]["positive_observer"]["positive"] = positive_observation
        ingested = dict(positive_observation["before"], phase="ingested")
        self.doc["replica_history"].append(ingested)
        self.doc["acceptance"]["sealed_ledger"] = self.ledger()
        self.save()

        def positive_prometheus():
            observation = self.prometheus_observation(selector, expr)
            return observation if prom_signal_is_positive(observation["queries"]) else None
        prometheus = self.wait("matching positive Prometheus expression", positive_prometheus, 300, .05)
        self.doc["queries"].update(prometheus["queries"])
        def woken():
            row = self.sample("waking")
            if row["replicas"] > 0:
                row["phase"] = "woken"
                self.doc["replica_history"][-1] = row
                self.save()
                return row
            return None
        self.wait("operator wake", woken, 300)
        before_ids = {r["id"] for r in before}
        def committed():
            rows = [r for r in self.ledger() if r["id"] not in before_ids]
            return rows if rows and all(r["status"] == "committed" for r in rows) and sum(r["rows"] for r in rows) == BATCH else None
        self.doc["acceptance"]["committed_ledger"] = self.wait("new segments committed to Iceberg", committed, 300)
        self.doc["acceptance"]["committed_at"] = time.time()
        def exact():
            response = self.query("wake")
            return response if [r["host"] for r in response.get("rows", [])] == [f"wake-{i:04d}" for i in range(BATCH)] else None
        self.doc["acceptance"]["query_response"] = self.wait("exact newly ingested row IDs", exact, 120)
        # Wait for old compactor scrape series to disappear before disabling the independent publisher.
        self.park("negative-parking")
        comp_selector = f'namespace="default",app_kubernetes_io_instance="{self.name}",app_kubernetes_io_component="compactor"'
        def no_compactor_series():
            q = self.prom(f"siglake_compactor_sealed_pending{{{comp_selector}}}")
            return q if not q["response"]["data"]["result"] else None
        self.doc["acceptance"]["absent_compactor_series"] = self.wait("compactor series stale", no_compactor_series, 360)
        # Retain a complete ordinary reconcile while the compactor series is absent.
        # Use the bounded polling helper instead of assuming a sleep is evidence.
        def ordinary_decision():
            logs = self.kube("logs", "deployment/wakeup-operator-siglake-operator", "--timestamps=true")
            start = self.doc["acceptance"]["absent_compactor_series"]["time"]
            for line in logs.splitlines():
                if "reconcile decision" not in line:
                    continue
                try:
                    at = datetime.datetime.fromisoformat(line.split()[0].replace("Z", "+00:00")).timestamp()
                except (ValueError, IndexError):
                    continue
                if at >= start and "ingester_rps_per_pod: Some(" in line and "query_in_flight_per_pod: Some(" in line:
                    return True
            return False
        self.wait("ordinary unaffected-tier decision with no compactor series", ordinary_decision, 120)
        self.doc["acceptance"]["unaffected_until"] = time.time()
        self.doc["acceptance"]["negative_parked"] = self.sample("negative-parked")
        self.doc["acceptance"]["negative_started_at"] = time.time()
        for env in self.cr["spec"]["extraEnv"]:
            if env["name"] == "SIGLAKE_WAL_SEALED_PUBLISH_SECS":
                env["value"] = "0"
        self.apply(self.cr, "cluster-publisher-disabled.json")
        # Stop only the publisher through the existing supported knob, not the operator or catalog.
        self.doc["acceptance"]["publisher_disabled"] = self.cr
        def missing():
            q = self.prom(expr)
            if not q["response"]["data"]["result"]:
                return q
            return None
        self.doc["acceptance"]["missing_activation"] = self.wait("publisher absent activation", missing, 360, .25)
        self.doc["acceptance"]["negative_woken"] = self.wait("missing-publisher fallback to one", lambda: self.sample("negative-woken") if self.sample("negative-waking")["replicas"] == 1 else None, 180)
        self.kube("rollout", "status", "deployment/" + self.name + "-ingester", "--timeout=180s", timeout=200)
        self.kube("rollout", "status", "statefulset/" + self.name + "-query", "--timeout=180s", timeout=200)
        logs = self.kube("logs", "deployment/wakeup-operator-siglake-operator", "--timestamps=true")
        (self.out / "operator.log").write_text(logs)
        self.doc["acceptance"]["operator_log"] = logs
        self.doc["acceptance"]["workloads"] = json.loads(self.kube("get", "deployment,sts,pods", "-l", f"app.kubernetes.io/instance={self.name}", "-o", "json"))
        self.save()

    def cleanup(self):
        for proc, log in self.pfs:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
            log.close()
        if self.owned_id:
            try:
                (self.out / "final-objects.json").write_text(self.kube("get", "siglakeclusters,deployments,sts,pods,events", "-A", "-o", "json"))
                (self.out / "operator-final.log").write_text(self.kube("logs", "deployment/wakeup-operator-siglake-operator", "--timestamps=true"))
                pods = json.loads(self.kube("get", "pods", "-l", f"app.kubernetes.io/instance={self.name}", "-o", "json"))["items"]
                for pod in pods:
                    name = pod["metadata"]["name"]
                    (self.out / f"{name}.log").write_text(self.kube("logs", name, "--all-containers=true", "--timestamps=true"))
            except Exception as exc:
                self.doc["diagnostics_error"] = str(exc)
            ids = self.run("docker", "ps", "-a", "--no-trunc", "--filter", f"label=io.x-k8s.kind.cluster={self.kind}", "--format", "{{.ID}}").splitlines()
            if ids != [self.owned_id]:
                raise RuntimeError("cluster identity changed; refusing teardown")
            self.run("kind", "delete", "cluster", "--name", self.kind, timeout=180)
            if self.kind in self.run("kind", "get", "clusters").splitlines():
                raise RuntimeError("cluster teardown not verified")
        self.doc["cleanup"] = {"verified": True, "owned_control_plane": self.owned_id}
        self.save()
        self.commands.close()
        self.tmp.cleanup()


def main():
    def interrupt(signum, frame):
        raise RuntimeError(f"interrupted by signal {signum}")
    signal.signal(signal.SIGTERM, interrupt)
    signal.signal(signal.SIGINT, interrupt)
    round_ = Round()
    code = 1
    try:
        round_.bootstrap()
        round_.exercise()
        spec = importlib.util.spec_from_file_location("wakeup_grader", ROOT / "scripts/grade-kind-compactor-wakeup.py")
        grader = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(grader)
        evidence = grader.grade_acceptance(round_.doc)
        round_.doc["evidence"] = evidence
        round_.doc["grade"] = "verified"
        code = 0
    except Exception as exc:
        round_.doc["grade"] = "unverified"
        round_.doc["failure"] = str(exc)
        print("UNVERIFIED:", exc, flush=True)
    finally:
        try:
            round_.cleanup()
        except Exception as exc:
            round_.doc["grade"] = "unverified"
            round_.doc["cleanup"] = {"verified": False, "error": str(exc)}
            round_.save()
            code = 1
    return code


if __name__ == "__main__":
    raise SystemExit(main())
