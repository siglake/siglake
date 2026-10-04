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
import os
from pathlib import Path
import re
import signal
import shutil
import subprocess
import tempfile
import time
import urllib.parse
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parent.parent
BATCH = 500
PROM_RELEASE = "kube-prometheus-stack"
PROM_VERSION = "77.11.1"  # Same pin as the ordinary kind round.


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
        self.doc = {"schema_version": 3, "revisions": {"repository_commit": self.sha,
                    "repository_commit_source": "siglake_source_commit_env" if os.environ.get("SIGLAKE_SOURCE_COMMIT") else "git_rev_parse_head"},
                    "settings": {"cluster": self.name, "namespace": "default", "kind_cluster": self.kind, "sample_max_age_seconds": 120,
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

    def prom(self, expression):
        at = time.time()
        response = self.http(self.prom_url + "/api/v1/query?" + urllib.parse.urlencode({"query": expression, "time": at}))
        if response.get("status") != "success":
            raise RuntimeError("Prometheus query failed")
        return {"expression": expression, "time": at, "response": response}

    def sample(self, phase):
        dep = json.loads(self.kube("get", "deployment", self.name + "-compactor", "-o", "json"))
        pods = json.loads(self.kube("get", "pods", "-l", f"app.kubernetes.io/instance={self.name},app.kubernetes.io/component=compactor", "-o", "json"))
        row = {"at": time.time(), "phase": phase, "replicas": dep["spec"]["replicas"],
               "pods": [p["metadata"]["name"] for p in pods["items"]]}
        self.doc["replica_history"].append(row)
        self.save()
        return row

    def ledger(self):
        sql = "SELECT COALESCE(json_agg(t), '[]') FROM (SELECT id, rows, status, committed_at_ms FROM wal_segments WHERE tenant='default' ORDER BY id) t"
        return json.loads(self.kube("exec", "postgres-0", "--", "psql", "-U", "siglake", "-d", "siglake", "-Atc", sql))

    def ingest(self, prefix):
        payload = {"resourceLogs": [{"resource": {"attributes": [{"key": "host.name", "value": {"stringValue": f"{prefix}-{i:04d}"}}]},
                    "scopeLogs": [{"logRecords": [{"body": {"stringValue": f"wakeup {prefix} event={i}"}}]}]} for i in range(BATCH)]}
        response = self.http(self.ingest_url + "/v1/logs", payload)
        (self.out / f"ingest-{prefix}.json").write_text(json.dumps({"request": payload, "response": response}, indent=2))
        partial = response.get("partialSuccess", response.get("partial_success", {}))
        if int(partial.get("rejectedLogRecords", partial.get("rejected_log_records", 0))):
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
        self.doc["acceptance"]["positive_started_at"] = time.time()
        self.ingest("wake")
        selector = f'namespace="default",app_kubernetes_io_instance="{self.name}",app_kubernetes_io_component="ingester"'
        def positive():
            before_row = self.sample("waiting-for-signal")
            if before_row["replicas"] != 0 or before_row["pods"]:
                raise RuntimeError("compactor woke before positive signal retained")
            queries = {"published_depth": self.prom(f"siglake_wal_segments_sealed{{{selector}}}"),
                       "sample_age": self.prom(f"siglake_wal_segments_sealed_sample_age_seconds{{{selector}}}"),
                       "operator_expression": self.prom(expr)}
            values = queries["operator_expression"]["response"]["data"]["result"]
            if len(values) != 1 or float(values[0]["value"][1]) <= 0:
                return None
            ledger = self.ledger()
            row = self.sample("ingested")
            if row["replicas"] != 0 or row["pods"]:
                raise RuntimeError("positive signal was not retained while compactor absent")
            self.doc["queries"].update(queries)
            self.doc["acceptance"]["sealed_ledger"] = ledger
            return row
        self.wait("fresh positive signal at zero", positive, 300, .25)
        self.wait("operator wake", lambda: self.sample("woken") if self.sample("waking")["replicas"] > 0 else None, 300)
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
