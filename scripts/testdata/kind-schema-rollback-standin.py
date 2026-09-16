#!/usr/bin/env python3
"""PATH stand-ins for the offline schema-rollback probe check."""

from __future__ import annotations

import json
import os
import pathlib
import signal
import sys
import time


STATE_DIR = pathlib.Path(os.environ["SCHEMA_ROLLBACK_STANDIN_STATE"])
CALLS = STATE_DIR / "calls.log"
GROUP_BEHAVIOR = os.environ.get("SCHEMA_ROLLBACK_STANDIN_GROUP_BEHAVIOR", "current")
ROLLOUT_BEHAVIOR = os.environ.get("SCHEMA_ROLLBACK_STANDIN_ROLLOUT_BEHAVIOR", "current")


def load(name: str, default):
    path = STATE_DIR / name
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def save(name: str, value) -> None:
    (STATE_DIR / name).write_text(json.dumps(value), encoding="utf-8")


def record(tool: str, args: list[str]) -> None:
    with CALLS.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps([tool, *args]) + "\n")


def option(args: list[str], name: str) -> str | None:
    try:
        return args[args.index(name) + 1]
    except (ValueError, IndexError):
        return None


def image_job(image: str, uid: str) -> dict:
    suffix = "aaaaaaaa" if image == "siglake:kind" else "bbbbbbbb"
    return {
        "metadata": {
            "name": f"rollback-migrate-schema-v1-{suffix}",
            "uid": uid,
            "creationTimestamp": "2026-09-09T00:00:00Z",
        },
        "spec": {"template": {"spec": {"containers": [{"image": image}]}}},
        "status": {
            "succeeded": 1,
            "completionTime": "2026-09-09T00:00:01Z",
        },
    }


def new_operator(image: str) -> dict:
    return {
        "image": image,
        "generation": 0,
        "observed_generation": 0,
        "template_image": None,
        "pod_image": None,
        "workload_generation": 0,
        "rollout_complete": False,
        "remaining": 0,
        "held": 0,
        "jobs": {},
    }


def complete_rollout(operator: dict) -> None:
    operator["observed_generation"] = operator["generation"]
    operator["template_image"] = operator["image"]
    operator["pod_image"] = operator["image"]
    operator["workload_generation"] = operator["workload_generation"] + 1
    operator["rollout_complete"] = True
    operator["remaining"] = 0


def advance_rollout(operator: dict) -> None:
    remaining = operator.get("remaining", 0)
    if remaining > 0:
        operator["remaining"] = remaining - 1
        if operator["remaining"] == 0:
            complete_rollout(operator)
        save("operator.json", operator)


def docker(args: list[str]) -> int:
    if args[:2] == ["image", "inspect"]:
        image = args[-1]
        print("sha256:image-a" if image == "siglake:kind" else "sha256:" + image.replace(":", "-"))
    return 0


def kind(args: list[str]) -> int:
    return 0


def helm(args: list[str]) -> int:
    command = next((arg for arg in args if arg in {"status", "upgrade", "rollback"}), "")
    chart = load("chart.json", {"revision": 1, "image": "siglake:kind", "jobs": []})
    if command == "status":
        print(json.dumps({"version": chart["revision"]}))
    elif command == "rollback":
        chart["revision"] += 1
        chart["image"] = "siglake:kind"
        save("chart.json", chart)
    elif command == "upgrade" and any("siglake-operator" in arg for arg in args):
        # The operator install. Keep the address it was started with: the
        # cluster's only Prometheus is kube-prometheus-stack's Service, so the
        # log this stand-in serves below says whether the operator can read it.
        save(
            "operator-chart.json",
            {
                "prometheus_url": next(
                    (
                        arg.split("=", 1)[1]
                        for arg in args
                        if arg.startswith("prometheus.url=")
                    ),
                    "http://prometheus-server.monitoring.svc.cluster.local:80",
                )
            },
        )
    elif command == "upgrade":
        chart["revision"] += 1
        repository = next(
            (arg.split("=", 1)[1] for arg in args if arg.startswith("image.repository=")),
            "siglake",
        )
        tag = next(
            (arg.split("=", 1)[1] for arg in args if arg.startswith("image.tag=")),
            "kind-rollback-probe",
        )
        chart["image"] = f"{repository}:{tag}"
        chart["jobs"].append(chart["revision"])
        save("chart.json", chart)
    return 0


def chart_jobs() -> dict:
    chart = load("chart.json", {"revision": 1, "image": "siglake:kind", "jobs": []})
    items = []
    for revision in chart["jobs"]:
        items.append(
            {
                "metadata": {
                    "name": f"siglake-migrate-schema-{revision}",
                    "uid": f"chart-job-{revision}",
                    "creationTimestamp": f"2026-09-09T00:00:0{revision}Z",
                },
                "spec": {
                    "template": {
                        "spec": {"containers": [{"image": "siglake:kind-rollback-probe"}]}
                    }
                },
                "status": {
                    "succeeded": 1,
                    "completionTime": f"2026-09-09T00:00:1{revision}Z",
                },
            }
        )
    return {"items": items}


def chart_pods() -> dict:
    image = load("chart.json", {"image": "siglake:kind"})["image"]
    return {
        "items": [
            {
                "metadata": {
                    "name": f"siglake-{component}-0",
                    "labels": {"app.kubernetes.io/component": component},
                },
                "spec": {"containers": [{"image": image}]},
                "status": {"phase": "Running"},
            }
            for component in ("ingester", "compactor", "query")
        ]
    }


def apply_file(path: str) -> None:
    try:
        document = json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return
    if document.get("kind") != "SiglakeCluster":
        return
    image = document["spec"]["image"]
    operator = load("operator.json", new_operator(image))
    previous_pod_image = operator.get("pod_image")
    operator["image"] = image
    operator["generation"] += 1
    operator["rollout_complete"] = False
    if image == "siglake:kind-rollback-probe":
        operator["held"] = max(operator["held"], 1)
    if image not in operator["jobs"]:
        uid = "operator-a-1" if image == "siglake:kind" else "operator-b-1"
        operator["jobs"][image] = image_job(image, uid)
    if (
        ROLLOUT_BEHAVIOR == "stuck-revert"
        and image == "siglake:kind"
        and previous_pod_image == "siglake:kind-rollback-probe"
    ):
        # The controller has acknowledged the generation and updated the
        # workload templates, but the old B pods never roll. This proves that
        # generation convergence by itself is not accepted.
        operator["observed_generation"] = operator["generation"]
        operator["template_image"] = image
        operator["workload_generation"] += 1
        operator["remaining"] = 0
    elif ROLLOUT_BEHAVIOR.startswith("delay:"):
        operator["remaining"] = int(ROLLOUT_BEHAVIOR.split(":", 1)[1])
    else:
        complete_rollout(operator)
    save("operator.json", operator)


def operator_workloads(kind: str) -> dict:
    operator = load("operator.json", new_operator("siglake:kind"))
    components = ("ingester", "compactor") if kind == "Deployment" else ("query",)
    items = []
    for component in components:
        generation = operator.get("workload_generation", 0)
        complete = operator.get("rollout_complete", False)
        status = {
            "observedGeneration": generation,
            "updatedReplicas": 1 if complete else 0,
            "readyReplicas": 1 if complete else 0,
        }
        if kind == "Deployment":
            status["availableReplicas"] = 1 if complete else 0
        else:
            status.update(
                {
                    "currentReplicas": 1,
                    "currentRevision": f"{component}-{generation - (0 if complete else 1)}",
                    "updateRevision": f"{component}-{generation}",
                }
            )
        items.append(
            {
                "metadata": {
                    "name": f"rollback-{component}",
                    "generation": generation,
                    "labels": {
                        "app.kubernetes.io/component": component,
                        "app.kubernetes.io/instance": "rollback",
                        "app.kubernetes.io/managed-by": "siglake-operator",
                    },
                },
                "spec": {
                    "replicas": 1,
                    "template": {
                        "spec": {"containers": [{"image": operator.get("template_image")}]}
                    },
                },
                "status": status,
            }
        )
    return {"items": items}


def operator_pods() -> dict:
    operator = load("operator.json", new_operator("siglake:kind"))
    ready = operator.get("rollout_complete", False)
    items = [
        {
            "metadata": {
                "name": f"rollback-{component}-0",
                "labels": {
                    "app.kubernetes.io/component": component,
                    "app.kubernetes.io/instance": "rollback",
                    "app.kubernetes.io/managed-by": "siglake-operator",
                },
            },
            "spec": {"containers": [{"image": operator.get("pod_image")}]},
            "status": {
                "phase": "Running",
                "conditions": [
                    {"type": "Ready", "status": "True" if ready else "False"}
                ],
            },
        }
        for component in ("ingester", "compactor", "query")
    ]
    for image, job in operator["jobs"].items():
        items.append(
            {
                "metadata": {
                    "name": f"{job['metadata']['name']}-done",
                    "labels": {
                        "app.kubernetes.io/component": job["metadata"]["name"].split(
                            "rollback-", 1
                        )[-1],
                        "app.kubernetes.io/instance": "rollback",
                        "app.kubernetes.io/managed-by": "siglake-operator",
                    },
                },
                "spec": {"containers": [{"image": image}]},
                "status": {
                    "phase": "Succeeded",
                    "conditions": [{"type": "Ready", "status": "False"}],
                },
            }
        )
    return {"items": items}


def kubectl(args: list[str]) -> int:
    namespace = option(args, "-n") or "default"
    joined = " ".join(args)
    if "port-forward" in args:
        with (STATE_DIR / "port-forward-pids").open("a", encoding="utf-8") as handle:
            handle.write(f"{os.getpid()}\n")
        signal.signal(signal.SIGTERM, lambda *_: raise_exit())
        while True:
            time.sleep(1)
    if "logs" in args:
        url = load("operator-chart.json", {"prometheus_url": ""})["prometheus_url"]
        print(f"INFO siglake_operator: started with --prometheus-url {url}")
        if "kube-prometheus-stack-prometheus" not in url:
            # What the reconciler logs when it cannot reach Prometheus: it holds
            # every replica count and stays Ready while doing it.
            print(
                "WARN prometheus query failed; HOLDING replica counts rather "
                "than reading the outage as idleness"
            )
    elif "create namespace" in joined:
        print('{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"siglake-rollback"}}')
    elif "apply" in args:
        path = option(args, "-f")
        if path:
            apply_file(path)
    elif "annotate" in args and "siglakecluster" in args:
        operator = load("operator.json", new_operator("siglake:kind"))
        image = operator["image"]
        operator["jobs"][image] = image_job(image, "operator-a-2")
        save("operator.json", operator)
    elif "delete" in args and ("job" in args or "jobs" in args):
        operator = load("operator.json", new_operator("siglake:kind"))
        operator["jobs"].pop("siglake:kind", None)
        save("operator.json", operator)
    elif "get" in args and "jobs" in args:
        if namespace == "default":
            print(json.dumps(chart_jobs()))
        else:
            operator = load("operator.json", new_operator("siglake:kind"))
            print(json.dumps({"items": list(operator["jobs"].values())}))
    elif "get" in args and "deployments" in args:
        print(json.dumps(operator_workloads("Deployment")))
    elif "get" in args and "statefulsets" in args:
        print(json.dumps(operator_workloads("StatefulSet")))
    elif "get" in args and "pods" in args:
        print(json.dumps(operator_pods() if namespace != "default" else chart_pods()))
    elif "get" in args and "siglakecluster" in args:
        operator = load("operator.json", new_operator("siglake:kind"))
        if any("jsonpath=" in arg for arg in args):
            print("1", end="")
        else:
            advance_rollout(operator)
            operator = load("operator.json", operator)
            print(
                json.dumps(
                    {
                        "metadata": {"generation": operator["generation"]},
                        "spec": {"image": operator["image"], "schemaVersion": 1},
                        "status": {
                            "schemaVersion": 1,
                            "observedGeneration": operator["observed_generation"],
                        },
                    }
                )
            )
    return 0


def raise_exit() -> None:
    raise SystemExit(0)


def curl(args: list[str]) -> int:
    url = next((arg for arg in args if arg.startswith("http")), "")
    if url.endswith("/metrics"):
        held = load("operator.json", {"held": 0})["held"]
        print(f"siglake_operator_rollout_held_total {held}")
        return 0
    data = next((arg[1:] for arg in args if arg.startswith("@")), None)
    if data is None:
        return 0
    payload = json.loads(pathlib.Path(data).read_text(encoding="utf-8"))
    rows = load("rows.json", {"null": 10, "probe": 0})
    if "resourceLogs" in payload:
        count = len(payload["resourceLogs"])
        image = load("chart.json", {"image": "siglake:kind"})["image"]
        previous = dict(rows)
        rows["probe" if image.endswith("rollback-probe") else "null"] += count
        save("rows.json", rows)
        if GROUP_BEHAVIOR.startswith("delay:"):
            lag = load("group-lag.json", {"delayed_responses": 0})
            lag.update({"rows": previous, "remaining": int(GROUP_BEHAVIOR.split(":", 1)[1])})
            save("group-lag.json", lag)
        print("{}")
        return 0
    query = payload.get("query", "")
    total = rows["null"] + rows["probe"]
    if "count(*)" in query:
        result = {"rows": [{"n": total}]}
    elif "GROUP BY" in query:
        grouped_rows = rows
        if GROUP_BEHAVIOR.startswith("delay:"):
            lag = load("group-lag.json", {"remaining": 0, "delayed_responses": 0})
            if lag.get("remaining", 0) > 0:
                grouped_rows = lag["rows"]
                lag["remaining"] -= 1
                lag["delayed_responses"] = lag.get("delayed_responses", 0) + 1
                save("group-lag.json", lag)
        if "rollback_probe" in query:
            if GROUP_BEHAVIOR == "missing-probe" and grouped_rows["probe"]:
                groups = [{"rollback_probe": None, "n": grouped_rows["null"] + grouped_rows["probe"]}]
            else:
                groups = [{"rollback_probe": None, "n": grouped_rows["null"]}]
                if grouped_rows["probe"]:
                    groups.append({"rollback_probe": 1, "n": grouped_rows["probe"]})
        else:
            groups = [{"host": "all", "n": grouped_rows["null"] + grouped_rows["probe"]}]
        if GROUP_BEHAVIOR == "mismatch":
            groups[0]["n"] -= 1
        result = {"rows": groups, "stats": {"phases": {"distributed": {"mode": "aggregate"}}}}
    else:
        result = {"rows": []}
    print(json.dumps(result))
    return 0


def main() -> int:
    tool = pathlib.Path(sys.argv[0]).name
    args = sys.argv[1:]
    record(tool, args)
    return {"curl": curl, "docker": docker, "helm": helm, "kind": kind, "kubectl": kubectl}[tool](args)


if __name__ == "__main__":
    raise SystemExit(main())
