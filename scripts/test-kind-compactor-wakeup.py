#!/usr/bin/env python3
"""Offline acceptance and lifecycle regressions; never contacts Docker or Kubernetes."""
import copy
import datetime
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent


def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / filename)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


grader = module("grader", "grade-kind-compactor-wakeup.py")
runner = module("runner", "kind-compactor-wakeup.py")


def capture():
    result = json.loads((ROOT / "scripts/testdata/kind-compactor-wakeup-verified.json").read_text())
    result["schema_version"] = 4
    result["settings"]["observer_max_bracket_seconds"] = 1
    next(row for row in result["replica_history"] if row["phase"] == "ingested")["at"] = 1789862039.8
    sha = result["revisions"]["repository_commit"]
    result["images"] = {f"{name}:{sha[:12]}": "sha256:" + digit * 64
                        for name, digit in (("siglake-wakeup", "a"), ("siglake-wakeup-operator", "b"))}
    result["build_versions"] = {name: f"siglake 0.2.1 ({sha})" for name in result["images"]}
    initial = {"id": "before", "rows": 500, "status": "committed", "committed_at_ms": 1789861800000}
    sealed = {"id": "new", "rows": 500, "status": "sealed", "committed_at_ms": None}
    committed = dict(sealed, status="committed", committed_at_ms=1789862135000)
    empty = {"status": "success", "data": {"resultType": "vector", "result": []}}
    cr = runner.cluster_manifest("siglake", "siglake:test")
    cr["spec"]["extraEnv"] = [{"name": "SIGLAKE_WAL_SEALED_PUBLISH_SECS", "value": "0"}]
    def timestamp(at):
        return datetime.datetime.fromtimestamp(at, datetime.timezone.utc).isoformat().replace("+00:00", "Z")
    result["acceptance"] = {
        "initial_ledger": [initial], "sealed_ledger": [initial, sealed], "committed_ledger": [committed],
        "positive_started_at": 1789861921, "committed_at": 1789862140,
        "positive_observer": {
            "pre_trigger": {
                "kind": "direct-signal",
                "direct": {"time": 1789861920.25, "source": "operator-managed ingester /metrics",
                           "published_depth": [{"labels": "{tenant=\"default\"}", "value": 0.0}],
                           "sample_age": [{"labels": "{tenant=\"default\"}", "value": 0.5}]},
                "state": {"at": 1789861920.5, "phase": "pre-trigger", "replicas": 0, "pods": []}},
            "positive": {
                "before": {"at": 1789862039.8, "phase": "waiting-for-direct-signal", "replicas": 0, "pods": []},
                "direct": {"time": 1789862039.9, "source": "operator-managed ingester /metrics",
                           "published_depth": [{"labels": "{tenant=\"default\"}", "value": 6.0}],
                           "sample_age": [{"labels": "{tenant=\"default\"}", "value": 0.25}]},
                "after": {"at": 1789862040, "phase": "waiting-for-direct-signal", "replicas": 0, "pods": []}}},
        "query_response": {"rows": [{"host": f"wake-{i:04d}"} for i in range(500)]},
        "absent_compactor_series": {"time": 1789862200, "response": copy.deepcopy(empty)},
        "unaffected_until": 1789862240,
        "negative_parked": {"at": 1789862241, "replicas": 0, "pods": []},
        "negative_started_at": 1789862242,
        "publisher_disabled": cr,
        "missing_activation": {"time": 1789862280, "response": copy.deepcopy(empty)},
        "negative_woken": {"at": 1789862300, "replicas": 1, "pods": []},
        "operator_log": (
            f'{timestamp(1789862041)} INFO reconcile decision namespace=default name=siglake summary=scaled compactor (ing 1→1, comp 0→1, qry 1→1) '
            'observed=ObservedSamples { ingester_rps_per_pod: Some(0.0), compactor_backlog: Some(0.1), query_in_flight_per_pod: Some(0.0) }\n'
            f'{timestamp(1789862220)} INFO reconcile decision namespace=default name=siglake summary=no-op '
            'observed=ObservedSamples { ingester_rps_per_pod: Some(0.0), compactor_backlog: Some(0.0), query_in_flight_per_pod: Some(0.0) }\n'
            f'{timestamp(1789862290)} INFO reconcile decision namespace=default name=siglake summary=scaled comp (ing 1→1, comp 0→1, qry 1→1) '
            'observed=ObservedSamples { ingester_rps_per_pod: Some(0.0), compactor_backlog: None, query_in_flight_per_pod: Some(0.0) }\n'),
        "workloads": {"items": [{"kind": kind, "metadata": {"name": f"siglake-{name}"},
                                  "spec": {"replicas": 1}, "status": {"readyReplicas": 1}}
                                 for name, kind in (("ingester", "Deployment"), ("query", "StatefulSet"))]}}
    return result


class EvidenceTests(unittest.TestCase):
    def test_complete_fixture(self):
        evidence = grader.grade_acceptance(capture())
        self.assertEqual(evidence["summary"]["committed_rows"], 500)
        self.assertEqual(evidence["summary"]["missing_publisher_woken_replicas"], 1)

    def refuse(self, mutate, reason):
        doc = capture()
        mutate(doc)
        with self.assertRaisesRegex(grader.Unverified, reason):
            grader.grade_acceptance(doc)

    def test_image_must_identify_pinned_source(self):
        self.refuse(lambda d: d.update(build_versions={}), "image build provenance")

    def test_activation_only_is_not_complete_acceptance(self):
        self.refuse(lambda d: d.update(schema_version=2), "schema_version 4")

    def test_queryable_wal_is_not_commit(self):
        self.refuse(lambda d: d["acceptance"]["committed_ledger"][0].update(status="sealed"), "committed certificates")

    def test_old_commits_cannot_be_credited(self):
        self.refuse(lambda d: d["acceptance"]["committed_ledger"][0].update(committed_at_ms=1789861900000), "predate")

    def test_another_segments_commit_cannot_be_credited(self):
        self.refuse(lambda d: d["acceptance"]["committed_ledger"][0].update(id="unrelated"), "IDs differ")

    def test_duplicate_ledger_rows_rejected(self):
        self.refuse(lambda d: d["acceptance"]["committed_ledger"].append(d["acceptance"]["committed_ledger"][0]), "duplicate IDs")

    def test_partial_and_duplicate_rows_rejected(self):
        self.refuse(lambda d: d["acceptance"]["query_response"]["rows"].pop(), "exactly the new row IDs")
        self.refuse(lambda d: d["acceptance"]["query_response"]["rows"][0].update(host="wake-0001"), "exactly the new row IDs")

    def test_disabled_publisher_must_be_recorded(self):
        self.refuse(lambda d: d["acceptance"]["publisher_disabled"]["spec"].update(extraEnv=[]), "disabled publisher")

    def test_failed_prom_query_is_not_absence(self):
        self.refuse(lambda d: d["acceptance"]["missing_activation"]["response"].update(status="error"), "successful empty vector")

    def test_present_compactor_series_not_credited(self):
        self.refuse(lambda d: d["acceptance"]["absent_compactor_series"]["response"]["data"].update(result=[{}]), "successful empty vector")

    def test_negative_control_must_start_without_pods(self):
        self.refuse(lambda d: d["acceptance"]["negative_parked"].update(pods=["terminating"]), "begin parked")

    def test_no_fallback_while_parked(self):
        self.refuse(lambda d: d["acceptance"]["negative_woken"].update(replicas=0), "restore exactly one")

    def test_fallback_timestamp_order(self):
        self.refuse(lambda d: d["acceptance"]["negative_woken"].update(at=1789862240), "out of order")

    def test_missing_tier_decisions(self):
        self.refuse(lambda d: d["acceptance"].update(operator_log=""), "ordinary ingester/query decisions")
        self.refuse(lambda d: d["acceptance"].update(operator_log=d["acceptance"]["operator_log"].replace("query_in_flight_per_pod: Some(0.0)", "query_in_flight_per_pod: None")), "ordinary ingester/query decisions")

    def test_unrelated_operator_decision_not_credited(self):
        self.refuse(lambda d: d["acceptance"].update(operator_log=d["acceptance"]["operator_log"].replace("name=siglake", "name=another")), "ordinary ingester/query decisions")

    def test_ordinary_positive_wake_is_not_missing_signal(self):
        self.refuse(lambda d: d["acceptance"].update(operator_log=d["acceptance"]["operator_log"].replace("compactor_backlog: None", "compactor_backlog: Some(2.0)")), "missing-signal decision")

    def test_floors_must_remain_ready(self):
        self.refuse(lambda d: d["acceptance"]["workloads"]["items"][0]["status"].update(readyReplicas=0), "ready floor")

    def test_direct_positive_requires_a_parked_bracket(self):
        self.refuse(lambda d: d["acceptance"]["positive_observer"]["positive"]["before"].update(replicas=1),
                    "preceding parked")

    def test_direct_positive_requires_depth_and_age(self):
        self.refuse(lambda d: d["acceptance"]["positive_observer"]["positive"]["direct"].update(sample_age=[]),
                    "missing sample_age")

    def test_direct_observer_must_precede_the_trigger(self):
        self.refuse(lambda d: d["acceptance"]["positive_observer"]["pre_trigger"]["state"].update(at=1789861922),
                    "start before")

    def test_positive_wake_requires_operator_decision(self):
        self.refuse(lambda d: d["acceptance"].update(operator_log=d["acceptance"]["operator_log"].replace(
            "compactor_backlog: Some(0.1)", "compactor_backlog: None", 1)), "positive-signal decision")

    def test_cli_requires_full_capture(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "capture.json"
            out = Path(tmp) / "graded.json"
            path.write_text(json.dumps(capture()))
            result = subprocess.run(["python3", str(ROOT / "scripts/grade-kind-compactor-wakeup.py"), str(path), "--require-acceptance", "--output", str(out)], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(out.read_text())["evidence"]["grade"], "verified")


class PartialSuccessTests(unittest.TestCase):
    """Run #173 crashed on `"partialSuccess": null` before collecting any evidence."""

    def test_accepted_shapes_report_no_rejection(self):
        for response in ({}, {"partialSuccess": None}, {"partial_success": None},
                         {"partialSuccess": {}}, {"partial_success": {}},
                         {"partialSuccess": {"rejectedLogRecords": None}},
                         {"partialSuccess": {"rejectedLogRecords": 0}},
                         {"partial_success": {"rejected_log_records": "0"}},
                         {"partialSuccess": {"errorMessage": ""}}):
            with self.subTest(response=response):
                self.assertEqual(runner.rejected_log_records(response), 0)

    def test_rejection_counts_are_still_read(self):
        # Protobuf JSON writes int64 as a string; both spellings must count.
        for response, count in (({"partialSuccess": {"rejectedLogRecords": "7"}}, 7),
                                ({"partialSuccess": {"rejectedLogRecords": 7}}, 7),
                                ({"partial_success": {"rejected_log_records": "7"}}, 7),
                                ({"partialSuccess": {"rejectedLogRecords": "500", "errorMessage": "full"}}, 500)):
            with self.subTest(response=response):
                self.assertEqual(runner.rejected_log_records(response), count)

    def ingest(self, response):
        with tempfile.TemporaryDirectory() as tmp:
            r = runner.Round.__new__(runner.Round)
            r.out, r.ingest_url = Path(tmp), "http://127.0.0.1:1/ignored"
            r.http = lambda url, payload=None: response
            try:
                r.ingest("probe")
            finally:
                self.evidence = json.loads((r.out / "ingest-probe.json").read_text())

    def test_null_partial_success_does_not_abort_the_capture(self):
        # Siglake's own reply shape, as retained in run #173's ingest-initial.json.
        self.ingest({"partialSuccess": None})
        self.assertEqual(len(self.evidence["request"]["resourceLogs"]), runner.BATCH)

    def test_rejected_records_still_abort_the_capture(self):
        with self.assertRaisesRegex(RuntimeError, "partially rejected"):
            self.ingest({"partialSuccess": {"rejectedLogRecords": "1"}})
        self.assertEqual(self.evidence["response"]["partialSuccess"]["rejectedLogRecords"], "1")


class PositiveObserverTests(unittest.TestCase):
    def observation(self, depth=6, age=.25, replicas=0, pods=None, at=20):
        return {"direct": {"time": at - .05,
                           "published_depth": [{"labels": '{tenant="default"}', "value": depth}],
                           "sample_age": [{"labels": '{tenant="default"}', "value": age}]},
                "state": {"at": at, "replicas": replicas, "pods": [] if pods is None else pods}}

    def test_direct_signal_wins_the_prometheus_wake_race(self):
        # Run 176 sampled zero, then Prometheus and the operator won the next
        # 43 ms. A direct scrape before that shared Prometheus scrape retains
        # the positive signal while both replica observations still read zero.
        before = {"at": 19.8, "replicas": 0, "pods": []}
        self.assertTrue(runner.direct_observation_is_positive(before, self.observation()))

    def test_following_wake_does_not_erase_the_preceding_parked_sample(self):
        before = {"at": 19.8, "replicas": 0, "pods": []}
        self.assertTrue(runner.direct_observation_is_positive(
            before, self.observation(replicas=1, pods=["compactor"])))

    def test_missing_or_stale_direct_evidence_is_not_positive(self):
        before = {"at": 19.8, "replicas": 0, "pods": []}
        missing = self.observation()
        missing["direct"]["sample_age"] = []
        self.assertFalse(runner.direct_observation_is_positive(before, missing))
        self.assertFalse(runner.direct_observation_is_positive(before, self.observation(age=121)))
        self.assertFalse(runner.direct_observation_is_positive(before, self.observation(depth=0)))

    def test_metrics_parser_keeps_distinct_tenant_series(self):
        text = (f'{runner.SEALED_DEPTH}{{tenant="a"}} 2\n'
                f'{runner.SEALED_DEPTH}{{tenant="b"}} 3.5\n')
        self.assertEqual(runner.metric_series(text, runner.SEALED_DEPTH), [
            {"labels": '{tenant="a"}', "value": 2.0},
            {"labels": '{tenant="b"}', "value": 3.5},
        ])


class BootstrapTests(unittest.TestCase):
    def test_activation_expression_matches_operator_fixture(self):
        fixture = json.loads((ROOT / "scripts/testdata/kind-compactor-wakeup-verified.json").read_text())
        self.assertEqual(runner.activation_expression("siglake"), fixture["queries"]["operator_expression"]["expression"])

    def test_cr_has_supported_zero_floor_and_catalog_claim_ceiling(self):
        cr = runner.cluster_manifest("wakeup", "siglake:exact")
        self.assertEqual(cr["spec"]["image"], "siglake:exact")
        self.assertEqual(cr["spec"]["autoscaling"]["compactor"], {"min": 0, "max": 2, "target": 5})
        self.assertGreater(cr["spec"]["autoscaling"]["ewmaHalfLifeSecs"], 0)
        self.assertEqual(cr["spec"]["autoscaling"]["ingester"]["min"], 1)
        self.assertEqual(cr["spec"]["autoscaling"]["query"]["min"], 1)

    def test_validated_opt_in_routes_before_generic_cluster_creation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scripts = root / "scripts"
            scripts.mkdir()
            for name in ("kind-round.sh", "kind-common.bash"):
                (scripts / name).write_text((ROOT / "scripts" / name).read_text())
            # The stand-in is the sole allowed side effect; any generic bootstrap
            # would need missing tools and fail instead of printing this receipt.
            (scripts / "kind-compactor-wakeup.py").write_text("print('dedicated-helper')\n")
            import os
            env = dict(os.environ, COMPACTOR_WAKEUP_CAPTURE="1", COMPACTOR_WAKEUP_CLUSTER="wakeup")
            result = subprocess.run(["bash", str(scripts / "kind-round.sh")], env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.strip(), "dedicated-helper")
            for key in ("COMPACTOR_POD_LABEL_CAPTURE", "INGESTER_POD_LABEL_CAPTURE", "POSTGRES_OUTAGE_PROBE", "SCHEMA_ROLLBACK_PROBE"):
                bad = subprocess.run(["bash", str(scripts / "kind-round.sh")], env=dict(env, **{key: "1"}), capture_output=True, text=True)
                self.assertNotEqual(bad.returncode, 0, key)
                self.assertNotIn("dedicated-helper", bad.stdout)
            mirror = subprocess.run(["bash", str(scripts / "kind-round.sh")], env=dict(env, KIND_ROUND_MIRROR_RECLAIM_ARM="on"), capture_output=True, text=True)
            self.assertNotEqual(mirror.returncode, 0)
            self.assertNotIn("dedicated-helper", mirror.stdout)
            (scripts / "kind-compactor-wakeup.py").unlink()
            missing = subprocess.run(["bash", str(scripts / "kind-round.sh")], env=env, capture_output=True, text=True)
            self.assertNotEqual(missing.returncode, 0)

    def test_pod_monitor_scrapes_each_pod_once(self):
        mon = runner.pod_monitor("wakeup")
        self.assertEqual(mon["kind"], "PodMonitor")
        self.assertEqual(mon["spec"]["selector"]["matchLabels"], {"app.kubernetes.io/instance": "wakeup"})
        self.assertEqual(len(mon["spec"]["podMetricsEndpoints"]), 1)
        self.assertIn("app.kubernetes.io/component", mon["spec"]["podTargetLabels"])

    def test_cleanup_refuses_changed_ownership(self):
        with tempfile.TemporaryDirectory() as tmp:
            r = runner.Round.__new__(runner.Round)
            r.pfs, r.owned_id, r.kind, r.doc, r.out = [], "mine", "kind-round", {}, Path(tmp)
            r.kube = lambda *a, **kw: "{}"
            calls = []
            def command(*args, **kwargs):
                calls.append(args)
                return "somebody-else\n"
            r.run = command
            with self.assertRaisesRegex(RuntimeError, "identity changed"):
                r.cleanup()
            self.assertFalse(any(args[:3] == ("kind", "delete", "cluster") for args in calls))

    def test_existing_cluster_is_never_reused(self):
        r = runner.Round.__new__(runner.Round)
        r.kind = "occupied"
        r.run = lambda *args, **kwargs: "occupied\n"
        with patch.object(runner.shutil, "which", return_value="/bin/tool"):
            with self.assertRaisesRegex(RuntimeError, "refusing to reuse"):
                r.bootstrap()


if __name__ == "__main__":
    unittest.main()
