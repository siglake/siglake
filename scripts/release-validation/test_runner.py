import contextlib
import datetime as dt
import io
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace

spec = importlib.util.spec_from_file_location('validation', Path(__file__).with_name('run.py'))
v = importlib.util.module_from_spec(spec); spec.loader.exec_module(v)


class IsolationTests(unittest.TestCase):
    def config(self):
        return {'services': {name: {'image': 'old', 'environment': {}, 'build': '.', 'container_name': 'global', 'ports': ['9000:9000'], 'command': [], 'volumes': [{'type': 'volume', 'source': 'data', 'target': '/data'}]} for name in v.SERVICES}}

    def test_release_template_never_builds_or_binds_global_ports(self):
        config = v.isolated_config(self.config(), 'ghcr.io/siglake/siglake@sha256:pin')
        self.assertEqual(config['volumes'], {'data': {}})
        for name, service in config['services'].items():
            self.assertNotIn('build', service)
            self.assertNotIn('container_name', service)
            for port in service['ports']:
                self.assertEqual(port['host_ip'], '127.0.0.1')
                self.assertEqual(port['published'], '0')
        self.assertEqual(config['services']['query-server']['mem_limit'], '4g')

    def test_bind_mount_cannot_reach_customer_data(self):
        config = self.config()
        config['services']['postgres']['volumes'][0]['type'] = 'bind'
        with self.assertRaises(ValueError):
            v.isolated_config(config, 'release')

    def test_version_selection_adds_0_2_1_without_changing_historical_versions(self):
        self.assertEqual(v.SUPPORTED_VERSIONS, ('v0.1.0', 'v0.2.0', 'v0.2.1'))
        self.assertEqual(v.PROVENANCE_VERSIONS, {'v0.2.1'})
        args = v.argument_parser().parse_args([
            '--version', 'v0.2.1', '--profile', 'smoke', '--out', '/unused'
        ])
        self.assertEqual(args.version, 'v0.2.1')
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            v.argument_parser().parse_args([
                '--version', 'v0.3.0', '--profile', 'smoke', '--out', '/unused'
            ])

    def test_product_provenance_accepts_the_release_commit(self):
        source = '5f14976533d79de60d31a727091241108ad1162e'
        self.assertEqual(
            v.product_provenance('siglake 0.2.1 (5f14976)\n', 'v0.2.1', source, 'siglake'),
            {'binary': 'siglake', 'version': '0.2.1', 'revision': '5f14976'},
        )
        self.assertEqual(
            v.product_provenance(
                'siglake-operator 0.2.1 (5f14976533d7)\n',
                'v0.2.1',
                source,
                'siglake-operator',
            ),
            {'binary': 'siglake-operator', 'version': '0.2.1', 'revision': '5f14976533d7'},
        )

    def test_product_provenance_refuses_wrong_or_unverifiable_identity(self):
        source = '5f14976533d79de60d31a727091241108ad1162e'
        for output in (
            'siglake 0.2.0 (5f14976)\n',
            'siglake 0.2.1 (aaaaaaaa)\n',
            'siglake 0.2.1 (unknown)\n',
            'siglake 0.2.1 (5f149)\n',
            'wrong-binary 0.2.1 (5f14976)\n',
        ):
            with self.subTest(output=output), self.assertRaises(ValueError):
                v.product_provenance(output, 'v0.2.1', source, 'siglake')

    def test_cleanup_failure_overrides_success(self):
        with tempfile.TemporaryDirectory() as temp:
            args = SimpleNamespace(out=str(Path(temp)/'run'), version='v0.2.0', profile='smoke', dependency_policy='public')
            run = v.Run(args); run.mutated = True; run.summary['status'] = 'passed'
            with patch.object(run, 'cmd', side_effect=RuntimeError('docker unavailable')):
                run.cleanup()
            self.assertEqual(run.summary['status'], 'failed')
            self.assertEqual(run.summary['cleanup'], 'failed')
            self.assertTrue((run.out/'sha256.json').exists())

    def test_artifacts_cannot_be_overwritten(self):
        with tempfile.TemporaryDirectory() as temp:
            args = SimpleNamespace(out=str(Path(temp)/'run'), version='v0.2.0', profile='smoke', dependency_policy='public')
            v.Run(args)
            with self.assertRaises(FileExistsError):
                v.Run(args)

    def test_restart_discovers_new_random_ports_before_querying(self):
        with tempfile.TemporaryDirectory() as temp:
            args = SimpleNamespace(out=str(Path(temp)/'run'), version='v0.2.0', profile='smoke', dependency_policy='public')
            run = v.Run(args)
            replies = iter(['', '127.0.0.1:40001', '127.0.0.1:40002', '127.0.0.1:40003'])
            with patch.object(run, 'cmd', side_effect=lambda *a, **kw: next(replies)), \
                 patch.object(run, 'oracle') as oracle, \
                 patch.object(v.urllib.request, 'urlopen') as health:
                run.restart('query-server')
                health.assert_called_once_with('http://127.0.0.1:40002/healthz', timeout=10)
                oracle.assert_called_once()
                self.assertEqual(run.ingest, 'http://127.0.0.1:40001')

    def test_docker_capacity_parsers_retain_available_bytes(self):
        self.assertEqual(v.parse_docker_root_dir('"/var/lib/docker"\n'), '/var/lib/docker')
        self.assertEqual(
            v.parse_filesystem_capacity(
                'Filesystem          Avail Mounted on\n/dev/mapper/data  987654 /var/lib/docker\n'
            ),
            {'filesystem': '/dev/mapper/data', 'mountpoint': '/var/lib/docker', 'available_bytes': 987654},
        )
        with self.assertRaises(ValueError):
            v.parse_filesystem_capacity('Filesystem Avail Mounted on\n/dev/data unknown /var/lib/docker\n')

    def test_storage_full_classification_is_bounded_to_failed_cohort(self):
        started = dt.datetime(2026, 9, 26, 12, 0, tzinfo=dt.timezone.utc)
        ended = started + dt.timedelta(seconds=120)
        earlier = 'minio | 2026-09-26T11:59:59Z HTTP 507 XMinioStorageFull'
        current = 'compactor | 2026-09-26T12:00:30.123Z HTTP 507 XMinioStorageFull'
        self.assertEqual(v.classify_cohort_failure(earlier, started, ended), 'visibility_mismatch')
        self.assertEqual(v.classify_cohort_failure(current, started, ended), 'storage_capacity_exhausted')

    def test_audit_checkpoint_requires_persisted_probe_and_inline_coverage(self):
        covered = {'stats': {'served_by': 'tier1_inline', 'rows_scanned': 0}}
        self.assertIsNone(v.audit_checkpoint_problem([{'n': 1}], covered))
        self.assertIn(
            'exactly one',
            v.audit_checkpoint_problem([{'n': 0}], covered),
        )
        self.assertIn(
            'served_by=\'materialized\'',
            v.audit_checkpoint_problem(
                [{'n': 1}],
                {'stats': {'served_by': 'materialized', 'rows_scanned': 0}},
            ),
        )

    def test_cleanup_fails_on_unexpected_supported_store_guard_refusal(self):
        refusal = (
            'query-server | ERROR error=cannot establish compatibility: inner cause '
            + v.SIDE_AGGREGATE_GUARD_REFUSAL
            + '\n'
        )
        with tempfile.TemporaryDirectory() as temp:
            args = SimpleNamespace(
                out=str(Path(temp)/'run'),
                version='v0.2.1',
                profile='smoke',
                dependency_policy='public',
            )
            run = v.Run(args)
            run.mutated = True
            run.summary['status'] = 'passed'
            replies = iter([refusal, '', '', '', ''])
            with patch.object(run, 'cmd', side_effect=lambda *a, **kw: next(replies)), \
                 patch.object(v, 'run_command', return_value=''):
                run.cleanup()
            self.assertEqual(run.summary['status'], 'failed')
            self.assertEqual(run.summary['artifact_review'], 'failed')
            self.assertEqual(run.summary['side_aggregate_guard_refusals'], 1)
            self.assertEqual(
                (run.out/'side-aggregate-guard-refusals.log').read_text(),
                refusal,
            )

    def test_failed_cohort_reports_current_project_storage_exhaustion_without_docker(self):
        with tempfile.TemporaryDirectory() as temp:
            args = SimpleNamespace(out=str(Path(temp)/'run'), version='v0.2.0', profile='smoke', dependency_policy='public')
            run = v.Run(args)
            run.ingest = 'http://127.0.0.1:40001'
            started = dt.datetime(2026, 9, 26, 12, 0, tzinfo=dt.timezone.utc)
            ended = started + dt.timedelta(seconds=120)
            logs = 'compactor | 2026-09-26T12:01:00Z HTTP 507 XMinioStorageFull\n'
            with patch.object(v, 'now_utc', side_effect=[started, ended]), \
                 patch.object(v, 'http', return_value={}), \
                 patch.object(run, 'sql', return_value=[]), \
                 patch.object(run, 'cmd', return_value=logs) as cmd, \
                 patch.object(v.time, 'monotonic', side_effect=[0, 121]), \
                 patch.object(v.time, 'sleep'):
                with self.assertRaisesRegex(v.StorageCapacityExhausted, 'HTTP 507 XMinioStorageFull'):
                    run.ingest_and_check()
            cmd.assert_called_once_with(
                'logs', '--no-color', '--timestamps', '--since', started.isoformat(), timeout=60
            )
            self.assertEqual((run.out/'cohort-failure.log').read_text(), logs)
