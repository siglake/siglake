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
