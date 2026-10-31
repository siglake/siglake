import contextlib
import datetime as dt
import io
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import call, patch
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

    def test_version_selection_keeps_historical_checks_and_adds_0_4_0(self):
        self.assertEqual(
            v.SUPPORTED_VERSIONS,
            ('v0.1.0', 'v0.2.0', 'v0.2.1', 'v0.3.0', 'v0.4.0'),
        )
        self.assertEqual(v.PROVENANCE_VERSIONS, {'v0.2.1', 'v0.3.0', 'v0.4.0'})
        self.assertEqual(
            v.QUERY_AUDIT_AGGREGATE_VERSIONS,
            {'v0.2.1', 'v0.3.0', 'v0.4.0'},
        )
        for version in ('v0.2.1', 'v0.3.0', 'v0.4.0'):
            args = v.argument_parser().parse_args([
                '--version', version, '--profile', 'smoke', '--out', '/unused'
            ])
            self.assertEqual(args.version, version)
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            v.argument_parser().parse_args([
                '--version', 'v0.5.0', '--profile', 'smoke', '--out', '/unused'
            ])

    def test_candidate_inputs_are_exact_all_or_none_and_smoke_only(self):
        source = 'a' * 40
        engine = f'ghcr.io/siglake/siglake:{source}'
        operator = f'ghcr.io/siglake/siglake-operator:{source}'
        self.assertEqual(
            v.candidate_config('v0.4.0', 'smoke', 'public', source, engine, operator),
            {
                'source_commit': source,
                'images': {'engine': engine, 'operator': operator},
            },
        )
        refusals = (
            ('v0.4.0', 'smoke', 'public', source, '', ''),
            ('v0.3.0', 'smoke', 'public', source, engine, operator),
            ('v0.4.0', '24h', 'public', source, engine, operator),
            ('v0.4.0', '72h', 'public', source, engine, operator),
            ('v0.4.0', 'smoke', 'cached-diagnostic', source, engine, operator),
            ('v0.4.0', 'smoke', 'public', source[:-1], engine, operator),
            (
                'v0.4.0', 'smoke', 'public', source,
                f'ghcr.io/foreign/siglake:{source}', operator,
            ),
            (
                'v0.4.0', 'smoke', 'public', source,
                'ghcr.io/siglake/siglake:0.4.0', operator,
            ),
        )
        for values in refusals:
            with self.subTest(values=values), self.assertRaises(ValueError):
                v.candidate_config(*values)

    def test_actual_parser_rejects_whitespace_candidate_intent_before_side_effects(self):
        source = 'a' * 40
        engine = f'ghcr.io/siglake/siglake:{source}'
        operator = f'ghcr.io/siglake/siglake-operator:{source}'
        malformed = (
            (' ', '', ''),
            ('\t', '', ''),
            ('\n', '', ''),
            (' ', '\t', '\n'),
            (' ', '', '\t'),
            (' ' + source, engine, operator),
            (source + '\t', engine, operator),
            (source, engine + ' ', operator),
            (source, engine, '\n' + operator),
        )
        with tempfile.TemporaryDirectory() as temp:
            for index, (candidate_source, candidate_engine, candidate_operator) in enumerate(
                malformed
            ):
                out = Path(temp) / f'refused-{index}'
                argv = [
                    '--version', 'v0.4.0',
                    '--profile', 'smoke',
                    '--candidate-source', candidate_source,
                    '--candidate-engine-image', candidate_engine,
                    '--candidate-operator-image', candidate_operator,
                    '--out', str(out),
                ]
                with (
                    self.subTest(
                        values=(candidate_source, candidate_engine, candidate_operator)
                    ),
                    patch.object(v, 'run_command') as command,
                    patch.object(v, 'http') as request,
                    contextlib.redirect_stderr(io.StringIO()),
                    self.assertRaises(SystemExit),
                ):
                    v.parse_arguments(argv)
                command.assert_not_called()
                request.assert_not_called()
                self.assertFalse(out.exists())

    def test_actual_parser_and_prepare_choose_only_the_exact_requested_source(self):
        source = 'a' * 40
        engine = f'ghcr.io/siglake/siglake:{source}'
        operator = f'ghcr.io/siglake/siglake-operator:{source}'
        selections = (
            ([], None, 'v0.4.0^{commit}'),
            ([
                '--candidate-source', '',
                '--candidate-engine-image', '',
                '--candidate-operator-image', '',
            ], None, 'v0.4.0^{commit}'),
            ([
                '--candidate-source', source,
                '--candidate-engine-image', engine,
                '--candidate-operator-image', operator,
            ], {
                'source_commit': source,
                'images': {'engine': engine, 'operator': operator},
            }, source + '^{commit}'),
        )
        with tempfile.TemporaryDirectory() as temp:
            for index, (candidate_args, expected_candidate, expected_ref) in enumerate(selections):
                argv = [
                    '--version', 'v0.4.0',
                    '--profile', 'smoke',
                    '--out', str(Path(temp) / f'run-{index}'),
                    *candidate_args,
                ]
                args = v.parse_arguments(argv)
                self.assertEqual(args.candidate, expected_candidate)
                run = v.Run(args)
                with patch.object(
                    v, 'run_command', side_effect=RuntimeError('prepare boundary')
                ) as command, self.assertRaisesRegex(RuntimeError, 'prepare boundary'):
                    run.prepare()
                command.assert_called_once_with([
                    'git', '-C', str(v.ROOT), 'rev-parse', expected_ref,
                ])

    def test_candidate_platform_selection_refuses_foreign_or_ambiguous_types(self):
        digest = 'sha256:' + 'b' * 64
        valid = {
            'schemaVersion': 2,
            'manifests': [{
                'mediaType': 'application/vnd.oci.image.manifest.v1+json',
                'digest': digest,
                'platform': {'os': 'linux', 'architecture': 'amd64'},
            }],
        }
        self.assertEqual(v.platform_descriptor(valid, 'linux', 'amd64')['digest'], digest)
        with self.assertRaisesRegex(ValueError, '0 manifests'):
            v.platform_descriptor(valid, 'linux', 'arm64')
        foreign = {'schemaVersion': 2, 'manifests': [dict(valid['manifests'][0])]}
        foreign['manifests'][0]['mediaType'] = 'application/vnd.oci.artifact.manifest.v1+json'
        with self.assertRaisesRegex(ValueError, 'not an OCI/Docker image manifest'):
            v.platform_descriptor(foreign, 'linux', 'amd64')

    def test_candidate_manifest_graph_requires_exact_source_label(self):
        source = 'a' * 40
        child_digest = 'sha256:' + 'b' * 64
        config_digest = 'sha256:' + 'c' * 64
        index = {
            'schemaVersion': 2,
            'mediaType': 'application/vnd.oci.image.index.v1+json',
            'manifests': [{
                'mediaType': 'application/vnd.oci.image.manifest.v1+json',
                'digest': child_digest,
                'platform': {'os': 'linux', 'architecture': 'amd64'},
            }],
        }
        manifest = {
            'schemaVersion': 2,
            'mediaType': 'application/vnd.oci.image.manifest.v1+json',
            'config': {
                'mediaType': 'application/vnd.oci.image.config.v1+json',
                'digest': config_digest,
            },
            'layers': [{
                'mediaType': 'application/vnd.oci.image.layer.v1.tar+gzip',
                'digest': 'sha256:' + 'd' * 64,
            }],
        }
        config = {
            'os': 'linux',
            'architecture': 'amd64',
            'config': {'Labels': {
                'org.opencontainers.image.source': v.OCI_SOURCE,
                'org.opencontainers.image.revision': source[:12],
            }},
        }
        replies = [
            (v.json.dumps(index).encode(), 'sha256:' + 'e' * 64, index['mediaType']),
            (v.json.dumps(manifest).encode(), child_digest, manifest['mediaType']),
            (v.json.dumps(config).encode(), config_digest, 'application/json'),
        ]
        with patch.object(v, 'http', return_value={'token': 'public'}), patch.object(
            v, '_registry_bytes', side_effect=replies
        ):
            graph = v.resolve_candidate_manifest(
                'siglake/siglake', source, 'linux', 'amd64', source
            )
        self.assertEqual(graph['image'], f'ghcr.io/siglake/siglake@{child_digest}')
        self.assertEqual(graph['config_digest'], config_digest)
        self.assertEqual(graph['source_revision'], source[:12])

        config['config']['Labels']['org.opencontainers.image.source'] = 'https://example.invalid/x'
        replies[-1] = (v.json.dumps(config).encode(), config_digest, 'application/json')
        with patch.object(v, 'http', return_value={'token': 'public'}), patch.object(
            v, '_registry_bytes', side_effect=replies
        ), self.assertRaisesRegex(ValueError, 'foreign source'):
            v.resolve_candidate_manifest(
                'siglake/siglake', source, 'linux', 'amd64', source
            )
        config['config']['Labels'] = {
            'org.opencontainers.image.source': v.OCI_SOURCE,
            'org.opencontainers.image.revision': 'f' * 12,
        }
        replies[-1] = (v.json.dumps(config).encode(), config_digest, 'application/json')
        with patch.object(v, 'http', return_value={'token': 'public'}), patch.object(
            v, '_registry_bytes', side_effect=replies
        ), self.assertRaisesRegex(ValueError, 'revision does not match'):
            v.resolve_candidate_manifest(
                'siglake/siglake', source, 'linux', 'amd64', source
            )

    def test_candidate_workflow_keeps_inputs_out_of_shell_source(self):
        workflow = (
            Path(__file__).resolve().parents[2]
            / '.github/workflows/release-validation.yml'
        ).read_text()
        self.assertIn(
            'version: [v0.1.0, v0.2.0, v0.2.1, v0.3.0, v0.4.0]',
            workflow,
        )
        for name in (
            'candidate_source',
            'candidate_engine_image',
            'candidate_operator_image',
        ):
            self.assertIn(f'{name}:', workflow)
        run_blocks = []
        lines = workflow.splitlines()
        for index, line in enumerate(lines):
            if line.strip() != 'run: |':
                continue
            indent = len(line) - len(line.lstrip())
            block = []
            for nested in lines[index + 1:]:
                nested_indent = len(nested) - len(nested.lstrip())
                if nested.strip() and nested_indent <= indent:
                    break
                block.append(nested)
            run_blocks.append('\n'.join(block))
        shell_source = '\n'.join(run_blocks)
        self.assertNotIn('${{ inputs.candidate_', shell_source)
        self.assertIn('--candidate-source "$CANDIDATE_SOURCE"', shell_source)

    def test_optional_feature_defaults_are_absent_from_packaged_workloads(self):
        root = Path(__file__).resolve().parents[2]
        compactor = (
            root / 'deploy/helm/siglake/templates/deployment-compactor.yaml'
        ).read_text()
        query = (
            root / 'deploy/helm/siglake/templates/statefulset-query-server.yaml'
        ).read_text()
        operator = (root / 'crates/siglake-operator/src/render.rs').read_text()
        for knob, template in (
            ('SIGLAKE_AUTO_PROMOTE_MIN_PCT', compactor),
            ('SIGLAKE_SEGMENTED_INDEX_WRITES', compactor),
            ('SIGLAKE_SEGMENTED_INDEX_READS', query),
        ):
            self.assertNotIn(f'- name: {knob}', template)
            self.assertNotIn(f'"{knob}"', operator)
        self.assertIn('.Values.compactor.extraEnv', compactor)
        self.assertIn('.Values.query.extraEnv', query)
        self.assertIn('extra_env_flows_into_all_tiers', operator)

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

    def test_capacity_reserve_fails_before_minio_reaches_its_emergency_threshold(self):
        reserve = 64 * 1024**3
        self.assertIsNone(v.capacity_reserve_problem(reserve, reserve))
        problem = v.capacity_reserve_problem(reserve - 1, reserve)
        self.assertIn(str(reserve - 1), problem)
        self.assertIn(str(reserve), problem)

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

    def test_audit_checkpoint_retries_one_transport_timeout_within_its_deadline(self):
        covered = {'stats': {'served_by': 'tier1_inline', 'rows_scanned': 0}}
        with tempfile.TemporaryDirectory() as temp:
            args = SimpleNamespace(
                out=str(Path(temp)/'run'),
                version='v0.2.1',
                profile='smoke',
                dependency_policy='public',
            )
            run = v.Run(args)
            run.query = 'http://127.0.0.1:40001'
            with patch.object(
                run,
                'sql',
                side_effect=[TimeoutError('timed out'), [{'n': 1}]],
            ) as sql, patch.object(
                run,
                'sql_response',
                return_value=covered,
            ) as aggregate, patch.object(
                v.time,
                'monotonic',
                side_effect=[0, 0, 1, 2],
            ), patch.object(v.time, 'sleep') as sleep:
                run.await_audit_checkpoint('SELECT 1 /* marker */', 'before_restart')
            self.assertEqual(sql.call_count, 2)
            aggregate.assert_called_once_with(
                'SELECT status, count(*) AS n FROM query_audit GROUP BY status ORDER BY status',
                timeout=30,
            )
            sleep.assert_called_once_with(2)
            event = (run.out/'events.jsonl').read_text()
            self.assertIn('"check": "query_audit_checkpoint"', event)

    def test_24h_audit_checkpoint_uses_the_final_oracle_with_a_distinct_marker(self):
        with tempfile.TemporaryDirectory() as temp:
            args = SimpleNamespace(
                out=str(Path(temp)/'run'),
                version='v0.3.0',
                profile='72h',
                dependency_policy='public',
            )
            run = v.Run(args)
            with patch.object(
                run,
                'sql',
                return_value=[{'audit_probe': 1}],
            ) as sql, patch.object(run, 'await_audit_checkpoint') as checkpoint:
                marker = run.run_audit_checkpoint('24h', '24h')
            self.assertEqual(
                marker,
                v.audit_marker_query(run.project, '24h'),
            )
            self.assertNotEqual(marker, v.audit_marker_query(run.project))
            sql.assert_called_once_with(marker)
            checkpoint.assert_called_once_with(marker, '24h')

    def test_execute_wires_distinct_24h_and_unchanged_final_audit_markers(self):
        with tempfile.TemporaryDirectory() as temp:
            args = SimpleNamespace(
                out=str(Path(temp)/'run'),
                version='v0.3.0',
                profile='72h',
                dependency_policy='public',
            )
            run = v.Run(args)
            run.ingest = 'http://127.0.0.1:40001'
            run.query = 'http://127.0.0.1:40002'
            with patch.object(run, 'prepare'), \
                 patch.object(run, 'refused'), \
                 patch.object(run, 'event'), \
                 patch.object(run, 'save'), \
                 patch.object(run, 'ingest_and_check'), \
                 patch.object(run, 'resources'), \
                 patch.object(run, 'batch'), \
                 patch.object(run, 'restart'), \
                 patch.object(
                     run,
                     'sql',
                     return_value=[{'audit_probe': 1}],
                 ) as sql, \
                 patch.object(run, 'await_audit_checkpoint') as checkpoint, \
                 patch.object(
                     v.time,
                     'monotonic',
                     side_effect=[0, 0, 86400, 259200, 259200],
                 ), \
                 patch.object(v.time, 'sleep'):
                run.execute()

            marker_24h = v.audit_marker_query(run.project, '24h')
            marker_final = v.audit_marker_query(run.project)
            self.assertNotEqual(marker_24h, marker_final)
            self.assertEqual(
                marker_final,
                'SELECT 1 AS audit_probe /* release-validation-audit-'
                + run.project + ' */',
            )
            self.assertEqual(
                checkpoint.call_args_list,
                [
                    call(marker_24h, '24h'),
                    call(marker_final, 'before_restart'),
                    call(marker_final, 'after_query_server_restart'),
                ],
            )
            self.assertEqual(
                sql.call_args_list,
                [call(marker_24h), call(marker_final)],
            )

    def test_audit_checkpoint_does_not_retry_product_http_errors(self):
        with tempfile.TemporaryDirectory() as temp:
            args = SimpleNamespace(
                out=str(Path(temp)/'run'),
                version='v0.2.1',
                profile='smoke',
                dependency_policy='public',
            )
            run = v.Run(args)
            error = v.urllib.error.HTTPError('/api/v1/sql', 500, 'failed', {}, None)
            with patch.object(run, 'sql', side_effect=error), patch.object(
                v.time, 'monotonic', side_effect=[0, 0]
            ), self.assertRaises(v.urllib.error.HTTPError):
                run.await_audit_checkpoint('SELECT 1 /* marker */', 'before_restart')
            error.close()

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
