#!/usr/bin/env python3
"""Public artifact validation. Standard library; Docker Compose v2 and git required.

Each run owns a unique Compose project. Never builds a product image, uses the
caller’s registry login, rewrites old results, or prunes unrelated Docker state.
"""
import argparse
import concurrent.futures
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[2]
DURATIONS = {'smoke': 120, '24h': 86400, '72h': 259200}
SUPPORTED_VERSIONS = ('v0.1.0', 'v0.2.0', 'v0.2.1')
PROVENANCE_VERSIONS = frozenset({'v0.2.1'})
PRODUCT_REPOSITORIES = {
    'engine': 'siglake/siglake',
    'operator': 'siglake/siglake-operator',
}
PRODUCT_BINARIES = {
    'engine': 'siglake',
    'operator': 'siglake-operator',
}
TOKEN = 'release-validation-fixture-token'  # disposable local fixture, not a credential
SERVICES = ['postgres', 'minio', 'minio-init', 'ingester', 'compactor', 'query-server']
LOG_TIMESTAMP = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})')


class StorageCapacityExhausted(AssertionError):
    pass


def now_utc():
    return dt.datetime.now(dt.timezone.utc)


def utc():
    return now_utc().isoformat()


def write_json(path, value):
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(value, indent=2) + '\n')
    tmp.replace(path)


def run_command(argv, env=None, timeout=300):
    result = subprocess.run(argv, env=env, capture_output=True, text=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f'{argv[0]} {argv[1:3]} exited {result.returncode}: {result.stderr[-2500:]}')
    return result.stdout


def parse_docker_root_dir(output):
    try:
        root = json.loads(output)
    except json.JSONDecodeError as error:
        raise ValueError('docker info returned an invalid Docker root directory') from error
    if not isinstance(root, str) or not Path(root).is_absolute():
        raise ValueError('docker info returned a non-absolute Docker root directory')
    return root


def parse_filesystem_capacity(output):
    lines = [line for line in output.splitlines() if line.strip()]
    if len(lines) != 2:
        raise ValueError('df returned an unexpected number of lines')
    fields = lines[1].split(maxsplit=2)
    if len(fields) != 3:
        raise ValueError('df returned an incomplete backing-filesystem reading')
    filesystem, available, mountpoint = fields
    try:
        available_bytes = int(available)
    except ValueError as error:
        raise ValueError('df returned a non-numeric available-byte reading') from error
    if available_bytes < 0:
        raise ValueError('df returned a negative available-byte reading')
    return {'filesystem': filesystem, 'mountpoint': mountpoint, 'available_bytes': available_bytes}


def filesystem_capacity(path, env=None):
    output = run_command(
        ['df', '--block-size=1', '--output=source,avail,target', '--', path],
        env,
    )
    return parse_filesystem_capacity(output)


def parse_log_time(line):
    match = LOG_TIMESTAMP.search(line)
    if not match:
        return None
    value = match.group(0)
    if value.endswith('Z'):
        value = value[:-1] + '+00:00'
    parsed = dt.datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(dt.timezone.utc)


def classify_cohort_failure(logs, started_at, ended_at):
    for line in logs.splitlines():
        if 'XMinioStorageFull' not in line or not re.search(r'\b507\b', line):
            continue
        logged_at = parse_log_time(line)
        if logged_at is not None and started_at <= logged_at <= ended_at:
            return 'storage_capacity_exhausted'
    return 'visibility_mismatch'


def http(url, data=None, token=TOKEN, timeout=30):
    headers = {'Content-Type': 'application/json'}
    if token:
        headers['Authorization'] = 'Bearer ' + token
    request = urllib.request.Request(url, data=None if data is None else json.dumps(data).encode(), headers=headers)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read() or '{}')


def anonymous_manifest(repo, version):
    url = 'https://ghcr.io/token?service=ghcr.io&scope=repository:' + repo + ':pull'
    token = http(url, token=None)['token']
    headers = {'Authorization': 'Bearer ' + token, 'Accept': 'application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.manifest.v1+json'}
    with urllib.request.urlopen(urllib.request.Request('https://ghcr.io/v2/' + repo + '/manifests/' + version.removeprefix('v'), headers=headers), timeout=30) as response:
        content = response.read()
        digest = 'sha256:' + hashlib.sha256(content).hexdigest()
        if response.headers.get('Docker-Content-Digest') != digest:
            raise RuntimeError('registry digest does not match manifest bytes')
    return 'ghcr.io/' + repo + '@' + digest


def product_provenance(output, version, source_commit, expected_binary):
    match = re.fullmatch(
        r'(?P<binary>\S+) (?P<version>\d+\.\d+\.\d+(?:[-+][^\s]+)?) '
        r'\((?P<revision>[0-9a-f]+)\)\s*',
        output,
    )
    if not match:
        raise ValueError(f'published product returned an invalid --version value: {output!r}')
    if match.group('binary') != expected_binary:
        raise ValueError(
            f'published product reports binary {match.group("binary")}, expected {expected_binary}'
        )
    expected_version = version.removeprefix('v')
    if match.group('version') != expected_version:
        raise ValueError(
            f'published product reports version {match.group("version")}, expected {expected_version}'
        )
    revision = match.group('revision')
    if len(revision) < 7 or not source_commit.startswith(revision):
        raise ValueError(
            f'published product reports revision {revision}, expected release commit {source_commit}'
        )
    return {
        'binary': match.group('binary'),
        'version': match.group('version'),
        'revision': revision,
    }


def isolated_config(config, image):
    """Start from the release's Compose contract, removing global host bindings."""
    selected = {name: config['services'][name] for name in SERVICES}
    volumes = set()
    for name, service in selected.items():
        for key in ('build', 'container_name', 'profiles', 'networks', 'restart'):
            service.pop(key, None)
        service['ports'] = []
        service['cpus'] = 2
        service['mem_limit'] = '4g' if name in ('query-server', 'compactor', 'ingester') else '1g'
        service['memswap_limit'] = service['mem_limit']
        service['logging'] = {'driver': 'json-file', 'options': {'max-size': '10m', 'max-file': '3'}}
        for mount in service.get('volumes', []):
            if mount['type'] != 'volume':
                raise ValueError('validation refuses bind mounts from a release template')
            volumes.add(mount['source'])
        if name in ('ingester', 'compactor', 'query-server'):
            service['image'] = image
            service['environment']['RUST_LOG'] = 'info'
        if name == 'ingester':
            service['environment']['SIGLAKE_AUTH_TOKENS'] = TOKEN
            service['ports'] = [{'target': 8088, 'host_ip': '127.0.0.1', 'published': '0'}]
        if name == 'query-server':
            service['environment']['SIGLAKE_QUERY_TOKENS'] = TOKEN
            service['environment']['SIGLAKE_QUERY_RESULT_CACHE'] = 'off'
            # Removing WAL overlay proves the oracle reads committed object-store data.
            command = service['command']
            if '--query-wal-buffer-dir' in command:
                i = command.index('--query-wal-buffer-dir')
                del command[i:i+2]
            service['ports'] = [{'target': p, 'host_ip': '127.0.0.1', 'published': '0'} for p in (8089, 9105)]
    return {'services': selected, 'volumes': {name: {} for name in sorted(volumes)}}


class Run:
    def __init__(self, args):
        self.args = args
        self.project = 'llvalidate-' + uuid.uuid4().hex[:12]
        self.out = Path(args.out).resolve()
        self.out.mkdir(parents=True, exist_ok=False)
        (self.out / 'docker-auth').mkdir(mode=0o700)
        (self.out / 'docker-auth/config.json').write_text('{}')
        self.env = {key: value for key, value in os.environ.items() if not key.startswith(('SIGLAKE_', 'COMPOSE_', 'DOCKER_AUTH'))}
        self.env['DOCKER_CONFIG'] = str(self.out / 'docker-auth')
        self.compose = ['docker', 'compose', '-p', self.project, '-f', str(self.out / 'compose.json')]
        self.summary = dict(schema_version=1, started_at=utc(), version=args.version,
                            profile=args.profile, dependency_policy=args.dependency_policy, requested_seconds=DURATIONS[args.profile],
                            project=self.project, status='running', cleanup='pending',
                            checks=[], limitations=['Single-node Compose; Kubernetes/operator, OIDC tenant isolation, multi-replica distribution, upgrades, retention/delete, schema evolution and expensive-query cancellation are separate required qualification tracks.',
                            'Bounded baseline load: 100 events per cycle, three simultaneous oracle queries; this is not saturation or a large-working-set performance benchmark.'])
        self.expected = 0
        self.cycles = 0
        self.resource_samples = 0
        self.start = None
        self.mutated = False
        self.docker_root_dir = None
        self.save()

    def save(self):
        write_json(self.out / 'summary.json', self.summary)

    def event(self, name, **fields):
        record = dict(at=utc(), check=name, **fields)
        with (self.out / 'events.jsonl').open('a') as stream:
            stream.write(json.dumps(record) + '\n')
        print(json.dumps(record), flush=True)

    def cmd(self, *args, timeout=300):
        return run_command([*self.compose, *args], self.env, timeout)

    def prepare(self):
        version = self.args.version
        self.summary['source_commit'] = run_command(['git', '-C', str(ROOT), 'rev-parse', version + '^{commit}']).strip()
        self.summary['harness_commit'] = run_command(['git', '-C', str(ROOT), 'rev-parse', 'HEAD']).strip()
        self.summary['harness_sha256'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        self.summary['harness_dirty'] = bool(run_command(['git', '-C', str(ROOT), 'status', '--porcelain']).strip())
        (self.out / 'harness.py').write_bytes(Path(__file__).read_bytes())
        if version in PROVENANCE_VERSIONS:
            self.summary['product_images'] = {
                role: anonymous_manifest(repo, version)
                for role, repo in PRODUCT_REPOSITORIES.items()
            }
            self.summary['image'] = self.summary['product_images']['engine']
            self.summary['product_provenance'] = {}
            for role, image in self.summary['product_images'].items():
                run_command(['docker', 'pull', image], self.env, timeout=600)
                output = run_command(
                    ['docker', 'run', '--rm', '--network', 'none', '--pull', 'never', image, '--version'],
                    self.env,
                    timeout=60,
                )
                self.summary['product_provenance'][role] = product_provenance(
                    output, version, self.summary['source_commit'], PRODUCT_BINARIES[role]
                )
        else:
            self.summary['image'] = anonymous_manifest(
                PRODUCT_REPOSITORIES['engine'], version
            )
        self.summary['platform'] = run_command(['docker', 'info', '--format', '{{.OSType}}/{{.Architecture}}'], self.env).strip()
        self.docker_root_dir = parse_docker_root_dir(
            run_command(['docker', 'info', '--format', '{{json .DockerRootDir}}'], self.env).strip()
        )
        backing = filesystem_capacity(self.docker_root_dir, self.env)
        self.summary['docker_backing_filesystem'] = dict(
            measured_at=utc(), docker_root_dir=self.docker_root_dir, **backing
        )
        self.save()
        template = run_command(['git', '-C', str(ROOT), 'show', version + ':deploy/docker-compose.yml'])
        source = self.out / 'release-compose.yml'
        source.write_text(template)
        config = json.loads(run_command(['docker', 'compose', '-p', self.project, '-f', str(source), 'config', '--format', 'json'], self.env))
        config = isolated_config(config, self.summary['image'])
        # Resolve every dependency to an immutable digest as well. Empty client
        # auth config proves that public pulls do not depend on the user's login.
        pins = {}
        for service in config['services'].values():
            image = service['image']
            if image not in pins:
                if '@sha256:' in image or self.args.dependency_policy == 'public':
                    run_command(['docker', 'pull', image], self.env, timeout=600)
                inspect = json.loads(run_command(['docker', 'image', 'inspect', image], self.env))[0]
                pins[image] = image if '@sha256:' in image else inspect['RepoDigests'][0]
            service['image'] = pins[image]
        self.summary['dependency_images'] = pins
        write_json(self.out / 'compose.json', config)
        self.summary['compose_sha256'] = hashlib.sha256((self.out / 'compose.json').read_bytes()).hexdigest()
        self.save()
        self.mutated = True
        self.cmd('up', '-d', '--no-build', '--wait', '--wait-timeout', '180', timeout=240)
        self.refresh_endpoints()
        self.event('isolated_install', result='pass', dependency_policy=self.args.dependency_policy, image=self.summary['image'])

    def refresh_endpoints(self):
        self.ingest = 'http://' + self.cmd('port', 'ingester', '8088').strip()
        self.query = 'http://' + self.cmd('port', 'query-server', '8089').strip()
        self.metrics = 'http://' + self.cmd('port', 'query-server', '9105').strip()

    def sql(self, query):
        return http(self.query + '/api/v1/sql', {'query': query})['rows']

    def refused(self, url, body, code):
        try:
            http(url, body, token=None)
        except urllib.error.HTTPError as error:
            if error.code == code:
                return
            raise
        raise AssertionError('unauthenticated request unexpectedly accepted')

    def ingest_and_check(self):
        prefix = f'validation-{self.cycles:07d}-'
        cohort_started_at = now_utc()
        records = []
        now = time.time_ns()
        for i in range(100):
            records.append({'resource': {'attributes': [{'key': 'host.name', 'value': {'stringValue': f'host-{i%4}'}}, {'key': 'service.name', 'value': {'stringValue': 'release-validation'}}]}, 'scopeLogs': [{'scope': {'name': 'validation'}, 'logRecords': [{'timeUnixNano': str(now - (i % 7) * 1000000000), 'body': {'stringValue': prefix + f'{i:03d}'}}]}]})
        result = http(self.ingest + '/v1/logs', {'resourceLogs': records})
        if int((result.get('partialSuccess') or {}).get('rejectedLogRecords', 0)):
            raise AssertionError('OTLP rejected records')
        self.expected += 100
        wanted = [{'raw': prefix + f'{i:03d}'} for i in range(100)]
        deadline = time.monotonic() + 120
        while True:
            rows = self.sql("SELECT raw FROM events WHERE raw LIKE '" + prefix + "%' ORDER BY raw")
            if rows == wanted:
                break
            if time.monotonic() >= deadline:
                cohort_ended_at = now_utc()
                try:
                    logs = self.cmd(
                        'logs', '--no-color', '--timestamps',
                        '--since', cohort_started_at.isoformat(),
                        timeout=60,
                    )
                    (self.out / 'cohort-failure.log').write_text(logs)
                except Exception as error:
                    logs = ''
                    self.summary['cohort_failure_log_error'] = str(error)
                    self.save()
                if classify_cohort_failure(logs, cohort_started_at, cohort_ended_at) == 'storage_capacity_exhausted':
                    raise StorageCapacityExhausted(
                        'Docker backing-store capacity exhausted during the failed cohort window: '
                        f'MinIO returned HTTP 507 XMinioStorageFull; got {len(rows)} of 100 expected IDs'
                    )
                raise AssertionError(f'committed cohort differs from 100 expected IDs: got {len(rows)}')
            time.sleep(2)
        self.oracle()
        self.cycles += 1
        self.summary.update(cycles=self.cycles, acknowledged_events=self.expected, last_progress_at=utc())
        self.save()

    def oracle(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            counts, groups, filtered = list(pool.map(self.sql, [
                'SELECT count(*) AS n FROM events',
                'SELECT host, count(*) AS n FROM events GROUP BY host ORDER BY host',
                "SELECT count(*) AS n FROM events WHERE host = 'host-2' AND raw LIKE 'validation-%'",
            ]))
        assert counts == [{'n': self.expected}], (counts, self.expected)
        assert groups == [{'host': f'host-{i}', 'n': self.expected//4} for i in range(4)], groups
        assert filtered == [{'n': self.expected//4}], filtered

    def batch(self):
        job = http(self.query + '/api/v1/sql', {'query': 'SELECT count(*) AS n FROM events', 'priority': 'batch'})
        job_id = job['job_id']
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            status = http(self.query + '/api/v1/jobs/' + job_id)
            if status['status'] == 'succeeded':
                rows = http(self.query + '/api/v1/jobs/' + job_id + '/result')['rows']
                assert rows == [{'n': self.expected}], rows
                self.event('batch_result', result='pass')
                return
            if status['status'] == 'failed':
                raise AssertionError(status)
            time.sleep(1)
        raise AssertionError('batch job did not finish in 60 seconds')

    def resources(self):
        ids = self.cmd('ps', '-aq').split()
        containers = json.loads(run_command(['docker', 'inspect', *ids], self.env))
        samples = []
        for c in containers:
            name = c['Config']['Labels']['com.docker.compose.service']
            state = c['State']
            samples.append(dict(service=name, state=state, memory_limit=c['HostConfig']['Memory'], restart_count=c['RestartCount'], image=c['Image']))
            assert not state['OOMKilled'], f'{name} OOM'
            assert c['RestartCount'] == 0, f'{name} unexpected restart'
            assert state['Running'] or (name == 'minio-init' and state['ExitCode'] == 0), f'{name} stopped'
            if name in ('query-server', 'compactor', 'ingester'):
                assert c['HostConfig']['Memory'] == 4 * 1024**3
        stats = run_command(['docker', 'stats', '--no-stream', '--format', '{{json .}}', *ids], self.env)
        backing = filesystem_capacity(self.docker_root_dir, self.env)
        with (self.out / 'resources.jsonl').open('a') as stream:
            stream.write(json.dumps(dict(at=utc(), docker_backing_filesystem=backing,
                                         containers=samples, stats=[json.loads(line) for line in stats.splitlines()])) + '\n')
        with urllib.request.urlopen(self.metrics + '/metrics', timeout=15) as response:
            with (self.out / 'metrics.prom').open('a') as stream:
                stream.write('# collected_at ' + utc() + '\n' + response.read().decode() + '\n')
        self.resource_samples += 1

    def restart(self, service):
        self.cmd('restart', service, timeout=120)
        self.refresh_endpoints()
        # Retry only transport/readiness, never silently swallow wrong answers.
        deadline = time.monotonic() + 120
        while True:
            try:
                urllib.request.urlopen(self.query + '/healthz', timeout=10).close()
                break
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
                if time.monotonic() >= deadline:
                    raise
                time.sleep(2)
        self.oracle()
        self.event('restart_persistence', service=service, result='pass')

    def execute(self):
        self.prepare()
        self.refused(self.ingest + '/v1/logs', {'resourceLogs': []}, 401)
        self.refused(self.query + '/api/v1/sql', {'query': 'SELECT 1'}, 401)
        self.event('authentication_refusal', result='pass')
        self.start = time.monotonic()
        self.summary['workload_started_at'] = utc()
        self.save()
        last_resource = last_restart = self.start
        checkpoint = False
        while time.monotonic() - self.start < DURATIONS[self.args.profile]:
            self.ingest_and_check()
            now = time.monotonic()
            if self.cycles == 1 or now - last_resource >= 60:
                self.resources()
                self.batch()
                last_resource = now
            if now - last_restart >= 3600:
                self.restart('query-server')
                self.restart('compactor')
                last_restart = now
            if not checkpoint and now - self.start >= 86400:
                self.event('24h_checkpoint', result='pass', elapsed_seconds=now-self.start, cleanup='pending')
                checkpoint = True
            time.sleep(10)
        for service in ('query-server', 'ingester', 'compactor'):
            self.restart(service)
        self.batch()
        self.resources()
        checks = [
            'anonymous product image pull',
            'fresh isolated install',
            'unauthenticated refusal',
            'exact committed IDs per cohort',
            'exact total and grouped counts',
            'filtered counts under concurrent queries',
            'batch query result',
            'persistence across component restart',
            'no OOM or unexpected restarts',
            'verified resource limits',
        ]
        if self.args.version in PROVENANCE_VERSIONS:
            checks.insert(1, 'engine/operator provenance matches release tag')
        self.summary.update(status='passed', workload_elapsed_seconds=time.monotonic()-self.start,
                            checks=checks, resource_samples=self.resource_samples)

    def cleanup(self):
        if self.mutated:
            try:
                (self.out / 'containers.log').write_text(self.cmd('logs', '--no-color', '--timestamps', timeout=60))
            except Exception as error:
                self.summary['log_collection_error'] = str(error)
            try:
                self.cmd('down', '--volumes', '--remove-orphans', '--timeout', '30', timeout=180)
                remains = {}
                for resource in ('container', 'network', 'volume'):
                    remains[resource] = run_command(['docker', resource, 'ls', '-q', '--filter', 'label=com.docker.compose.project=' + self.project], self.env).split()
                self.summary['remaining_resources'] = remains
                if any(remains.values()):
                    raise RuntimeError('run-owned Docker resources remain')
                self.summary['cleanup'] = 'passed'
            except Exception as error:
                self.summary['cleanup'] = 'failed'
                self.summary['cleanup_error'] = str(error)
                self.summary['status'] = 'failed'
        else:
            self.summary['cleanup'] = 'not_needed'
        self.summary['finished_at'] = utc()
        self.save()
        hashes = {}
        for path in self.out.iterdir():
            if path.is_file() and path.name != 'sha256.json':
                hashes[path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
        write_json(self.out / 'sha256.json', hashes)


def argument_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--version', required=True, choices=SUPPORTED_VERSIONS)
    parser.add_argument('--profile', required=True, choices=DURATIONS)
    parser.add_argument('--dependency-policy', choices=['public', 'cached-diagnostic'], default='public', help='cached-diagnostic uses existing dependency images; NEVER qualifies anonymous clean install')
    parser.add_argument('--out', required=True, help='new, durable results directory; must not already exist')
    return parser


def main():
    args = argument_parser().parse_args()
    run = Run(args)
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f'signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    try:
        run.execute()
    except KeyboardInterrupt as error:
        run.summary.update(status='interrupted', error=str(error))
    except Exception as error:
        run.summary.update(status='failed', error=f'{type(error).__name__}: {error}')
        run.event('failure', error=run.summary['error'])
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        run.cleanup()
    print(json.dumps(run.summary, indent=2))
    return 0 if run.summary['status'] == 'passed' else 1


if __name__ == '__main__':
    sys.exit(main())
