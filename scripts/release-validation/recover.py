#!/usr/bin/env python3
"""Idempotent cleanup after SIGKILL, timeout or host reboot. Requires Docker access."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import datetime


def recover(out):
    out = Path(out).resolve()
    summary_path = out/'summary.json'
    if not summary_path.exists():
        return 0  # runner never started
    summary = json.loads(summary_path.read_text())
    if summary.get('cleanup') in ('passed', 'not_needed'):
        return 0
    project = summary['project']
    if not re.fullmatch(r'llvalidate-[0-9a-f]{12}', project):
        raise ValueError('refusing an unrecognized project identity')
    config = out/'compose.json'
    if not config.exists():
        summary['cleanup'] = 'not_needed'
    else:
        if hashlib.sha256(config.read_bytes()).hexdigest() != summary['compose_sha256']:
            raise ValueError('refusing cleanup with a changed compose configuration')
        doc = json.loads(config.read_text())
        if 'name' in doc or any('name' in v or v.get('external') for v in doc.get('volumes', {}).values()):
            raise ValueError('refusing global/external resources')
        command = ['docker', 'compose', '-p', project, '-f', str(config)]
        with (out/'recovery.log').open('a') as log:
            subprocess.run(command+['logs', '--no-color', '--timestamps'], stdout=log, stderr=log, timeout=60, check=False)
            result = subprocess.run(command+['down', '--volumes', '--remove-orphans', '--timeout', '30'], stdout=log, stderr=log, timeout=180)
        remains = {}
        for resource in ('container', 'network', 'volume'):
            remains[resource] = subprocess.check_output(['docker', resource, 'ls', '-q', '--filter', 'label=com.docker.compose.project='+project], text=True, timeout=30).split()
        summary['remaining_resources'] = remains
        summary['cleanup'] = 'passed' if result.returncode == 0 and not any(remains.values()) else 'failed'
    if summary['status'] == 'running':
        summary['status'] = 'interrupted'
    if summary['cleanup'] == 'failed':
        summary['status'] = 'failed'
    summary['recovered_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    tmp = out/'summary.tmp'
    tmp.write_text(json.dumps(summary, indent=2)+'\n'); tmp.replace(summary_path)
    hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in out.iterdir() if p.is_file() and p.name != 'sha256.json'}
    (out/'sha256.json').write_text(json.dumps(hashes, indent=2)+'\n')
    return 0 if summary['cleanup'] != 'failed' else 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('out')
    sys.exit(recover(parser.parse_args().out))
