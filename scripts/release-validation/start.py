#!/usr/bin/env python3
"""Start a duration-bounded user service; tmux/client loss cannot stop the run."""
import argparse
import datetime
import grp
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import uuid

HERE = Path(__file__).resolve().parent
p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--version', required=True, choices=['v0.1.0', 'v0.2.0', 'v0.2.1'])
p.add_argument('--profile', required=True, choices=['smoke', '24h', '72h'])
p.add_argument('--dependency-policy', default='public', choices=['public', 'cached-diagnostic'])
p.add_argument('--results-root', required=True)
a = p.parse_args()
run_id = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:8]
root = Path(a.results_root).resolve(); root.mkdir(parents=True, exist_ok=True)
out = root / (a.version+'-'+a.profile+'-'+run_id)
unit = 'siglake-validation-'+run_id
# Run a committed immutable worktree; changing files under a 72h process makes
# the evidence hard to reproduce. Copies are unnecessary if HEAD is clean.
repo = HERE.parents[1]
if subprocess.check_output(['git', '-C', str(repo), 'status', '--porcelain', '--untracked-files=no'], text=True).strip():
    p.error('commit harness changes before starting a durable run')
head = subprocess.check_output(['git', '-C', str(repo), 'rev-parse', 'HEAD'], text=True).strip()
snapshot = root/('harness-'+head[:12])
if not snapshot.exists():
    subprocess.run(['git', '-C', str(repo), 'worktree', 'add', '--detach', str(snapshot), head], check=True)
runner = snapshot/'scripts/release-validation'
command = [sys.executable, str(runner/'run.py'), '--version', a.version, '--profile', a.profile,
           '--dependency-policy', a.dependency_policy, '--out', str(out)]
recovery = [sys.executable, str(runner/'recover.py'), str(out)]
# User services inherit the user manager's old group list. sg supplies Docker
# access without restarting the user's other services.
def docker_access(argv):
    try:
        grp.getgrnam('docker')
    except KeyError:
        return argv
    return ['sg', 'docker', '-c', shlex.join(argv)]

# systemd parses quotes itself; shlex.join safely protects each fixed argument.
post = shlex.join(docker_access(recovery)).replace('%', '%%')
hours = {'smoke': 1, '24h': 26, '72h': 74}[a.profile]
args = ['systemd-run', '--user', '--collect', '--unit', unit,
        '--property=RuntimeMaxSec='+str(hours*3600), '--property=TimeoutStopSec=300',
        '--property=ExecStopPost='+post, '--property=Restart=no',
        '--property=StandardOutput=append:'+str(root/(unit+'.log')),
        '--property=StandardError=append:'+str(root/(unit+'.log')),
        *docker_access(command)]
subprocess.run(args, check=True)
(root/(unit+'.json')).write_text(json.dumps(dict(unit=unit, out=str(out), harness_commit=head, command=command), indent=2)+'\n')
print(json.dumps(dict(unit=unit, results=str(out), log=str(root/(unit+'.log'))), indent=2))
