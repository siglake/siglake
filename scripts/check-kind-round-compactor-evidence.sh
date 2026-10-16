#!/usr/bin/env bash
# Execute the actual pre-teardown collector with a local kubectl fixture.
set -euo pipefail
cd "$(dirname "$0")/.."
python3 - <<'PY'
import json, os, pathlib, subprocess, tempfile
source = pathlib.Path('scripts/kind-round.sh').read_text()
start = source.index('compactor_evidence_command() {')
end = source.index('\ncleanup() {', start)
functions = source[start:end]
with tempfile.TemporaryDirectory(prefix='siglake-compactor-capture-') as tmp:
    root = pathlib.Path(tmp)
    bin_dir = root/'bin'; bin_dir.mkdir()
    stub = bin_dir/'kubectl'
    stub.write_text('''#!/usr/bin/env python3
import json, os, sys
args = sys.argv[1:]
mode = os.environ['MODE']
if 'logs' in args:
    if mode == 'log-error':
        print('transport failure', file=sys.stderr); sys.exit(7)
    print('retained previous refusal' if '--previous' in args else 'retained current refusal')
elif '--raw' in args:
    print('ok' if args[-1] == '/readyz' else 'siglake_compactor_conditional_write_refused_total 1')
elif 'deployment' in args:
    print(json.dumps({'kind':'Deployment','metadata':{'name':'siglake-compactor'}}))
elif '-o' in args and args[args.index('-o')+1] == 'json':
    labels = {'app.kubernetes.io/instance':'siglake', 'app.kubernetes.io/component':'compactor'}
    if mode == 'wrong-label': labels['app.kubernetes.io/component'] = 'query'
    pod = {'metadata':{'name':'compactor-abc','namespace':'default','labels':labels}, 'status':{'phase':'Running','conditions':[{'type':'Ready','status':'True'}], 'containerStatuses':[{'name':'compactor','restartCount':1 if mode == 'restarted' else 0}]}}
    print(json.dumps({'items':[] if mode == 'no-pods' else [pod]}))
else:
    print('fixture diagnostic output')
''')
    stub.chmod(0o755)
    prelude = 'set -euo pipefail\nlog() { :; }\ndump_section() { :; }\narray_contains() { return 1; }\nKUBE_CONTEXT=kind-fixture\nCLUSTER_NAME=fixture\nNAMESPACE=default\n' + functions
    def run(mode, code='dump_cluster_state 0', result=None):
        result = result or root/mode
        env = dict(os.environ, PATH=str(bin_dir)+':'+os.environ['PATH'], MODE=mode, RESULTS_DIR=str(result))
        proc = subprocess.run(['bash','-c',prelude+'\n'+code],env=env,text=True,capture_output=True)
        assert proc.returncode == 0, proc.stderr
        return proc, result/'compactor-evidence'
    p,d=run('healthy')
    receipt=json.loads((d/'capture.json').read_text())
    assert receipt['status']=='capture_complete' and 'retained current refusal' in p.stdout
    assert not any(c['name'].endswith('-previous') for c in receipt['commands'])
    assert (d/'compactor-abc-metrics.stdout').read_text().endswith(' 1\n')
    assert 'FAILURE_DIAGNOSTICS_BEGIN' not in p.stdout
    p,d=run('restarted')
    receipt=json.loads((d/'capture.json').read_text())
    assert receipt['status']=='capture_complete' and 'retained previous refusal' in p.stdout
    assert (d/'compactor-abc-previous.stdout').is_file()
    for mode in ['log-error','no-pods','wrong-label']:
        p,d=run(mode)
        receipt=json.loads((d/'capture.json').read_text())
        assert receipt['status']=='capture_incomplete', mode
        if mode=='log-error': assert any(c['exit_code']==7 for c in receipt['commands'])
    p,d=run('failed-round',code='dump_cluster_state 9')
    assert 'FAILURE_DIAGNOSTICS_BEGIN status=9' in p.stdout
    assert json.loads((d/'capture.json').read_text())['status']=='capture_complete'
    before={p.name:p.read_bytes() for p in d.iterdir()}
    p,again=run('failed-round',result=d.parent)
    assert 'COMPACTOR_EVIDENCE_INCOMPLETE existing_or_unwritable=' in p.stdout
    assert before=={p.name:p.read_bytes() for p in again.iterdir()}
    print('PASS: seven compactor collection paths; healthy, restarted, three incomplete captures, failed round, repeat preservation')
PY
