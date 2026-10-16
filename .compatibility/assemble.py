"""Assemble this audit without promoting source matches to behavioral proof.

Run from the POC root after generating the raw inventories. Refuses to replace
an existing matrix: retain reviewed evidence and review drift on later audits.
"""
import collections
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / '.compatibility'
SKILL = Path('/Users/maxim/.codex/skills/sdk-go-compatibility')
spec = importlib.util.spec_from_file_location('gate', SKILL / 'scripts/gate.py')
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)

def read(name):
    return json.loads((OUT / name).read_text())

def write(name, data):
    (OUT / name).write_text(json.dumps(data, indent=2) + '\n')

def supplemental(inv, root, module):
    """Discover literal environment keys and flag registration sites.

    This does not infer dynamically computed names, defaults, or semantics.
    Setters/functional options already have normal declaration rows.
    """
    pattern = re.compile(r'\b(?:os\.(?:Getenv|LookupEnv)\s*\(\s*"[^"\n]+"|flag\.[A-Z]\w*\s*\([^\n]*)')
    for path in sorted(root.rglob('*.go')):
        relative = path.relative_to(root)
        if any(p.startswith('.') or p in ('vendor', 'testdata') for p in relative.parts) or path.name.endswith('_test.go'):
            continue
        for line, text in enumerate(path.read_text().splitlines(), 1):
            match = pattern.search(text)
            if not match:
                continue
            source = module + '/' + relative.as_posix()
            key = 'supplemental:' + source + ':' + str(line)
            signature = text.strip()
            inv['items'].append(dict(id=key, kind='configuration-path', signature=signature,
                source=source, line=line, fingerprint=hashlib.sha256((key+'\n'+signature).encode()).hexdigest()))
    inv['items'].sort(key=lambda i: i['id'])
    return inv

baseline = supplemental(read('baseline-resolved.json'), Path('/tmp/sdk-go-official-audit-v1.49.0'), 'go.temporal.io/sdk')
candidate = supplemental(read('candidate-final.json'), ROOT, 'github.com/mfateev/sdk-go-poc')
write('baseline.json', baseline)
write('candidate.json', candidate)
subprocess.run(['python3', str(SKILL/'scripts/gate.py'), 'init', '--inventory', str(OUT/'baseline.json'),
    '--candidate-inventory', str(OUT/'candidate.json'), '--matrix', str(OUT/'matrix.json')], check=True)
matrix = read('matrix.json')
candidates = {i['id'].split('@')[0]:i for i in candidate['items']}
host = {i['id'].split('@')[0]:i for i in read('host-inventory.json')['items']}
metadata = read('metadata.json')
environment = dict(metadata=metadata, cache='default unless explicitly recorded', tags='default')

def record(run_id, command, log, exit_file, cwd=None, toolchain=None, synthetic_tests=None):
    path = OUT / 'logs' / log
    tests = {}
    for line in path.read_text().splitlines():
        try:
            event = json.loads(line)
        except (ValueError, TypeError):
            continue
        if event.get('Test') and event.get('Action') in ('pass','fail','skip'):
            tests[event['Package']+'/'+event['Test']] = {'pass':'passed','fail':'failed','skip':'skipped'}[event['Action']]
    if synthetic_tests:
        tests.update(synthetic_tests)
    code = int((OUT/'logs'/exit_file).read_text())
    matrix['runs'][run_id] = dict(baseline_identity=gate.identity(baseline), candidate_identity=gate.identity(candidate),
        command=command, cwd=cwd or str(ROOT), toolchain=toolchain or metadata['runtime']['tool_version'],
        environment=environment, exit_code=code, result='passed' if code==0 else 'failed',
        log='logs/'+log, log_sha256=hashlib.sha256(path.read_bytes()).hexdigest(), tests=tests)

record('native-all','../golang-go/bin/tgo test -json ./...', 'native-all.jsonl','native-all.exit')
for run_id, name in [('plugin-control','TestPluginRegistrationControl'),('plugin-start','TestPluginStartNativeRegistration'),
                     ('plugin-replay','TestPluginReplayNativeRegistration'),('plugin-policy','TestPluginPanicPolicyMutation'),
                     ('activity-context','TestActivityContextOptionalBaselineComparison')]:
    record(run_id, '../golang-go/bin/tgo test -json ./.compatibility/probes -run '+name,run_id+'.jsonl',run_id+'.exit')
record('official-focused', "go run . unit-test -run 'Test(GetActivityOptions|GetLocalActivityOptions|GetChildWorkflowOptions|PayloadLimitOptionsToLimits|.*Plugin.*)$'",
       'official-focused.jsonl','official-focused.exit', '/tmp/sdk-go-official-audit-v1.49.0/internal/cmd/build', metadata['standard_go'])
record('official-tgo-compile', "tgo test -json -run '^$' ./...",'official-tgo-compile.jsonl','official-tgo-compile.exit','/tmp/sdk-go-official-audit-v1.49.0')
matrix['checks'] = {'official-tgo-compile': matrix['runs'].pop('official-tgo-compile')}
record('dynamic-live', '.compatibility/dynamic-check -live -history-dir .compatibility/live-histories',
       'dynamic-live.log','dynamic-live.exit', synthetic_tests={
           'dynamic/live/cache1000': 'passed','dynamic/live/cache0':'passed','dynamic/replay/recorded-histories':'passed'})
matrix['runs']['dynamic-live']['test_result_provenance'] = 'Named scenarios from inspected main.go assertions and success sentinels; checker exits on any failed assertion. This is not go test output.'
matrix['reviews'] = dict(inventory_completeness='', build_variants='', configuration_paths='')
matrix['review_notes'] = dict(
    inventory_completeness='REPORT.md: zero unresolved parser/alias issues after exact dependency supplements; external configuration expansion and promoted method sets remain incomplete.',
    build_variants='Official root compiled with tgo (Go 1.28) and selected standard-Go 1.26 race tests. Other OS/arch, tags, independently-versioned module configurations remain unverified.',
    configuration_paths='assemble.py deterministically adds literal os.Getenv/LookupEnv and flag registration sites. Dynamic keys, callback parameter graphs and documented-only configuration remain incomplete.')

for item, entry in zip(baseline['items'], matrix['entries']):
    assert item['id']==entry['id']
    api = item['id'].split('@')[0]
    adapted_api = api.replace('go.temporal.io/sdk/','github.com/mfateev/sdk-go-poc/',1)
    target = candidates.get(adapted_api)
    retained = host.get(api)
    if target:
        entry.update(status='unverified', target=adapted_api,
            trace=target['source']+':'+str(target['line']),
            reason='Candidate declaration exists. Source mapping is not proof of default/explicit/invalid/unset/interaction behavior or preservation across the isolate boundary.')
        entry['mapping_kind']='native-or-adapted-candidate'
    elif retained:
        entry.update(status='unverified', target=api+' [resolved pinned host SDK]',
            trace=retained['source']+':'+str(retained['line']),
            reason='Export exists in the actual host fork. POC integration, wrapper reachability and each field/hook contract require semantic evidence.')
        entry['mapping_kind']='host-declaration-only'
    else:
        entry.update(status='pending', target='unmapped',trace=item['source']+':'+str(item['line']),
            reason='No direct declaration match in candidate or root host inventory. Check native equivalents and independently versioned contrib modules; absence of a name alone is not an unsupported-behavior finding.')
    if api.startswith('supplemental:'):
        entry.update(status='pending',target='configuration path requires manual trace',trace=item['source']+':'+str(item['line']),reason='Literal configuration site discovered; validation/defaults/consumption and relevance remain unverified.')
    plugin = any(s in api for s in ['worker.Plugin.StartWorker','worker.Plugin.ReplayWorkflow',
        'worker.PluginStartWorkerOptions.WorkerRegistry','worker.PluginReplayWorkflowOptions.WorkflowReplayRegistry',
        'worker.Options.WorkflowPanicPolicy'])
    if plugin:
        entry.update(status='needs_decision',reason='Native plugin registry bypass or post-plugin panic-policy capture differs from direct wrapper registration; see REPORT.md findings P1/P2.',
            question='Support native registration and effective post-plugin options, or explicitly reject the configurations?',
            conflicting_evidence=['worker/worker.go:36,135,231','upstream internal/plugin.go:108-116,216-232,285-300','.compatibility/probes/plugin_test.go'],
            alternatives=['Support native callbacks and honor effective mutated options','Reject these configurations explicitly'],
            decision_state={'reference':'user reply 2026-10-16 to audit plugin question','quote':'Leave the behavior decision open'},
            observations=[{'run':'plugin-start','test':'github.com/mfateev/sdk-go-poc/.compatibility/probes/TestPluginStartNativeRegistration'},
                          {'run':'plugin-replay','test':'github.com/mfateev/sdk-go-poc/.compatibility/probes/TestPluginReplayNativeRegistration'},
                          {'run':'plugin-policy','test':'github.com/mfateev/sdk-go-poc/.compatibility/probes/TestPluginPanicPolicyMutation'}])
    if api.startswith('go.temporal.io/sdk/workflow.SideEffect') or api.startswith('go.temporal.io/sdk/workflow.MutableSideEffect'):
        entry.update(status='unsupported',target='no native implementation', trace='API_COVERAGE_PLAN.md:17; SDK_REFERENCE.md Additional workflow APIs',
            reason='Official baseline exposes SideEffect/MutableSideEffect; the current native API deliberately omits them. No user-approved audit exclusion. Host equivalents remain available to ordinary SDK workflows.')
    if any(api.startswith('go.temporal.io/sdk/worker.'+name) for name in ['Worker.RegisterActivity','ActivityRegistry.RegisterActivity']):
        entry.update(status='unsupported',reason='POC rejects context-free activities accepted by the pinned host SDK. Documented restriction; no approved audit exclusion.',
            trace='internal/activityref/name.go:31; worker/worker.go:128',
            observations=[{'run':'activity-context','test':'github.com/mfateev/sdk-go-poc/.compatibility/probes/TestActivityContextOptionalBaselineComparison'}])

write('matrix.json',matrix)
counts = collections.Counter(e['status'] for e in matrix['entries'])
families={}
for label, predicate in {
    'options':lambda s:bool(re.search(r'(Options|Config|Policy|Tuner|Poller|Limits)',s)),
    'worker':lambda s:'/worker.' in s,
    'plugins':lambda s:'Plugin' in s,
    'contrib':lambda s:'/contrib/' in s,
}.items():
    entries=[e for e in matrix['entries'] if predicate(e['id'].split('@')[0])]
    families[label]=dict(total=len(entries),statuses=dict(collections.Counter(e['status'] for e in entries)))
write('summary.json',dict(baseline_items=len(baseline['items']),candidate_items=len(candidate['items']),
    baseline_ast_items=len(read('baseline-resolved.json')['items']),candidate_ast_items=len(read('candidate-final.json')['items']),
    statuses=dict(counts),families=families,external_types=len(set(baseline['external_references'])|set(candidate['external_references']))))
print(json.dumps(read('summary.json'),indent=2))
