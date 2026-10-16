"""Preserve the preceding audit and seed its v1.50.0 inventory review.

Usage: python3 .compatibility/reset-handler/refresh.py BASELINE CANDIDATE
The compressed outputs are immutable; do not overwrite a reviewed audit.
"""
import collections
import gzip
import importlib.util
import json
from pathlib import Path
import sys

out = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('gate', '/Users/maxim/.codex/skills/sdk-go-compatibility/scripts/gate.py')
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)
baseline, candidate = [json.loads(Path(p).read_text()) for p in sys.argv[1:]]
old = json.loads((out.parent/'matrix.json').read_text())
previous = {e['id']: e for e in old['entries']}
entries, added, changed = [], [], []
for item in baseline['items']:
    before = previous.get(item['id'])
    entry = dict(id=item['id'], fingerprint=item['fingerprint'], status='pending', target='',
                 trace=item['source']+':'+str(item['line']), reason='New upstream declaration requires review.', evidence=[], cases={})
    if before:
        if before['fingerprint'] == item['fingerprint']:
            entry = dict(before)
            if entry['status'] == 'needs_decision' and 'WorkflowPanicPolicy' not in item['id']:
                entry = dict(id=item['id'], fingerprint=item['fingerprint'], status='unverified', target=before['target'],
                    trace='worker/worker.go; worker/plugin_test.go; example/plugins/check/main.go',
                    reason='User authorized native plugin registry routing; routing is fixed and tested. Exhaustive plugin lifecycle/field evidence remains incomplete.',
                    evidence=[], cases={}, previous_assessment=before)
        else:
            changed.append(item['id'])
            entry['reason'] = 'Upstream declaration changed; previous assessment retained for review.'
            entry['previous_assessment'] = before
    else:
        added.append(item['id'])
    entries.append(entry)
ids = {e['id'] for e in entries}
removed = [dict(entry=e, review_status='pending', reason='Review removed or relocated upstream declarations; no scope exclusion inferred.') for e in old['entries'] if e['id'] not in ids]
matrix = dict(schema=1, baseline_identity=gate.identity(baseline), candidate_identity=gate.identity(candidate),
    reviews={k:'' for k in gate.REVIEWS}, external_reviews={}, entries=entries, runs={},
    previous_matrix='../matrix.json', removed_baseline_entries=removed,
    note='Targeted reset/plugin validation is recorded separately in VALIDATION.md and logs. No successful full compatibility status is assigned from package passes.')
for name, data in [('baseline',baseline),('candidate',candidate),('matrix',matrix)]:
    with (out/(name+'.json.gz')).open('xb') as f:
        f.write(gzip.compress((json.dumps(data,indent=2)+'\n').encode(),mtime=0))
(out/'inventory-drift.json').write_text(json.dumps(dict(baseline_items=len(baseline['items']),candidate_items=len(candidate['items']),
    statuses=dict(collections.Counter(e['status'] for e in entries)),added_ids=added,changed_ids=changed,
    removed_ids=[e['entry']['id'] for e in removed],baseline_issues=baseline['issues'],candidate_issues=candidate['issues']),indent=2)+'\n')
