# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Independent full enumeration of the compiled native cost-choice case."""
import csv
import importlib.util
import json
from pathlib import Path
import sys

spec = importlib.util.spec_from_file_location('region_oracle', Path(__file__).with_name('oracle.py'))
oracle = importlib.util.module_from_spec(spec)
spec.loader.exec_module(oracle)
PROFILES = ('aggregation-heavy', 'join-heavy')
BOUNDARY = {('csv/main', 'LS/main'), ('LE/main', 'output/main')}
COMPILER_CUTS = {
    ('in-join-raw/build', 'in-join-raw/probe'),
    ('in-join-summary/build', 'in-join-summary/probe'),
    ('in-aggregate-total/globalAgg', 'in-join-summary/build'),
    ('sort-raw/main', 'union/main'),
}
OPTIONAL = {
    ('LS/main', 'filter/main'), ('LS/main', 'in-aggregate-total/localAgg'),
    ('LS/main', 'in-join-raw/probe'), ('filter/main', 'in-join-raw/build'),
    ('filter/main', 'in-join-summary/probe'),
    ('in-aggregate-total/localAgg', 'in-aggregate-total/globalAgg'),
    ('in-join-raw/probe', 'sort-raw/main'),
    ('in-join-summary/probe', 'union/main'), ('union/main', 'LE/main'),
}


def rows(folder, name):
    with (folder/name).open(encoding='utf-8-sig', newline='') as f:
        return list(csv.DictReader(f))


def verify(root):
    meta = json.loads((root/'metadata.json').read_text(encoding='utf-8-sig'))
    assert meta['pruning'] is True
    assert meta['repetitions'] >= 1
    folder = root/'fork-0'
    ops, links, samples = (rows(folder, name) for name in ('operators.csv','edges.csv','results.csv'))
    assert {r['case'] for r in ops} == set(PROFILES)
    assert {r['case'] for r in links} == set(PROFILES)
    assert {r['case'] for r in samples} == set(PROFILES)
    assert (folder/(PROFILES[0]+'.logical.json')).read_bytes() == (folder/(PROFILES[1]+'.logical.json')).read_bytes()
    graphs, optimum = {}, {}
    for profile in PROFILES:
        oo = [r for r in ops if r['case'] == profile]
        ee = [r for r in links if r['case'] == profile]
        weights = {r['operator']:float(r['estimate']) for r in oo}
        assert len(oo) == len(weights) == 13
        assert {r['operator'] for r in oo if r['inside']=='false'} == {'csv/main','output/main'}
        for op in oo:
            heavy = ('-aggregate-' if profile == 'aggregation-heavy' else '-join-') in op['operator']
            assert float(op['estimate']) == (8 if heavy else 1), 'unexpected weight'
            if op['operator'] != 'csv/main':
                assert 'Schema[' in op['input_schema'], 'missing input schema'
            assert 'Schema[' in op['output_schema'], 'missing output schema'
        pairs = [(r['from'],r['to']) for r in ee]
        assert len(ee) == len(set(pairs)) == 15
        assert set(pairs) == BOUNDARY | COMPILER_CUTS | OPTIONAL, 'unexpected compiled graph'
        edges = [(r['from'],r['to'],r['required']=='true') for r in ee]
        for r in ee:
            pair = (r['from'],r['to'])
            assert (r['compiler_required']=='true') == (pair in COMPILER_CUTS)
            assert (r['compiler_output_blocking']=='true') == (pair in COMPILER_CUTS)
            assert (r['required']=='true') == (pair in COMPILER_CUTS | BOUNDARY)
            join_internal = pair[0].endswith('/build')
            agg_internal = pair[0].endswith('/localAgg')
            assert (r['output_internal']=='true') == (join_internal or agg_internal)
            assert (r['input_internal']=='true') == (join_internal or agg_internal)
            assert int(r['input_port']) == (1 if pair in {
                ('LS/main','in-join-raw/probe'), ('filter/main','in-join-summary/probe')
            } else 0)
            assert r['output_port'] == '0'
        required = {i for i,e in enumerate(edges) if e[2]}
        optional = [i for i,e in enumerate(edges) if not e[2]]
        assert len(optional) == 9
        best, winners, valid = float('inf'), [], 0
        for mask in range(1 << len(optional)):
            cuts = required | {i for bit,i in enumerate(optional) if mask & (1 << bit)}
            cost = oracle.cost_of(weights, weights, edges, cuts)
            if cost is None:
                continue
            valid += 1
            if cost < best:
                best, winners = cost, [cuts]
            elif cost == best:
                winners.append(cuts)
        assert len(winners) == 1, 'not a unique minimum'
        assert best == (13 if profile == 'aggregation-heavy' else 28)
        selected = winners[0]
        seen = set()
        for r in (r for r in samples if r['case'] == profile):
            key = int(r['repetition']), r['strategy']
            assert key not in seen, 'duplicate sample'
            seen.add(key)
            assert r['fork'] == '0'
            assert r['order'] == ('XY' if int(r['repetition']) % 2 == 0 else 'YX')
            cuts = {int(x) for x in r['cuts'].split(';') if x}
            assert cuts == selected, 'planner did not select the unique minimum'
            assert float(r['cost']) == best == oracle.cost_of(weights, weights, edges, cuts)
            assert int(r['total_ns']) == sum(int(r[k]) for k in ('decomposition_ns','search_ns','composition_ns'))
        assert seen == {(i,s) for i in range(meta['repetitions']) for s in ('X','Y')}, 'missing sample'
        graphs[profile] = (weights, edges, oo, ee)
        optimum[profile] = dict(cost=best, cuts=sorted(selected),
            optional_cuts=[list(pairs[i]) for i in sorted(selected-required)],
            assignments=1 << len(optional), feasible=valid, unique=True)
    a, b = (graphs[p] for p in PROFILES)
    # Ordered manifests must agree before transferring any cut indices between profiles.
    assert [{k:v for k,v in r.items() if k not in ('case','estimate')} for r in a[2]] == [
        {k:v for k,v in r.items() if k not in ('case','estimate')} for r in b[2]]
    assert [{k:v for k,v in r.items() if k != 'case'} for r in a[3]] == [
        {k:v for k,v in r.items() if k != 'case'} for r in b[3]]
    table = [[oracle.cost_of(g[0], g[0], g[1], set(optimum[p]['cuts']))
              for g in (a,b)] for p in PROFILES]
    assert table == [[13,34],[14,28]], 'no strict cost-ranking reversal'
    return dict(profiles=optimum, cost_table=table, checked_samples=len(samples),
                assignments=sum(o['assignments'] for o in optimum.values()),
                scope='fixed-estimate planning correctness; no execution-time or timing-winner claim')


if __name__ == '__main__':
    root = Path(sys.argv[1])
    result = verify(root)
    (root/'verification.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8',newline='\n')
    print(json.dumps(result,indent=2))
