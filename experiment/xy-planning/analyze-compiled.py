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


"""Verify compiled-graph plans independently, then summarize pruning-on paired timings."""
import csv
import importlib.util
import json
from pathlib import Path
from statistics import median
import sys


def rows(path):
    with path.open(encoding='utf-8-sig', newline='') as stream:
        return list(csv.DictReader(stream))


def analyze(root):
    spec = importlib.util.spec_from_file_location('xy_oracle', Path(__file__).with_name('oracle.py'))
    oracle = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(oracle)
    metadata = json.loads((root/'metadata.json').read_text(encoding='utf-8-sig'))
    expected_cases = set(metadata.get('cases', ['kmeans','files','branching-files']))
    native = metadata.get('suite', '').startswith('native')
    large = metadata.get('suite') == 'native-large'
    module_spec = importlib.util.spec_from_file_location('module_oracle', Path(__file__).with_name('module-oracle.py'))
    module_oracle = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module_oracle)
    all_rows, graphs = [], {}
    for fork in range(metadata['forks']):
        folder = root/f'fork-{fork}'
        ops, edge_rows = rows(folder/'operators.csv'), rows(folder/'edges.csv')
        assert {r['case'] for r in ops} == expected_cases
        current = {}
        for name in sorted({r['case'] for r in ops}):
            selected_ops = sorted((r for r in ops if r['case'] == name), key=lambda r:r['operator'])
            selected_edges = [r for r in edge_rows if r['case'] == name]
            current[name] = (selected_ops, selected_edges)
        if graphs:
            assert current == graphs, 'compiled graphs changed across JVMs'
        graphs = current
        measurements = rows(folder/'results.csv')
        assert len(measurements) == len(expected_cases) * 2 * metadata['repetitions']
        assert all(int(r['fork']) == fork for r in measurements)
        assert len({(r['case'], r['repetition'], r['strategy']) for r in measurements}) == len(measurements)
        all_rows.extend(measurements)
    summary, verification = [], []
    lines = ['# Compiled X/Y planning comparison', '',
        f"Pruning on; {len(expected_cases)} strictly compiled {'native-operator' if native else 'saved demo'} workflows; alternating X/Y order in each fresh JVM.",
        f"{metadata['forks']} JVMs, {metadata['warmups']} warm-up pairs and {metadata['repetitions']} measured pairs per workflow per JVM.",
        'Times include Y decomposition and flat-plan reconstruction. Compilation, exhaustive reference search and result validation are excluded.', '',
        '| Workflow | Operators / edges | X median ms | Y median ms | Y/X paired median | Cost X = Y | States X / Y |',
        '|---|---:|---:|---:|---:|---:|---:|']
    for name, (ops, link_rows) in graphs.items():
        weights = {r['operator']:float(r['estimate']) for r in ops}
        assert len(weights) == len(ops), 'physical stages collapsed into one ID'
        links = [(r['from'],r['to'],r['required']=='true') for r in link_rows]
        assert all(a in weights and b in weights for a,b,_ in links)
        required = {i for i,e in enumerate(links) if e[2]}
        if large:
            inside = {r['operator'] for r in ops if r['inside'] == 'true'}
            boundary = {i for i,(a,b,_) in enumerate(links) if (a in inside) != (b in inside)}
            compiler_required = {i for i,r in enumerate(link_rows) if r['compiler_required'] == 'true'}
            assert required == compiler_required | boundary, 'non-compiler internal cut added'
            independent = module_oracle.minimum(weights,links,inside)
        else:
            independent = module_oracle.enumerate_minimum(weights,links)
            independent['method'] = 'whole-graph exhaustive enumeration'
        best = independent['minimum_cost']
        measures = [r for r in all_rows if r['case'] == name]
        for r in measures:
            cuts = {int(i) for i in r['cuts'].split(';') if i}
            assert required <= cuts <= set(range(len(links)))
            assert oracle.cost_of(weights, weights, links, cuts) == float(r['cost']) == best
            assert int(r['total_ns']) > 0
            assert sum(int(r[k]) for k in ('decomposition_ns','search_ns','composition_ns')) == int(r['total_ns'])
        pairs = {(r['fork'],r['repetition'],r['strategy']):r for r in measures}
        ratios, fork_details = [], []
        for fork in range(metadata['forks']):
            this_ratios = []
            for rep in range(metadata['repetitions']):
                x,y = [pairs[(str(fork),str(rep),s)] for s in ('X','Y')]
                assert x['order'] == y['order']
                assert x['order'] == ('XY' if (rep+fork)%2 == 0 else 'YX')
                this_ratios.append(int(y['total_ns'])/int(x['total_ns']))
            ratios.extend(this_ratios)
            fork_details.append({'fork':fork, 'paired_y_over_x':median(this_ratios),
                **{s+'_median_ms':median(int(r['total_ns'])/1e6 for r in measures if r['fork']==str(fork) and r['strategy']==s) for s in ('X','Y')}})
        medians = {s:median(int(r['total_ns'])/1e6 for r in measures if r['strategy']==s) for s in ('X','Y')}
        states = {s:sorted({int(r['search_states']) for r in measures if r['strategy']==s}) for s in ('X','Y')}
        state_text = {s:(v[0] if len(v)==1 else f'{min(v)}-{max(v)}') for s,v in states.items()}
        winner = 'X' if all(f['paired_y_over_x']>1.1 for f in fork_details) else ('Y' if all(f['paired_y_over_x']<1/1.1 for f in fork_details) else 'close/mixed')
        result = dict(case=name,operators=len(ops),edges=len(links),x_ms=medians['X'],y_ms=medians['Y'],paired_y_over_x=median(ratios),cost=best,x_states=state_text['X'],y_states=state_text['Y'],winner=winner)
        summary.append(result)
        verification.append(dict(case=name,assignments=independent['assignments'],independent_oracle=independent,minimum_cost=best,verified_plans=len(measures),forks=fork_details,
            y_phase_median_ms={k:median(int(r[k])/1e6 for r in measures if r['strategy']=='Y') for k in ('decomposition_ns','search_ns','composition_ns')},
            gc_affected_plans=sum(int(r['gc_count'])>0 for r in measures)))
        lines.append(f"| {name} | {len(ops)} / {len(links)} | {medians['X']:.4f} | {medians['Y']:.4f} | {median(ratios):.2f} | {best:g} | {state_text['X']} / {state_text['Y']} |")
    graph_scope = ([
        ('Native CSV, aggregate, hash-join and sort descriptors are strictly compiled. Each group branches into an aggregate and raw rows, then joins the statistic back to those rows. Groups occur before and inside the loop. The source CSV is real and schemas are inferred; no workflow data processing is timed. Saved logical JSON and physical manifests record the full configured graphs.' if large else
         'Native CSV, filter, aggregate, sort and hash-join descriptors are strictly compiled. The source CSV is real and schemas are inferred; no workflow data processing is timed. Saved logical JSON and physical manifests record the full configured graphs.'),
        ('The compiler supplies aggregate global-stage and hash-join build blocking outputs and the join input dependency. The sort is a sink, so its blocking output creates no outgoing cut. The local aggregate output deliberately remains unmarked as in the code. Only the two external loop-boundary cuts are added by the research adapter.' if large else
         'The compiler supplies all internal blocking ports/dependencies: aggregate global-stage outputs, sort outputs and hash-join build outputs. The local aggregate output deliberately remains unmarked as in the code. Only the two external loop-boundary cuts are added by the research adapter.'),
        'No native K-means descriptor exists in this checkout. These are file-summary and summary-join workflows, not a native K-means benchmark. Physical layer names distinguish local/global aggregate and build/probe join stages in the oracle.'
    ] if native else [
        'The two saved demos use Python UDFs. The file demo has embedded filename/text records, not native Read File or a control-variable port; the branching variant counts words and characters on two branches that merge at LoopEnd.',
        'Both original demos terminate at LoopEnd. Only the incoming loop boundary is added as a required cut. The compiler marks no UDF output as blocking, including the table-based K-means UDF; no artificial internal blocking edge is added.'
    ])
    lines += ['', '## Interpretation and scope', '',
        'Inspect the remaining search states and decomposition/reconstruction costs before generalizing the timing result. Physical graph size and real blocking dependencies matter together.',
        'Cost is the sum of the maximum fixed operator estimate in each region, with every original physical operator set to 1 and the collapsed macro to 0. These are normalized model costs, not measured runtime or materialized bytes.',
        *graph_scope,
        'Loop operators still carry requiresMaterializedExecution=true. The research harness directly calls PASTA search; the normal main dispatcher forces full materialization. These are planner timings, not a claim that main can execute the returned pipelined loop plans.',
        'Compiled executors, schemas, partition requirements and operator flags are retained in Y, apart from the explicit synthetic zero-cost macro. GUI result-view storage settings are excluded from this topology/fixed-cost comparison.',
        ('Each selected plan was checked by the production evaluator and an independent connected-component/Kahn oracle. The oracle exhaustively enumerates each sealed module, sums its exact minima and validates the composed plan on the full graph. It checks single entry/exit, mandatory boundary cuts and all body nodes on entry-to-exit paths. Full-graph enumeration also cross-checks cases with at most 14 optional edges. Assignment counts include both module searches and any full cross-check; larger cases are not globally exhaustively enumerated.' if large else
         'Each selected plan was checked by the production evaluator and an independent connected-component/Kahn oracle. The oracle enumerates all edge assignments to confirm the minimum.'),
        'The per-JVM >10% paired-median rule labels an operational timing difference, not statistical significance. No outliers or GC samples were removed.', '',
        '## Per-JVM paired medians', '']
    for item in verification:
        lines.append(f"- {item['case']}: " + '; '.join(f"JVM {f['fork']}: X {f['X_median_ms']:.4f} ms, Y {f['Y_median_ms']:.4f} ms, paired Y/X {f['paired_y_over_x']:.2f}" for f in item['forks']))
    (root/'REPORT.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')
    (root/'verification.json').write_text(json.dumps(verification,indent=2)+'\n',encoding='utf-8')
    with (root/'summary.csv').open('w',newline='',encoding='utf-8') as stream:
        writer = csv.DictWriter(stream,fieldnames=list(summary[0]))
        writer.writeheader(); writer.writerows(summary)
    print('\n'.join(lines[:12]))
    print(f"Verified {len(all_rows)} selected plans and {sum(r['assignments'] for r in verification)} independent assignments (see method per case).")


if __name__ == '__main__':
    analyze(Path(sys.argv[1]))
