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

"""Independent region-component/Kahn oracle for the saved generated workflows."""
import csv
import json
from collections import defaultdict
from pathlib import Path
import sys


def cost_of(ids, weights, edges, cuts):
    parent = {i:i for i in ids}
    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i
    for n, (a,b,_) in enumerate(edges):
        if n not in cuts:
            parent[find(a)] = find(b)
    maxima = {}
    for i in ids:
        r = find(i)
        maxima[r] = max(maxima.get(r,0),weights[i])
    arcs = {r:set() for r in maxima}
    incoming = {r:0 for r in maxima}
    for n in cuts:
        a,b,_ = edges[n]
        a,b = find(a),find(b)
        if a == b:
            return None
        if b not in arcs[a]:
            arcs[a].add(b)
            incoming[b] += 1
    ready = [r for r in maxima if incoming[r] == 0]
    seen = 0
    while ready:
        r = ready.pop()
        seen += 1
        for other in arcs[r]:
            incoming[other] -= 1
            if incoming[other] == 0:
                ready.append(other)
    return sum(maxima.values()) if seen == len(maxima) else None


def verify(root):
    folder = root/'fork-0'
    costs = defaultdict(set)
    for file in root.glob('fork-*/results.csv'):
        for r in csv.DictReader(file.open()):
            costs[r['case']].add(float(r['cost']))
    weights = defaultdict(dict)
    edges = defaultdict(list)
    for r in csv.DictReader((folder/'operators.csv').open()):
        weights[r['case']][r['operator']] = float(r['estimate'])
    for r in csv.DictReader((folder/'edges.csv').open()):
        edges[r['case']].append((r['from'],r['to'],r['required']=='true'))
    assert len(costs) == len(weights) == len(edges) == 32
    # Both rejection paths: a materialized edge inside one region and a cycle between regions.
    assert cost_of('abc',dict.fromkeys('abc',1),[('a','b',False),('b','c',False),('a','c',True)],{2}) is None
    assert cost_of('abc',dict.fromkeys('abc',1),[('a','b',False),('b','c',False),('a','c',True)],{0,1,2}) == 3
    crossing = [('a','b',False),('c','d',False),('a','d',True),('c','b',True)]
    assert cost_of('abcd',dict.fromkeys('abcd',1),crossing,{2,3}) is None
    results = []
    for case in sorted(weights):
        links = edges[case]
        required = {i for i,e in enumerate(links) if e[2]}
        optional = [i for i in range(len(links)) if i not in required]
        best = float('inf')
        valid = 0
        for mask in range(1 << len(optional)):
            selected = required | {index for bit,index in enumerate(optional) if mask & (1 << bit)}
            cost = cost_of(weights[case], weights[case], links, selected)
            if cost is not None:
                valid += 1
                best = min(best,cost)
        assert costs[case] == {best}, (case,best,costs[case])
        results.append(dict(case=case,assignments=1 << len(optional),schedulable=valid,minimum_cost=best))
    (root/'oracle.json').write_text(json.dumps(results,indent=2)+'\n',encoding='utf-8',newline='\n')
    print('Independent oracle agrees for all 32 graph/cost combinations:',sum(r['assignments'] for r in results),'assignments checked.')


if __name__ == '__main__':
    verify(Path(sys.argv[1]))
