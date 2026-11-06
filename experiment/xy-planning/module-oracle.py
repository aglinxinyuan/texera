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

"""Exact independent enumeration for a sealed loop with compulsory boundary cuts.

The zero-weight macro keeps both boundary arcs, including paths through the loop.
No region crosses a boundary, so the two module objectives add. A path from entry
to exit makes contraction preserve cycles. Full enumeration cross-checks small cases.
"""
import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location('region_oracle', Path(__file__).with_name('oracle.py'))
oracle = importlib.util.module_from_spec(spec)
spec.loader.exec_module(oracle)


def enumerate_minimum(weights, edges):
    required = {i for i,e in enumerate(edges) if e[2]}
    optional = [i for i in range(len(edges)) if i not in required]
    best, best_cuts, valid = float('inf'), None, 0
    for mask in range(1 << len(optional)):
        cuts = required | {index for bit,index in enumerate(optional) if mask & (1 << bit)}
        cost = oracle.cost_of(weights, weights, edges, cuts)
        if cost is not None:
            valid += 1
            if cost < best:
                best, best_cuts = cost, sorted(cuts)
    assert best_cuts is not None, 'no schedulable plan'
    return dict(minimum_cost=best, cuts=best_cuts, assignments=1 << len(optional), valid_assignments=valid)


def minimum(weights, edges, inside, full_limit=14):
    assert inside and inside < set(weights), 'invalid block membership'
    assert all(a in weights and b in weights for a,b,_ in edges)
    assert all(w >= 0 for w in weights.values())
    assert oracle.cost_of(weights, weights, edges, set(range(len(edges)))) is not None, 'cyclic original graph'
    incoming = [(a,b,r) for a,b,r in edges if a not in inside and b in inside]
    outgoing = [(a,b,r) for a,b,r in edges if a in inside and b not in inside]
    assert len(incoming) == len(outgoing) == 1, 'not a sealed single-entry/single-exit loop'
    assert incoming[0][2] and outgoing[0][2], 'boundary cuts must be required'
    start, end = incoming[0][1], outgoing[0][0]
    inner_indices = [i for i,(a,b,_) in enumerate(edges) if a in inside and b in inside]
    def reachable(seed, reverse=False):
        seen = {seed}
        while True:
            before = set(seen)
            for i in inner_indices:
                a,b,_ = edges[i]
                if reverse:
                    a,b = b,a
                if a in seen:
                    seen.add(b)
            if seen == before:
                return seen
    assert reachable(start) == inside and reachable(end, True) == inside, 'body is not on entry-to-exit paths'
    macro = '<independent-loop-macro>'
    assert macro not in weights
    outer_indices = [i for i in range(len(edges)) if i not in set(inner_indices)]
    outer_edges = [(macro if a in inside else a, macro if b in inside else b, r)
                   for i in outer_indices for a,b,r in [edges[i]]]
    inner = enumerate_minimum({i:weights[i] for i in inside}, [edges[i] for i in inner_indices])
    outer = enumerate_minimum({**{i:w for i,w in weights.items() if i not in inside}, macro:0}, outer_edges)
    cuts = sorted([inner_indices[i] for i in inner['cuts']] + [outer_indices[i] for i in outer['cuts']])
    best = inner['minimum_cost'] + outer['minimum_cost']
    assert oracle.cost_of(weights, weights, edges, set(cuts)) == best, 'composed minimum is invalid'
    optional_count = sum(not r for _,_,r in edges)
    whole = enumerate_minimum(weights,edges) if optional_count <= full_limit else None
    if whole:
        assert whole['minimum_cost'] == best, 'module and whole minima differ'
    return dict(minimum_cost=best, cuts=cuts, method='sealed-module exhaustive enumeration',
                inner=inner, outer=outer, whole=whole, global_optional_edges=optional_count,
                assignments=inner['assignments']+outer['assignments']+(whole['assignments'] if whole else 0))
