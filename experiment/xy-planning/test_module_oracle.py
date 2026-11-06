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

"""Cross-check the independent module oracle against full enumeration on small DAGs."""
import importlib.util
from pathlib import Path
import random
import unittest

spec = importlib.util.spec_from_file_location('module_oracle', Path(__file__).with_name('module-oracle.py'))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class ModuleOracleTest(unittest.TestCase):
    def test_weighted_branching_and_bypass(self):
        rng = random.Random(9127)
        # Outer source branch bypasses the loop; inner LS branches before merging at LE.
        edges = [('s','LS',True), ('LS','a',False), ('LS','b',False),
                 ('a','LE',False), ('b','LE',False), ('LE','t',True),
                 ('s','c',False), ('c','t',False)]
        for _ in range(32):
            weights = {key:rng.randint(1,8) for key in ['s','LS','a','b','LE','t','c']}
            varied = [(a,b,required or rng.random()<.25) for a,b,required in edges]
            result = module.minimum(weights,varied,{'LS','a','b','LE'})
            whole = module.enumerate_minimum(weights,varied)
            self.assertEqual(result['minimum_cost'],whole['minimum_cost'])
            self.assertEqual(module.oracle.cost_of(weights,weights,varied,set(result['cuts'])),whole['minimum_cost'])

    def test_reject_invalid_boundaries_and_path(self):
        weights = dict.fromkeys(['s','LS','LE','t'],1)
        edges = [('s','LS',True),('LS','LE',False),('LE','t',True)]
        for bad in [edges+ [('s','LE',True)], [(a,b,False) for a,b,_ in edges],
                    [('s','LS',True),('LE','LS',False),('LE','t',True)]]:
            with self.assertRaises(AssertionError):
                module.minimum(weights,bad,{'LS','LE'})
        with self.assertRaises(AssertionError):
            module.minimum(weights,edges,{'LS','LE','missing'})

    def test_reject_cycle(self):
        weights=dict.fromkeys(['s','LS','LE','t'],1)
        with self.assertRaises(AssertionError):
            module.minimum(weights,[('s','LS',True),('LS','LE',False),('LE','t',True),('t','s',False)],{'LS','LE'})


if __name__ == '__main__':
    unittest.main()
