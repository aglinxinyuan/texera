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

"""A sensitivity run must change only the declared estimates, never its graph."""
import importlib.util
from pathlib import Path
import sys
import unittest

spec=importlib.util.spec_from_file_location('analysis',Path(__file__).with_name('analyze-compiled.py'))
analysis=importlib.util.module_from_spec(spec); spec.loader.exec_module(analysis)


class WeightedManifestTest(unittest.TestCase):
    def fixture(self,profile):
        case='inside-1-outside-1'; name=profile+'__'+case
        base=[dict(case=case,operator=op,estimate='1.0',inside='true',executor='original')
              for op in ['LS/main','in-aggregate-1/localAgg','in-aggregate-1/globalAgg','in-join-1/build','in-join-1/probe','LE/main']]
        selected='-aggregate-' if profile=='aggregation-heavy' else '-join-'
        weighted=[{**r,'case':name,'estimate':'8.0' if selected in r['operator'] else '1.0'} for r in base]
        edges=[dict(case=case,**{'from':'LS/main','to':'in-aggregate-1/localAgg','required':'false'})]
        changed=[{**r,'case':name} for r in edges]
        return name,weighted,changed,base,edges

    def test_both_profiles(self):
        for profile in ['aggregation-heavy','join-heavy']:
            analysis.validate_weighted_snapshot(*self.fixture(profile))

    def test_reject_wrong_weights_or_changed_graph(self):
        for field,value in [('estimate','99'),('inside','false'),('executor','replacement'),('operator','wrong')]:
            args=self.fixture('aggregation-heavy')
            args[1][0][field]=value
            with self.assertRaises(AssertionError): analysis.validate_weighted_snapshot(*args)
        args=self.fixture('join-heavy'); args[2][0]['required']='true'
        with self.assertRaises(AssertionError): analysis.validate_weighted_snapshot(*args)

    def test_reject_unknown_profile_or_missing_stage(self):
        args=self.fixture('aggregation-heavy')
        with self.assertRaises(AssertionError): analysis.validate_weighted_snapshot('typo__inside-1-outside-1',*args[1:])
        args[1].pop()
        with self.assertRaises(AssertionError): analysis.validate_weighted_snapshot(*args)


if __name__=='__main__':
    unittest.main(testRunner=unittest.TextTestRunner(stream=sys.stdout))
