/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.amber.engine.architecture.scheduling

import org.scalatest.flatspec.AnyFlatSpec

class XYPlanningComparisonSpec extends AnyFlatSpec {
  import XYPlanningComparison._

  for ((inside, outside) <- Seq((3, 1), (3, 3), (5, 1), (5, 3))) {
    "X and Y" should s"return schedulable equal-cost plans for $inside/$outside" in {
      val input = fixture(inside, outside)
      val x = searchX(input, pruning = false)
      val y = searchY(input, pruning = false)
      assert(x.cost == y.cost)
      assert(x.cost > 0 && x.cost.isFinite)
      assert(x.states == (1 << (inside + outside + 1)))
      assert(y.states == (1 << inside) + (1 << (outside + 1)))
      for (result <- Seq(x, y)) {
        assert(input.plan.getBlockingAndDependeeLinks.subsetOf(result.materialized))
        assert(evaluate(input, result.materialized).contains(result.cost))
        assert(result.regions.flatten == input.block ++ input.outside)
        assert(result.regions.map(_.size).sum == input.plan.operators.size)
      }
    }
  }

  it should "retain the exhaustive minimum with production pruning enabled" in {
    val input = fixture(3, 3)
    val reference = searchX(input, pruning = false)
    assert(searchX(input, pruning = true).cost == reference.cost)
    assert(searchY(input, pruning = true).cost == reference.cost)
  }

  it should "reject an assignment whose region graph has a cycle" in {
    val input = fixture(3, 1)
    assert(evaluate(input, input.plan.getBlockingAndDependeeLinks).isEmpty)
    assert(evaluate(input, input.plan.links).nonEmpty)
  }

  it should "reject unsealed blocks instead of reporting an invalid comparison" in {
    val input = fixture(3, 1)
    intercept[IllegalArgumentException] {
      searchY(input.copy(block = input.block - input.start), pruning = false)
    }
  }

  it should "reject invalid fixture sizes" in {
    intercept[IllegalArgumentException](fixture(2, 1))
    intercept[IllegalArgumentException](fixture(3, 0))
  }
}
