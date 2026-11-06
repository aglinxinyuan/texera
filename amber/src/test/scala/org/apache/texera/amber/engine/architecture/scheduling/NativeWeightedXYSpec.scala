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

class NativeWeightedXYSpec extends AnyFlatSpec {
  import XYPlanningComparison._
  for (
    (inside, outside) <- NativeBranchXYFixtures.sizes;
    profile <- Vector("aggregation-heavy", "join-heavy")
  ) {
    "Native weighted X/Y" should s"preserve the $inside/$outside graph and minimize $profile costs" in {
      val base = NativeBranchXYFixtures.compile(inside, outside)
      val weighted = NativeWeightedXYFixtures.withProfile(base, profile)
      assert(weighted.original eq base.original)
      assert(weighted.input.plan eq base.input.plan)
      assert(weighted.definition == base.definition)
      assert(weighted.input.block == base.input.block)
      val estimates = weighted.input.weights
      assert(estimates.keySet == base.input.weights.keySet)
      assert(estimates.values.count(_ == 8.0) == 2 * (inside + outside))
      for ((id, estimate) <- estimates) {
        val heavy = id.logicalOpId.id.contains(
          if (profile == "aggregation-heavy") "-aggregate-" else "-join-"
        )
        assert(estimate == (if (heavy) 8.0 else 1.0))
      }
      val x = searchX(weighted.input, pruning = true)
      val y = searchY(weighted.input, pruning = true)
      assert(x.cost == y.cost)
      if (inside + outside == 2) assert(x.cost == searchX(weighted.input, pruning = false).cost)
      for (r <- Seq(x, y)) {
        assert(evaluate(weighted.input, r.materialized).contains(r.cost))
        assert(r.regions.flatten == weighted.input.plan.operators.map(_.id))
        assert(r.regions.toSeq.map(_.size).sum == weighted.input.plan.operators.size)
      }
    }
  }
  it should "reject an unknown profile instead of silently using uniform costs" in {
    val base = NativeBranchXYFixtures.compile(1, 1)
    val error =
      intercept[IllegalArgumentException](NativeWeightedXYFixtures.withProfile(base, "typo"))
    assert(error.getMessage.contains("unknown cost profile"))
  }
}
