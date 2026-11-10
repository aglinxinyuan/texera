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

class XYPlanningSweepSpec extends AnyFlatSpec {
  import XYPlanningComparison._
  import XYPlanningSweep._

  for (
    shape <- Seq("chain", "shortcut", "diamond", "fanout"); profile <- Seq("uniform", "skewed")
  ) {
    "The varied fixtures" should s"preserve minimum cost with both pruning modes for $shape/$profile" in {
      val size = if (Set("diamond", "fanout")(shape)) 1 else 3
      val spec = Scenario(s"test-$shape", shape, size, 2, profile)
      val input = fixtureFor(spec)
      val reference = searchX(input, pruning = false)
      for (result <- Seq(searchY(input, false), searchX(input, true), searchY(input, true))) {
        assert(result.cost == reference.cost)
        assert(evaluate(input, result.materialized).contains(result.cost))
        assert(result.regions.flatten == input.block ++ input.outside)
        assert(result.regions.toSeq.map(_.size).sum == input.plan.operators.size)
      }
      val (inside, outside) = selectable(input)
      assert(reference.states == (1 << (inside + outside)))
      assert(searchY(input, false).states == (1 << inside) + (1 << outside))
    }
  }

  it should "include larger graphs, balanced and unbalanced modules, and stable weights" in {
    assert(scenarios.map(_.name).distinct.size == scenarios.size)
    assert(scenarios.size == 32)
    val inputs = scenarios.map(fixtureFor)
    assert(inputs.map(i => selectable(i).productIterator.map(_.asInstanceOf[Int]).sum).max == 13)
    assert(inputs.exists(i => { val (a, b) = selectable(i); a >= 3 * b }))
    assert(inputs.exists(i => { val (a, b) = selectable(i); b >= 3 * a }))
    assert(inputs.forall(i => i.weights.values.forall(_ > 0)))
    scenarios.foreach { s =>
      assert(fixtureFor(s).weights == fixtureFor(s).weights)
      assert(evaluate(fixtureFor(s), fixtureFor(s).plan.links).nonEmpty)
    }
  }

  it should "keep topology identical when only the cost profile changes" in {
    val a = fixtureFor(Scenario("same", "diamond", 2, 3, "uniform"))
    val b = fixtureFor(Scenario("same", "diamond", 2, 3, "skewed"))
    assert(a.plan.links == b.plan.links)
    assert(a.plan.getBlockingAndDependeeLinks == b.plan.getBlockingAndDependeeLinks)
    assert(a.weights != b.weights)
  }

  it should "reject invalid shapes, profiles and sizes" in {
    for (
      s <- Seq(
        Scenario("bad", "unknown", 2, 2, "uniform"),
        Scenario("bad", "chain", 2, 2, "unknown"),
        Scenario("bad", "chain", 0, 2, "uniform"),
        Scenario("bad", "shortcut", 2, 2, "uniform"),
        Scenario("bad", "diamond", 2, 0, "uniform")
      )
    ) {
      intercept[IllegalArgumentException](fixtureFor(s))
    }
  }
}
