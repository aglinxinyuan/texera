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

class NativeXYPlanningSpec extends AnyFlatSpec {
  import XYPlanningComparison._
  for (name <- Seq("aggregate-sort", "summary-join", "two-summaries-join")) {
    "Native X/Y planning" should s"preserve actual blocking ports and equal optimal costs for $name" in {
      val compiled = NativeXYFixtures.compile(name)
      val input = compiled.input
      val original = compiled.original
      assert(original.getBlockingAndDependeeLinks.nonEmpty)
      assert(
        input.plan.operators
          .map(op => CompiledXYBenchmark.operatorKey(op.id))
          .size == input.plan.operators.size
      )
      assert(original.operators.exists(_.id.layerName == "globalAgg"))
      assert(original.operators.exists(_.id.layerName == "localAgg"))
      val boundaries =
        input.plan.links.filter(l => input.block(l.fromOpId) != input.block(l.toOpId))
      assert(boundaries.size == 2)
      assert(
        input.plan.getBlockingAndDependeeLinks == original.getBlockingAndDependeeLinks ++ boundaries
      )
      for (
        op <- input.plan.operators; (port, (definition, links, schema)) <- op.outputPorts
        if !links.exists(boundaries)
      ) {
        assert((definition, links, schema) == original.getOperator(op.id).outputPorts(port))
      }
      assert(
        original.operators
          .filter(_.id.layerName == "localAgg")
          .forall(_.outputPorts.values.forall(!_._1.blocking))
      )
      assert(
        original.operators
          .filter(_.id.layerName == "globalAgg")
          .forall(_.outputPorts.values.forall(_._1.blocking))
      )
      if (name.contains("join")) {
        assert(
          original.operators.exists(op =>
            op.id.layerName == "build" && op.outputPorts.values.exists(_._1.blocking)
          )
        )
        assert(
          original.operators.exists(op =>
            op.id.layerName == "probe" && op.inputPorts.values.exists(_._1.dependencies.nonEmpty)
          )
        )
      }
      val parts = decompose(input)
      for (part <- Seq(parts.inner, parts.outer); op <- part.operators if op.id != parts.macroId) {
        val before = input.plan.getOperator(op.id)
        assert(op.copy(inputPorts = before.inputPorts, outputPorts = before.outputPorts) == before)
        assert(
          op.inputPorts.view.mapValues(p => (p._1, p._3)).toMap == before.inputPorts.view
            .mapValues(p => (p._1, p._3))
            .toMap
        )
        assert(
          op.outputPorts.view.mapValues(p => (p._1, p._3)).toMap == before.outputPorts.view
            .mapValues(p => (p._1, p._3))
            .toMap
        )
      }
      val reference = searchX(input, pruning = false)
      val x = searchX(input, pruning = true)
      val y = searchY(input, pruning = true)
      assert(x.cost == reference.cost && y.cost == reference.cost)
      for (result <- Seq(x, y)) {
        assert(evaluate(input, result.materialized).contains(result.cost))
        assert(result.regions.flatten == input.plan.operators.map(_.id))
        assert(result.regions.toSeq.map(_.size).sum == input.plan.operators.size)
      }
      val required = input.plan.getBlockingAndDependeeLinks
      val missing = original.getBlockingAndDependeeLinks.head
      intercept[IllegalArgumentException](evaluate(input, required - missing))
    }
  }
  it should "reject invalid native aggregation input instead of benchmarking a partial plan" in {
    intercept[Exception](NativeXYFixtures.compile("aggregate-sort", "missing_column"))
    intercept[IllegalArgumentException](NativeXYFixtures.compile("unsupported-case"))
  }
}
