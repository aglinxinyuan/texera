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

import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.aggregate.{
  AggregateOpDesc,
  AggregationFunction,
  AggregationOperation
}
import org.apache.texera.amber.operator.filter.{
  ComparisonType,
  FilterPredicate,
  SpecializedFilterOpDesc
}
import org.apache.texera.amber.operator.hashJoin.HashJoinOpDesc
import org.apache.texera.amber.operator.loop.{LoopEndOpDesc, LoopStartOpDesc}
import org.apache.texera.amber.operator.sort.{SortCriteriaUnit, SortOpDesc, SortPreference}
import org.apache.texera.amber.operator.source.scan.csv.CSVScanSourceOpDesc
import org.apache.texera.amber.operator.union.UnionOpDesc
import org.apache.texera.common.compiler.model.{LogicalLink, LogicalPlanPojo}
import org.scalatest.flatspec.AnyFlatSpec

/** A small correctness case, deliberately selected for a strict cost-ranking reversal. */
object NativeCostChoiceXYSpec {
  def compile(aggregateAttribute: String = "value"): CompiledXYFixtures.Compiled = {
    val begin = System.nanoTime()
    def named[T <: LogicalOp](op: T, id: String): T = { op.setOperatorId(id); op }
    val source = named(new CSVScanSourceOpDesc, "csv")
    source.fileName = Some(
      CompiledXYFixtures.folder.getParent.resolve("native-input.csv").toUri.toString
    )
    val start = named(new LoopStartOpDesc, "LS")
    start.initialization = "i = 0"
    start.output = "table"
    val end = named(new LoopEndOpDesc, "LE")
    end.update = "i += 1"
    end.condition = "i < 3"
    val filter = named(new SpecializedFilterOpDesc, "filter")
    filter.predicates = List(
      new FilterPredicate("value", ComparisonType.GREATER_THAN_OR_EQUAL_TO, "0")
    )
    val aggregate = named(new AggregateOpDesc, "in-aggregate-total")
    val operation = new AggregationOperation
    operation.aggFunction = AggregationFunction.SUM
    operation.attribute = aggregateAttribute
    operation.resultAttribute = "value"
    aggregate.aggregations = List(operation)
    aggregate.groupByKeys = List("file")
    def join(id: String): HashJoinOpDesc[String] = {
      val op = named(new HashJoinOpDesc[String], id)
      op.buildAttributeName = "file"
      op.probeAttributeName = "file"
      op
    }
    val rawJoin = join("in-join-raw")
    val summaryJoin = join("in-join-summary")
    def sort(id: String): SortOpDesc = {
      val op = named(new SortOpDesc, id)
      val criteria = new SortCriteriaUnit
      criteria.attributeName = "file"
      criteria.sortPreference = SortPreference.ASC
      op.attributes = List(criteria)
      op
    }
    val sortedRaw = sort("sort-raw")
    val output = sort("output")
    val union = named(new UnionOpDesc, "union")
    def link(from: LogicalOp, to: LogicalOp, port: Int = 0): LogicalLink =
      LogicalLink(
        from.operatorIdentifier,
        PortIdentity(),
        to.operatorIdentifier,
        PortIdentity(port)
      )
    val ops =
      List(source, start, filter, aggregate, rawJoin, summaryJoin, sortedRaw, union, end, output)
    val links = List(
      link(source, start),
      link(start, filter),
      link(start, aggregate),
      link(filter, rawJoin),
      link(start, rawJoin, 1),
      link(rawJoin, sortedRaw),
      link(aggregate, summaryJoin),
      link(filter, summaryJoin, 1),
      link(sortedRaw, union),
      link(summaryJoin, union),
      link(union, end),
      link(end, output)
    )
    NativeXYFixtures.compileLogical(
      LogicalPlanPojo(ops, links, List.empty, List.empty),
      start,
      end,
      begin
    )
  }

  // Reuses the same graph under both profiles. Timings are diagnostic, not a benchmark claim.
  def main(args: Array[String]): Unit = {
    val base = compile()
    CompiledXYBenchmark.run(
      args,
      NativeWeightedXYFixtures.profiles.map { profile =>
        (profile, "cost-choice", () => NativeWeightedXYFixtures.withProfile(base, profile))
      }
    )
  }
}

class NativeCostChoiceXYSpec extends AnyFlatSpec {
  import XYPlanningComparison._

  "Native cost choice" should "retain compiler ports and reverse the uniquely cheapest plan" in {
    val base = NativeCostChoiceXYSpec.compile()
    assert(base.input.plan.operators.size == 13)
    assert(base.input.plan.links.size == 15)
    val boundary =
      base.input.plan.links.filter(l => base.input.block(l.fromOpId) != base.input.block(l.toOpId))
    assert(boundary.size == 2)
    val required = base.original.getBlockingAndDependeeLinks ++ boundary
    assert(base.original.getBlockingAndDependeeLinks.size == 4)
    assert(base.input.plan.getBlockingAndDependeeLinks == required)
    assert(base.input.block.size == 11)
    val inputs = NativeWeightedXYFixtures.profiles.map(p =>
      NativeWeightedXYFixtures.withProfile(base, p).input
    )
    val results = inputs.map(input => {
      val x = searchX(input, pruning = true)
      val y = searchY(input, pruning = true)
      assert(x.cost == y.cost)
      assert(x.materialized == y.materialized)
      assert(searchX(input, pruning = false).cost == x.cost)
      assert(searchY(input, pruning = false).cost == x.cost)
      assert(evaluate(input, x.materialized).contains(x.cost))
      for (result <- Seq(x, y)) {
        assert(result.regions.flatten == input.plan.operators.map(_.id))
        assert(result.regions.toSeq.map(_.size).sum == input.plan.operators.size)
      }
      x
    })
    assert(results.map(_.cost) == Vector(13.0, 28.0))
    assert(results(0).materialized != results(1).materialized)
    assert(evaluate(inputs(1), results(0).materialized).contains(34.0))
    assert(evaluate(inputs(0), results(1).materialized).contains(14.0))

    // These are deliberately different valid partitions, not a tie or just larger numeric costs.
    def optionalCuts(result: Result): Set[(String, String)] =
      (result.materialized -- required).map(l =>
        (CompiledXYBenchmark.operatorKey(l.fromOpId), CompiledXYBenchmark.operatorKey(l.toOpId))
      )
    val ls = CompiledXYBenchmark.operatorKey(inputs.head.start)
    val filter = base.input.plan.operators.find(_.id.logicalOpId.id == "filter").get.id
    val union = base.input.plan.operators.find(_.id.logicalOpId.id == "union").get.id
    val fk = CompiledXYBenchmark.operatorKey(filter)
    val uk = CompiledXYBenchmark.operatorKey(union)
    assert(
      optionalCuts(results(0)) == Set((ls, "in-join-raw/probe"), (fk, "in-join-summary/probe"))
    )
    assert(
      optionalCuts(results(1)) == Set(
        (ls, "in-join-raw/probe"),
        (ls, fk),
        ("in-join-summary/probe", uk)
      )
    )
    assert(evaluate(inputs.head, required).isEmpty)
  }

  it should "reject a missing aggregate column during strict compilation" in {
    intercept[Exception](NativeCostChoiceXYSpec.compile("missing_column"))
  }
}
