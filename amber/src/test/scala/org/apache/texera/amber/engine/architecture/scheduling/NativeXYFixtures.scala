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

import org.apache.texera.amber.core.workflow._
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
import org.apache.texera.amber.util.JSONUtils
import org.apache.texera.common.compiler.{CompilationErrorHandling, WorkflowCompiler}
import org.apache.texera.common.compiler.model.{LogicalLink, LogicalPlanPojo}

/** Built-in descriptors and their real physical expansions; no hand-labeled internal cuts. */
object NativeXYFixtures {
  val names: Vector[String] = Vector("aggregate-sort", "summary-join", "two-summaries-join")

  def compile(name: String, aggregateAttribute: String = "value"): CompiledXYFixtures.Compiled = {
    require(names.contains(name), "unknown native case")
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
    def filter(id: String, attribute: String): SpecializedFilterOpDesc = {
      val op = named(new SpecializedFilterOpDesc, id)
      op.predicates = List(
        new FilterPredicate(attribute, ComparisonType.GREATER_THAN_OR_EQUAL_TO, "0")
      )
      op
    }
    def aggregate(id: String, fn: AggregationFunction, column: String): AggregateOpDesc = {
      val op = named(new AggregateOpDesc, id)
      val operation = new AggregationOperation
      operation.aggFunction = fn
      operation.attribute = aggregateAttribute
      operation.resultAttribute = column
      op.aggregations = List(operation)
      op.groupByKeys = List("file")
      op
    }
    val totals = aggregate("totals", AggregationFunction.SUM, "total")
    val sort = named(new SortOpDesc, "sort")
    val criteria = new SortCriteriaUnit
    criteria.attributeName = "total"
    criteria.sortPreference = SortPreference.DESC
    sort.attributes = List(criteria)
    val output = filter("result", "total")
    def link(from: LogicalOp, to: LogicalOp, port: Int = 0): LogicalLink =
      LogicalLink(
        from.operatorIdentifier,
        PortIdentity(),
        to.operatorIdentifier,
        PortIdentity(port)
      )
    val boundaries = List(link(source, start), link(end, output))
    val (bodyOps, bodyLinks): (List[LogicalOp], List[LogicalLink]) = name match {
      case "aggregate-sort" =>
        val positive = filter("positive", "value")
        (
          List(positive, totals, sort),
          List(link(start, positive), link(positive, totals), link(totals, sort), link(sort, end))
        )
      case _ =>
        val join = named(new HashJoinOpDesc[String], "join")
        join.buildAttributeName = "file"
        join.probeAttributeName = "file"
        if (name == "summary-join")
          (
            List(totals, join),
            List(link(start, totals), link(totals, join), link(start, join, 1), link(join, end))
          )
        else {
          val counts = aggregate("counts", AggregationFunction.COUNT, "count")
          (
            List(totals, counts, join, sort),
            List(
              link(start, totals),
              link(start, counts),
              link(totals, join),
              link(counts, join, 1),
              link(join, sort),
              link(sort, end)
            )
          )
        }
    }
    val logical = LogicalPlanPojo(
      List(source, start, end, output) ++ bodyOps,
      boundaries ++ bodyLinks,
      List.empty,
      List.empty
    )
    val definition =
      JSONUtils.objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(logical)
    val result =
      new WorkflowCompiler(new WorkflowContext()).compile(logical, CompilationErrorHandling.Strict)
    require(result.operatorIdToError.isEmpty, result.operatorIdToError.toString)
    val original = result.physicalPlan.getOrElse(
      throw new IllegalStateException("strict compile returned no plan")
    )
    require(
      original.operators.forall(op =>
        op.inputPorts.values.forall(_._3.isRight) && op.outputPorts.values.forall(_._3.isRight)
      ),
      "unresolved schema"
    )
    val startId = original.operators.find(_.id.logicalOpId == start.operatorIdentifier).get.id
    val endId = original.operators.find(_.id.logicalOpId == end.operatorIdentifier).get.id
    var block = Set(startId)
    var next =
      original.links.filter(l => block(l.fromOpId) && l.fromOpId != endId).map(_.toOpId) ++ block
    while (next != block) {
      block = next
      next =
        original.links.filter(l => block(l.fromOpId) && l.fromOpId != endId).map(_.toOpId) ++ block
    }
    require(block(endId), "no path through loop")
    val cuts = original.links.filter(l => block(l.fromOpId) != block(l.toOpId))
    val plan = original.copy(operators = original.operators.map { op =>
      op.copy(outputPorts = op.outputPorts.map {
        case (port, (definition, outgoing, schema)) =>
          port -> (
            (
              definition.copy(blocking = definition.blocking || outgoing.exists(cuts)),
              outgoing,
              schema
            )
          )
      })
    })
    CompiledXYFixtures.Compiled(
      XYPlanningComparison
        .Input(plan, block, startId, endId, plan.operators.map(_.id -> 1.0).toMap),
      original,
      System.nanoTime() - begin,
      definition
    )
  }
}

object NativeXYBenchmark {
  def main(args: Array[String]): Unit =
    CompiledXYBenchmark.run(
      args,
      NativeXYFixtures.names.map(name => (name, name, () => NativeXYFixtures.compile(name)))
    )
}
