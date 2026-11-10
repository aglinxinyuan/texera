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
import org.apache.texera.amber.operator.hashJoin.HashJoinOpDesc
import org.apache.texera.amber.operator.loop.{LoopEndOpDesc, LoopStartOpDesc}
import org.apache.texera.amber.operator.sort.{SortCriteriaUnit, SortOpDesc, SortPreference}
import org.apache.texera.amber.operator.source.scan.csv.CSVScanSourceOpDesc
import org.apache.texera.common.compiler.model.{LogicalLink, LogicalPlanPojo}
import scala.collection.mutable.ArrayBuffer

/** Each branch computes a per-file statistic and joins it back to the original rows. */
object NativeBranchXYFixtures {
  val sizes: Vector[(Int, Int)] = Vector((1, 1), (2, 2), (3, 3), (4, 4), (1, 4), (4, 1))
  def name(inside: Int, outside: Int): String = s"inside-$inside-outside-$outside"

  def compile(
      inside: Int,
      outside: Int,
      aggregateAttribute: String = "value"
  ): CompiledXYFixtures.Compiled = {
    require(inside >= 1 && inside <= 4 && outside >= 1 && outside <= 4)
    val begin = System.nanoTime()
    val ops = ArrayBuffer.empty[LogicalOp]
    val links = ArrayBuffer.empty[LogicalLink]
    def named[T <: LogicalOp](op: T, id: String): T = {
      op.setOperatorId(id)
      ops += op
      op
    }
    def link(from: LogicalOp, to: LogicalOp, port: Int = 0): Unit =
      links += LogicalLink(
        from.operatorIdentifier,
        PortIdentity(),
        to.operatorIdentifier,
        PortIdentity(port)
      )
    val source = named(new CSVScanSourceOpDesc, "csv")
    source.fileName = Some(
      CompiledXYFixtures.folder.getParent.resolve("native-input.csv").toUri.toString
    )
    val start = named(new LoopStartOpDesc, "LS")
    start.initialization = "i = 0"
    start.output = "table[table['value'] >= i]"
    val end = named(new LoopEndOpDesc, "LE")
    end.update = "i += 1"
    end.condition = "i < 3"
    val functions = Vector(
      AggregationFunction.SUM,
      AggregationFunction.COUNT,
      AggregationFunction.MIN,
      AggregationFunction.MAX
    )
    def enrich(input: LogicalOp, count: Int, prefix: String): LogicalOp = {
      (0 until count).foldLeft(input) { (previous, i) =>
        val agg = named(new AggregateOpDesc, s"${prefix}-aggregate-${i + 1}")
        val operation = new AggregationOperation
        operation.aggFunction = functions(i)
        operation.attribute = aggregateAttribute
        operation.resultAttribute = s"${prefix}_stat_${i + 1}"
        agg.aggregations = List(operation)
        agg.groupByKeys = List("file")
        val join = named(new HashJoinOpDesc[String], s"${prefix}-join-${i + 1}")
        join.buildAttributeName = "file"
        join.probeAttributeName = "file"
        link(previous, agg)
        link(agg, join)
        link(previous, join, 1)
        join
      }
    }
    link(enrich(source, outside, "out"), start)
    link(enrich(start, inside, "in"), end)
    val sort = named(new SortOpDesc, "sort")
    val criteria = new SortCriteriaUnit
    criteria.attributeName = "file"
    criteria.sortPreference = SortPreference.ASC
    sort.attributes = List(criteria)
    link(end, sort)
    NativeXYFixtures.compileLogical(
      LogicalPlanPojo(ops.toList, links.toList, List.empty, List.empty),
      start,
      end,
      begin
    )
  }
}

object NativeBranchXYBenchmark {
  def main(args: Array[String]): Unit =
    CompiledXYBenchmark.run(
      args,
      NativeBranchXYFixtures.sizes.map {
        case (inside, outside) =>
          val name = NativeBranchXYFixtures.name(inside, outside)
          (name, name, () => NativeBranchXYFixtures.compile(inside, outside))
      },
      exhaustiveLimit = 8
    )
}
