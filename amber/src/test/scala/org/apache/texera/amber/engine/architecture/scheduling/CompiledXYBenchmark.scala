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

import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters._
import scala.util.Random

/** Pruning-on timings after strict compilation; compilation and checks are outside the timers. */
object CompiledXYBenchmark {
  import XYPlanningComparison._
  private def gc: (Long, Long) = {
    val beans = ManagementFactory.getGarbageCollectorMXBeans.asScala
    (
      beans.map(b => math.max(0L, b.getCollectionCount)).sum,
      beans.map(b => math.max(0L, b.getCollectionTime)).sum
    )
  }
  def main(args: Array[String]): Unit = {
    require(args.length == 4, "usage: output-directory repetitions warmups fork")
    val output = Paths.get(args(0))
    val repetitions = args(1).toInt
    val warmups = args(2).toInt
    val fork = args(3).toInt
    require(repetitions > 0 && warmups >= 0 && fork >= 0)
    Files.createDirectories(output)
    println(
      s"Planner classes: ${classOf[CostBasedScheduleGenerator].getProtectionDomain.getCodeSource.getLocation}"
    )
    println(s"Java runtime: ${System.getProperty("java.runtime.version")}")
    val results = Files.newBufferedWriter(output.resolve("results.csv"), StandardCharsets.UTF_8)
    val operators = Files.newBufferedWriter(output.resolve("operators.csv"), StandardCharsets.UTF_8)
    val edges = Files.newBufferedWriter(output.resolve("edges.csv"), StandardCharsets.UTF_8)
    val compilation =
      Files.newBufferedWriter(output.resolve("compilation.csv"), StandardCharsets.UTF_8)
    def row(writer: java.io.BufferedWriter, values: Any*): Unit = {
      writer.write(
        values.map(v => "\"" + v.toString.replace("\"", "\"\"") + "\"").mkString(",") + "\n"
      )
      writer.flush()
    }
    try {
      row(
        results,
        "case",
        "fork",
        "repetition",
        "order",
        "strategy",
        "total_ns",
        "decomposition_ns",
        "search_ns",
        "composition_ns",
        "search_states",
        "cost",
        "regions",
        "cuts",
        "gc_count",
        "gc_ms"
      )
      row(
        operators,
        "case",
        "operator",
        "inside",
        "estimate",
        "executor",
        "requires_materialized",
        "is_loop_start",
        "input_schema",
        "output_schema"
      )
      row(edges, "case", "from", "to", "output_port", "required", "compiler_required")
      row(
        compilation,
        "case",
        "source",
        "compile_ns",
        "operators",
        "links",
        "exhaustive_cost",
        "exhaustive_states"
      )
      for ((name, file) <- new Random(3001 + fork).shuffle(CompiledXYFixtures.files)) {
        val compiled = CompiledXYFixtures.compile(CompiledXYFixtures.folder.resolve(file))
        val input = compiled.input
        val orderedEdges = input.plan.links.toVector.sortBy(_.toString)
        val reference = searchX(input, pruning = false)
        row(
          compilation,
          name,
          file,
          compiled.compileNs,
          input.plan.operators.size,
          orderedEdges.size,
          reference.cost,
          reference.states
        )
        input.plan.operators.toVector.sortBy(_.id.logicalOpId.id).foreach { op =>
          row(
            operators,
            name,
            op.id.logicalOpId.id,
            input.block(op.id),
            input.weights(op.id),
            op.opExecInitInfo.getClass.getSimpleName,
            op.requiresMaterializedExecution,
            op.isLoopStart,
            op.inputPorts.toVector
              .sortBy(_._1.id)
              .map(p => s"${p._1.id}:${p._2._3.toOption.get}")
              .mkString(";"),
            op.outputPorts.toVector
              .sortBy(_._1.id)
              .map(p => s"${p._1.id}:${p._2._3.toOption.get}")
              .mkString(";")
          )
        }
        orderedEdges.foreach { l =>
          row(
            edges,
            name,
            l.fromOpId.logicalOpId.id,
            l.toOpId.logicalOpId.id,
            l.fromPortId.id,
            input.plan.getBlockingAndDependeeLinks(l),
            compiled.original.getBlockingAndDependeeLinks(l)
          )
        }
        for (rep <- -warmups until repetitions) {
          val order = if (((rep + fork) & 1) == 0) Vector("X", "Y") else Vector("Y", "X")
          val pair = order.map { strategy =>
            val before = gc
            val result =
              if (strategy == "X") searchX(input, pruning = true)
              else searchY(input, pruning = true)
            val after = gc
            (strategy, result, after._1 - before._1, after._2 - before._2)
          }
          // Validate after both timed searches so validation does not favor the second search.
          pair.foreach {
            case (strategy, result, gcCount, gcMs) =>
              require(result.cost == reference.cost, s"nonoptimal $name/$strategy")
              require(
                evaluate(input, result.materialized).contains(result.cost),
                "invalid selected cuts"
              )
              require(result.regions.flatten == input.plan.operators.map(_.id), "missing operator")
              require(
                result.regions.toSeq.map(_.size).sum == input.plan.operators.size,
                "duplicate operator"
              )
              if (rep >= 0)
                row(
                  results,
                  name,
                  fork,
                  rep,
                  order.mkString,
                  strategy,
                  result.totalNs,
                  result.decompositionNs,
                  result.searchNs,
                  result.compositionNs,
                  result.states,
                  result.cost,
                  result.regions.size,
                  orderedEdges.indices
                    .filter(i => result.materialized(orderedEdges(i)))
                    .mkString(";"),
                  gcCount,
                  gcMs
                )
          }
        }
        println(
          s"Compiled XY fork=$fork case=$name equal cost=${reference.cost}, $repetitions measured pairs"
        )
      }
    } finally {
      results.close(); operators.close(); edges.close(); compilation.close()
    }
  }
}
