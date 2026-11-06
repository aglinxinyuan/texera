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

import org.apache.texera.amber.core.executor.OpExecInitInfo
import org.apache.texera.amber.core.tuple.Schema
import org.apache.texera.amber.core.virtualidentity._
import org.apache.texera.amber.core.workflow._
import org.apache.texera.amber.engine.architecture.scheduling.config.ResourceConfig
import org.apache.texera.amber.engine.architecture.scheduling.resourcePolicies.ResourceAllocator
import org.apache.texera.amber.engine.common.virtualidentity.util.COORDINATOR
import org.jgrapht.alg.shortestpath.BFSShortestPath

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters._

/** Planning-only pilot: one sealed block, fixed operator estimates, real PASTA search. */
object XYPlanningComparison {
  case class Input(
      plan: PhysicalPlan,
      block: Set[PhysicalOpIdentity],
      start: PhysicalOpIdentity,
      end: PhysicalOpIdentity,
      weights: Map[PhysicalOpIdentity, Double]
  ) {
    def outside: Set[PhysicalOpIdentity] = plan.operators.map(_.id) -- block
  }

  case class Result(
      materialized: Set[PhysicalLink],
      regions: Set[Set[PhysicalOpIdentity]],
      cost: Double,
      states: Int,
      decompositionNs: Long,
      searchNs: Long,
      compositionNs: Long,
      totalNs: Long
  )

  private def id(name: String): PhysicalOpIdentity =
    PhysicalOpIdentity(OperatorIdentity(name), "main")

  private[scheduling] def planOf(
      ids: Set[PhysicalOpIdentity],
      links: Set[PhysicalLink],
      required: Set[PhysicalLink]
  ): PhysicalPlan = {
    val schema = Schema()
    val operators = ids.map { op =>
      val incoming = links.filter(_.toOpId == op).groupBy(_.toPortId)
      val outgoing = links.filter(_.fromOpId == op).groupBy(_.fromPortId)
      PhysicalOp(
        op,
        WorkflowIdentity(0),
        ExecutionIdentity(0),
        OpExecInitInfo.Empty,
        parallelizable = false,
        derivePartition = _ => UnknownPartition(),
        inputPorts = incoming.map {
          case (port, edges) =>
            port -> ((InputPort(id = port), edges.toList, Right(schema)))
        },
        outputPorts = outgoing.map {
          case (port, edges) =>
            port -> (
              (
                OutputPort(id = port, blocking = edges.exists(required)),
                edges.toList,
                Right(schema)
              )
            )
        }
      )
    }
    PhysicalPlan(operators, links)
  }

  /** The body and outer bypass both contain genuine unschedulable edge assignments. */
  def fixture(inside: Int, outside: Int): Input = {
    require(inside >= 3 && outside >= 1, "inside >= 3 and outside >= 1 required")
    val body = Vector(id("LS")) ++ (1 until inside).map(i => id(s"b$i")) ++ Vector(id("LE"))
    val prefix = (0 to outside).map(i => id(s"s$i")).toVector
    val sink = id("sink")
    def edge(a: PhysicalOpIdentity, b: PhysicalOpIdentity, port: Int = 0): PhysicalLink =
      PhysicalLink(a, PortIdentity(port), b, PortIdentity())
    def chain(nodes: Vector[PhysicalOpIdentity]): Set[PhysicalLink] =
      nodes.sliding(2).map(pair => edge(pair(0), pair(1))).toSet
    val enter = edge(prefix.last, body.head)
    val leave = edge(body.last, sink)
    val blockingShortcut = edge(body(1), body.last, 1)
    val links =
      chain(body) ++ chain(prefix) ++ Set(enter, leave, blockingShortcut, edge(prefix.head, sink))
    val ids = body.toSet ++ prefix + sink
    val weights = ids.toVector
      .sortBy(_.logicalOpId.id)
      .zipWithIndex
      .map {
        case (op, n) =>
          op -> (1.0 + n % 5)
      }
      .toMap
    Input(
      planOf(ids, links, Set(enter, leave, blockingShortcut)),
      body.toSet,
      body.head,
      body.last,
      weights
    )
  }

  private class FixedCost(allocator: ResourceAllocator, weights: Map[PhysicalOpIdentity, Double])
      extends CostEstimator {
    override def allocateResourcesAndEstimateCost(
        region: Region,
        units: Int
    ): (ResourceConfig, Double) = {
      val (config, _) = allocator.allocate(region)
      (config, region.getOperators.map(op => weights(op.id)).max)
    }
  }

  private def generator(
      plan: PhysicalPlan,
      weights: Map[PhysicalOpIdentity, Double]
  ): CostBasedScheduleGenerator =
    new CostBasedScheduleGenerator(
      new WorkflowContext(),
      plan,
      COORDINATOR,
      Some(allocator => new FixedCost(allocator, weights))
    )

  private def validate(input: Input): Unit = {
    require(
      input.block.contains(input.start) && input.block.contains(input.end),
      "missing block endpoint"
    )
    require(
      input.start != input.end && input.block.subsetOf(input.plan.operators.map(_.id)),
      "invalid block members"
    )
    require(
      input.weights.keySet == input.plan.operators.map(_.id),
      "one estimate per operator required"
    )
    require(
      input.weights.values.forall(w => w.isFinite && w >= 0),
      "estimates must be finite and nonnegative"
    )
    val enter = input.plan.links.filter(l => !input.block(l.fromOpId) && input.block(l.toOpId))
    val leave = input.plan.links.filter(l => input.block(l.fromOpId) && !input.block(l.toOpId))
    require(
      enter.size == 1 && enter.head.toOpId == input.start,
      "block must have one entry at LoopStart"
    )
    require(
      leave.size == 1 && leave.head.fromOpId == input.end,
      "block must have one exit at LoopEnd"
    )
    require(
      (enter ++ leave).subsetOf(input.plan.getBlockingAndDependeeLinks),
      "boundary edges must be materialized"
    )
    val sub = input.plan.getSubPlan(input.block)
    require(
      new BFSShortestPath(sub.dag).getPath(input.start, input.end) != null,
      "missing LoopStart-to-LoopEnd path"
    )
  }

  def evaluate(input: Input, materialized: Set[PhysicalLink]): Option[Double] =
    generator(input.plan, input.weights).evaluateMaterialization(materialized).map(_.cost)

  def searchX(input: Input, pruning: Boolean): Result = {
    val begin = System.nanoTime()
    validate(input)
    val planner = generator(input.plan, input.weights)
    val result = planner.bottomUpSearch(
      globalSearch = true,
      oChains = pruning,
      oCleanEdges = pruning,
      oEarlyStop = pruning
    )
    val end = System.nanoTime()
    Result(
      result.state ++ input.plan.getBlockingAndDependeeLinks,
      result.regionDAG.vertexSet().asScala.map(_.getOperators.map(_.id)).toSet,
      result.cost,
      result.numStatesExplored,
      0,
      end - begin,
      0,
      end - begin
    )
  }

  def searchY(input: Input, pruning: Boolean): Result = {
    val begin = System.nanoTime()
    validate(input)
    val macroId = id("collapsed-block")
    require(!input.weights.contains(macroId), "reserved macro operator ID")
    def collapsed(link: PhysicalLink): PhysicalLink =
      link.copy(
        fromOpId = if (input.block(link.fromOpId)) macroId else link.fromOpId,
        toOpId = if (input.block(link.toOpId)) macroId else link.toOpId
      )
    val internal = input.plan.links.filter(l => input.block(l.fromOpId) && input.block(l.toOpId))
    val remaining = input.plan.links -- internal
    val required = input.plan.getBlockingAndDependeeLinks
    val inner = planOf(input.block, internal, required.intersect(internal))
    val outer = planOf(
      input.outside + macroId,
      remaining.map(collapsed),
      required.intersect(remaining).map(collapsed)
    )
    val decomposeEnd = System.nanoTime()
    val innerPlanner = generator(inner, input.weights)
    val innerResult = innerPlanner.bottomUpSearch(
      globalSearch = true,
      oChains = pruning,
      oCleanEdges = pruning,
      oEarlyStop = pruning
    )
    // Fixed boundaries isolate the macro. A zero estimate prevents double-counting block cost.
    val outerPlanner = generator(outer, input.weights + (macroId -> 0.0))
    val outerResult = outerPlanner.bottomUpSearch(
      globalSearch = true,
      oChains = pruning,
      oCleanEdges = pruning,
      oEarlyStop = pruning
    )
    val searchEnd = System.nanoTime()
    val originalOf = remaining.map(l => collapsed(l) -> l).toMap
    val materialized = required ++ innerResult.state ++ outerResult.state.map(originalOf)
    // Rebuild the flat plan with original IDs, storage URIs and worker allocations.
    val flat = generator(input.plan, input.weights)
      .evaluateMaterialization(materialized)
      .getOrElse(throw new IllegalStateException("composed Y plan is unschedulable"))
    require(flat.cost == innerResult.cost + outerResult.cost, "module costs do not compose")
    val end = System.nanoTime()
    Result(
      materialized,
      flat.regionDAG.vertexSet().asScala.map(_.getOperators.map(_.id)).toSet,
      flat.cost,
      innerResult.numStatesExplored + outerResult.numStatesExplored,
      decomposeEnd - begin,
      searchEnd - decomposeEnd,
      end - searchEnd,
      end - begin
    )
  }

  def main(args: Array[String]): Unit = {
    println(
      s"Planner classes: ${classOf[CostBasedScheduleGenerator].getProtectionDomain.getCodeSource.getLocation}"
    )
    println(s"Java runtime: ${System.getProperty("java.runtime.version")}")
    require(args.length <= 3, "usage: [output.csv] [repetitions] [warmups]")
    val output = Paths.get(args.headOption.getOrElse("target/xy-planning/results.csv"))
    val repetitions = args.lift(1).map(_.toInt).getOrElse(5)
    val warmups = args.lift(2).map(_.toInt).getOrElse(2)
    require(repetitions > 0 && warmups >= 0)
    Option(output.toAbsolutePath.getParent).foreach(Files.createDirectories(_))
    val writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)
    try {
      writer.write(
        "inside_edges,outside_edges,pruning,repetition,order,strategy,total_ns,decomposition_ns,search_ns,composition_ns,search_states,cost,regions\n"
      )
      for ((inside, outside) <- Seq((3, 1), (3, 3), (5, 1), (5, 3)); pruning <- Seq(false, true)) {
        val input = fixture(inside, outside)
        for (r <- -warmups until repetitions) {
          val order = if ((r & 1) == 0) Vector("X", "Y") else Vector("Y", "X")
          val results = order.map(s =>
            s -> (if (s == "X") searchX(input, pruning) else searchY(input, pruning))
          )
          require(results(0)._2.cost == results(1)._2.cost, "X/Y selected costs differ")
          results.foreach {
            case (strategy, result) =>
              require(
                evaluate(input, result.materialized).contains(result.cost),
                "invalid returned plan"
              )
              if (r >= 0)
                writer.write(
                  s"$inside,${outside + 1},$pruning,$r,${order.mkString},$strategy,${result.totalNs},${result.decompositionNs},${result.searchNs},${result.compositionNs},${result.states},${result.cost},${result.regions.size}\n"
                )
          }
          writer.flush()
        }
        println(s"XY checked inside=$inside outside=${outside + 1} pruning=$pruning")
      }
    } finally writer.close()
    println(s"XY results: ${output.toAbsolutePath}")
  }
}
