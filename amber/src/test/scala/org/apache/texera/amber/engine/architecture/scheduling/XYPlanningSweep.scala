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

import org.apache.texera.amber.core.virtualidentity._
import org.apache.texera.amber.core.workflow._

import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Random

/** Bounded, reproducible planning sweep; no data execution or production Y integration. */
object XYPlanningSweep {
  import XYPlanningComparison._

  case class Scenario(name: String, shape: String, size: Int, outside: Int, profile: String)

  val scenarios: Vector[Scenario] = {
    val shapes = Vector(
      ("chain-small", "chain", 3, 3),
      ("chain-medium", "chain", 5, 5),
      ("chain-large", "chain", 6, 6),
      ("shortcut-small", "shortcut", 3, 3),
      ("shortcut-medium", "shortcut", 5, 5),
      ("shortcut-large", "shortcut", 6, 6),
      ("diamond-small", "diamond", 1, 2),
      ("diamond-medium", "diamond", 2, 3),
      ("diamond-large", "diamond", 3, 2),
      ("fanout-small", "fanout", 1, 1),
      ("fanout-medium", "fanout", 2, 2),
      ("fanout-large", "fanout", 4, 2),
      ("chain-outer-heavy", "chain", 2, 10),
      ("chain-inner-heavy", "chain", 10, 2),
      ("shortcut-outer-heavy", "shortcut", 3, 9),
      ("shortcut-inner-heavy", "shortcut", 10, 2)
    )
    for ((name, shape, size, outside) <- shapes; profile <- Vector("uniform", "skewed"))
      yield Scenario(s"$name-$profile", shape, size, outside, profile)
  }

  def fixtureFor(s: Scenario): Input = {
    require(Set("chain", "shortcut", "diamond", "fanout")(s.shape), "unknown shape")
    require(Set("uniform", "skewed")(s.profile), "unknown profile")
    require(s.size >= 1 && s.outside >= 1, "positive sizes required")
    require(s.shape != "chain" || s.size >= 2, "chain needs two edges")
    require(s.shape != "shortcut" || s.size >= 3, "shortcut needs three edges")
    def id(name: String): PhysicalOpIdentity = PhysicalOpIdentity(OperatorIdentity(name), "main")
    val start = id("LS")
    val end = id("LE")
    val links = mutable.Set.empty[PhysicalLink]
    val required = mutable.Set.empty[PhysicalLink]
    def add(a: PhysicalOpIdentity, b: PhysicalOpIdentity, stored: Boolean = false): Unit = {
      val link = PhysicalLink(a, PortIdentity(if (stored) 1 else 0), b, PortIdentity())
      links += link
      if (stored) required += link
    }
    s.shape match {
      case "chain" | "shortcut" =>
        val nodes = Vector(start) ++ (1 until s.size).map(i => id(s"b$i")) ++ Vector(end)
        nodes.sliding(2).foreach(p => add(p(0), p(1)))
        if (s.shape == "shortcut") add(nodes(1), end, stored = true)
      case "diamond" =>
        var previous = start
        for (i <- 1 to s.size) {
          val a = id(s"d${i}a")
          val b = id(s"d${i}b")
          val join = if (i == s.size) end else id(s"d${i}join")
          add(previous, a)
          add(previous, b)
          add(a, join, stored = i == 1)
          add(b, join)
          previous = join
        }
      case "fanout" =>
        for (i <- 0 to s.size) {
          val arm = id(s"arm$i")
          add(start, arm)
          add(arm, end)
        }
        add(start, end, stored = true)
    }
    val block = links.flatMap(l => Set(l.fromOpId, l.toOpId)).toSet
    val prefix = (0 to s.outside).map(i => id(s"s$i"))
    prefix.sliding(2).foreach(p => add(p(0), p(1)))
    val sink = id("sink")
    add(prefix.last, start, stored = true)
    add(end, sink, stored = true)
    if (Set("shortcut", "fanout")(s.shape)) add(prefix.head, sink)
    val ids = block ++ prefix + sink
    val random = new Random(17)
    val weights = ids.toVector
      .sortBy(_.logicalOpId.id)
      .map { op =>
        op -> (if (s.profile == "uniform") 1.0 else Vector(1.0, 2.0, 4.0, 16.0)(random.nextInt(4)))
      }
      .toMap
    Input(planOf(ids, links.toSet, required.toSet), block, start, end, weights)
  }

  def selectable(input: Input): (Int, Int) = {
    val edges = input.plan.links -- input.plan.getBlockingAndDependeeLinks
    val inside = edges.count(l => input.block(l.fromOpId) && input.block(l.toOpId))
    (inside, edges.size - inside)
  }

  private def gc: (Long, Long) = {
    val beans = ManagementFactory.getGarbageCollectorMXBeans.asScala
    (beans.map(_.getCollectionCount.max(0)).sum, beans.map(_.getCollectionTime.max(0)).sum)
  }

  def main(args: Array[String]): Unit = {
    require(args.length == 4, "usage: output.csv repetitions warmups fork")
    val output = Paths.get(args(0))
    val repetitions = args(1).toInt
    val warmups = args(2).toInt
    val fork = args(3).toInt
    require(repetitions > 0 && warmups >= 0 && fork >= 0)
    println(
      s"Planner classes: ${classOf[CostBasedScheduleGenerator].getProtectionDomain.getCodeSource.getLocation}"
    )
    println(s"Java runtime: ${System.getProperty("java.runtime.version")}")
    Option(output.toAbsolutePath.getParent).foreach(Files.createDirectories(_))
    val writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)
    val manifest =
      Files.newBufferedWriter(output.resolveSibling("operators.csv"), StandardCharsets.UTF_8)
    val edges = Files.newBufferedWriter(output.resolveSibling("edges.csv"), StandardCharsets.UTF_8)
    val minimum = mutable.Map.empty[String, Double]
    try {
      writer.write(
        "case,shape,profile,inside_edges,outside_edges,operators,links,pruning,fork,repetition,order,strategy,total_ns,decomposition_ns,search_ns,composition_ns,search_states,cost,regions,gc_count,gc_ms\n"
      )
      manifest.write("case,operator,inside,estimate\n")
      edges.write("case,from,to,output_port,required\n")
      val random = new Random(1009 + fork)
      for (s <- random.shuffle(scenarios)) {
        val input = fixtureFor(s)
        val (inside, outside) = selectable(input)
        require(inside + outside <= 13, "exhaustive sweep limited to 8192 assignments")
        input.weights.toVector.sortBy(_._1.logicalOpId.id).foreach {
          case (op, weight) =>
            manifest.write(s"${s.name},${op.logicalOpId.id},${input.block(op)},$weight\n")
        }
        input.plan.links.toVector.sortBy(_.toString).foreach { link =>
          edges.write(
            s"${s.name},${link.fromOpId.logicalOpId.id},${link.toOpId.logicalOpId.id},${link.fromPortId.id},${input.plan
              .getBlockingAndDependeeLinks(link)}\n"
          )
        }
        manifest.flush()
        edges.flush()
        for (pruning <- random.shuffle(Vector(false, true))) {
          for (r <- -warmups until repetitions) {
            val order = if (((r + fork) & 1) == 0) Vector("X", "Y") else Vector("Y", "X")
            order.foreach { strategy =>
              val beforeGC = gc
              val result = if (strategy == "X") searchX(input, pruning) else searchY(input, pruning)
              val afterGC = gc
              val reference = minimum.getOrElseUpdate(s.name, result.cost)
              require(result.cost == reference, s"cost mismatch: ${s.name}/$pruning/$strategy")
              require(
                evaluate(input, result.materialized).contains(result.cost),
                "invalid returned plan"
              )
              require(result.regions.flatten == input.plan.operators.map(_.id), "missing operators")
              require(
                result.regions.toSeq.map(_.size).sum == input.plan.operators.size,
                "duplicate operator"
              )
              if (r >= 0) {
                writer.write(
                  s"${s.name},${s.shape},${s.profile},$inside,$outside,${input.plan.operators.size},${input.plan.links.size},$pruning,$fork,$r,${order.mkString},$strategy,${result.totalNs},${result.decompositionNs},${result.searchNs},${result.compositionNs},${result.states},${result.cost},${result.regions.size},${afterGC._1 - beforeGC._1},${afterGC._2 - beforeGC._2}\n"
                )
                writer.flush()
              }
            }
          }
          println(
            s"XY sweep fork=$fork case=${s.name} edges=$inside/$outside pruning=$pruning checked"
          )
        }
      }
    } finally {
      writer.close()
      manifest.close()
      edges.close()
    }
    println(s"XY sweep results: ${output.toAbsolutePath}")
  }
}
