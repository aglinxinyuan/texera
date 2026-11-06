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

import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.texera.amber.core.virtualidentity.OperatorIdentity
import org.apache.texera.amber.core.workflow._
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.util.JSONUtils
import org.apache.texera.common.compiler.{CompilationErrorHandling, WorkflowCompiler}
import org.apache.texera.common.compiler.model.{LogicalLink, LogicalPlanPojo}

import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters._

/** Compile saved GUI demos strictly; add only the research model's loop-boundary cuts. */
object CompiledXYFixtures {
  val files: Vector[(String, String)] = Vector(
    "kmeans" -> "demo1_kmeans_choose_k.json",
    "files" -> "demo2_per_file_counts.json",
    "branching-files" -> "demo3_branching_file_metrics.json"
  )
  def folder: Path = Iterator.iterate(Paths.get("").toAbsolutePath)(_.getParent)
    .takeWhile(_ != null).map(_.resolve("experiment/xy-planning/workflows"))
    .find(Files.isDirectory(_)).getOrElse(throw new IllegalStateException("fixture folder missing"))

  case class Compiled(input: XYPlanningComparison.Input, original: PhysicalPlan, compileNs: Long)

  def compile(path: Path): Compiled = {
    val begin = System.nanoTime()
    val mapper = JSONUtils.objectMapper
    val json = mapper.readTree(path.toFile)
    val exported = json.get("operators").elements().asScala.toVector
    val operators = exported.map { node =>
      // These fixtures use default ports only. Refuse unsupported exports, never silently drop ports.
      for ((field, prefix) <- Seq("inputPorts" -> "input-", "outputPorts" -> "output-")) {
        val ports = node.get(field).elements().asScala.toVector
        require(ports.size <= 1 && ports.forall(_.get("portID").asText() == prefix + "0"))
      }
      val flat = node.get("operatorProperties").deepCopy[ObjectNode]()
      flat.put("operatorType", node.get("operatorType").asText())
      flat.put("operatorID", node.get("operatorID").asText())
      mapper.treeToValue(flat, classOf[LogicalOp])
    }
    val links = json.get("links").elements().asScala.map { node =>
      val from = node.get("source")
      val to = node.get("target")
      require(from.get("portID").asText() == "output-0" && to.get("portID").asText() == "input-0")
      LogicalLink(OperatorIdentity(from.get("operatorID").asText()), PortIdentity(),
        OperatorIdentity(to.get("operatorID").asText()), PortIdentity())
    }.toList
    val result = new WorkflowCompiler(new WorkflowContext()).compile(
      LogicalPlanPojo(operators.toList, links, List.empty, List.empty), CompilationErrorHandling.Strict)
    require(result.operatorIdToError.isEmpty, result.operatorIdToError.toString)
    val original = result.physicalPlan.getOrElse(throw new IllegalStateException("no physical plan"))
    require(original.operators.forall(op => op.inputPorts.values.forall(_._3.isRight) && op.outputPorts.values.forall(_._3.isRight)), "unresolved compiled schema")
    def endpoint(kind: String) = {
      val ids = exported.filter(_.get("operatorType").asText() == kind).map(_.get("operatorID").asText())
      require(ids.size == 1, "exactly one loop required")
      original.operators.find(_.id.logicalOpId.id == ids.head).get.id
    }
    val start = endpoint("LoopStart")
    val end = endpoint("LoopEnd")
    var block = Set(start)
    var next = original.links.filter(l => block(l.fromOpId)).map(_.toOpId) ++ block
    while (next != block) { block = next; next = original.links.filter(l => block(l.fromOpId)).map(_.toOpId) ++ block }
    require(original.links.forall(_.fromOpId != end), "these demos have terminal LoopEnd")
    val boundaries = original.links.filter(l => block(l.fromOpId) != block(l.toOpId))
    val plan = original.copy(operators = original.operators.map { op =>
      op.copy(outputPorts = op.outputPorts.map { case (port, (definition, outgoing, schema)) =>
        port -> ((definition.copy(blocking = definition.blocking || outgoing.exists(boundaries)), outgoing, schema))
      })
    })
    Compiled(XYPlanningComparison.Input(plan, block, start, end, plan.operators.map(_.id -> 1.0).toMap), original, System.nanoTime() - begin)
  }
}
