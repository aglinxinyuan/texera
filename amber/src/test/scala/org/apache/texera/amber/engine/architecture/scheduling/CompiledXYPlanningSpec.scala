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

import org.apache.texera.amber.core.executor.OpExecWithCode
import org.scalatest.flatspec.AnyFlatSpec
import java.nio.file.Files

class CompiledXYPlanningSpec extends AnyFlatSpec {
  import XYPlanningComparison._
  for ((name, file) <- CompiledXYFixtures.files) {
    "Compiled X/Y plans" should s"retain schemas and equal minimum costs for terminal $name" in {
      val compiled = CompiledXYFixtures.compile(CompiledXYFixtures.folder.resolve(file))
      val input = compiled.input
      val expected = if (name == "branching-files") 5 else 4
      assert(input.plan.operators.size == expected)
      assert(input.plan.links.size == (if (name == "branching-files") 5 else 3))
      assert(input.plan.operators.forall(_.opExecInitInfo.isInstanceOf[OpExecWithCode]))
      assert(input.plan.getOperator(input.start).requiresMaterializedExecution)
      assert(input.plan.getBlockingAndDependeeLinks.size == 1)
      val x = searchX(input, pruning = true)
      val y = searchY(input, pruning = true)
      val exhaustive = searchX(input, pruning = false)
      assert(x.cost == y.cost && x.cost == exhaustive.cost)
      for (result <- Seq(x, y)) {
        assert(evaluate(input, result.materialized).contains(result.cost))
        assert(result.regions.flatten == input.plan.operators.map(_.id))
        assert(result.regions.toSeq.map(_.size).sum == expected)
      }
    }
  }
  it should "reject an invalid exported schema instead of benchmarking a partial graph" in {
    val original = Files.readString(CompiledXYFixtures.folder.resolve("demo2_per_file_counts.json"))
    val bad = Files.createTempFile("xy-invalid-schema", ".json")
    try {
      Files.writeString(bad, original.replace("\"integer\"", "\"NOT_A_TYPE\""))
      intercept[Exception](CompiledXYFixtures.compile(bad))
    } finally Files.deleteIfExists(bad)
  }
}
