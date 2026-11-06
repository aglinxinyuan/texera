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

/** Sensitivity profiles only: both physical stages of the chosen family cost 8, all others 1. */
object NativeWeightedXYFixtures {
  val profiles: Vector[String] = Vector("aggregation-heavy", "join-heavy")

  def withProfile(
      base: CompiledXYFixtures.Compiled,
      profile: String
  ): CompiledXYFixtures.Compiled = {
    require(profiles.contains(profile), "unknown cost profile")
    val heavyLayers =
      if (profile == "aggregation-heavy") Set("localAgg", "globalAgg") else Set("build", "probe")
    val weights = base.input.plan.operators.map { op =>
      op.id -> (if (heavyLayers(op.id.layerName)) 8.0 else 1.0)
    }.toMap
    base.copy(input = base.input.copy(weights = weights))
  }
}

object NativeWeightedXYBenchmark {
  def main(args: Array[String]): Unit =
    CompiledXYBenchmark.run(
      args,
      for {
        profile <- NativeWeightedXYFixtures.profiles
        (inside, outside) <- NativeBranchXYFixtures.sizes
      } yield {
        val workflow = NativeBranchXYFixtures.name(inside, outside)
        (
          s"${profile}__$workflow",
          workflow,
          () =>
            NativeWeightedXYFixtures.withProfile(
              NativeBranchXYFixtures.compile(inside, outside),
              profile
            )
        )
      },
      exhaustiveLimit = 8
    )
}
