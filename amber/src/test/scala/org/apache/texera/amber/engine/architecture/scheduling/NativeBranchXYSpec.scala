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

class NativeBranchXYSpec extends AnyFlatSpec {
  import XYPlanningComparison._
  for ((inside,outside) <- Seq((1,1),(2,2),(3,3),(4,4),(1,4),(4,1))) {
    "Native branching X/Y" should s"retain compiler cuts and equal costs with $inside/$outside groups" in {
      val compiled = NativeBranchXYFixtures.compile(inside,outside)
      val input = compiled.input
      val groups = inside+outside
      assert(input.plan.operators.size == 4+4*groups)
      assert(input.plan.links.size == 3+5*groups)
      assert(compiled.original.getBlockingAndDependeeLinks.size == 2*groups)
      val boundary = input.plan.links.filter(l => input.block(l.fromOpId) != input.block(l.toOpId))
      assert(boundary.size == 2)
      assert(input.plan.getBlockingAndDependeeLinks == compiled.original.getBlockingAndDependeeLinks ++ boundary)
      assert(input.block.size == 2+4*inside)
      assert(input.outside.size == 2+4*outside)
      assert(input.plan.operators.map(op => CompiledXYBenchmark.operatorKey(op.id)).size == input.plan.operators.size)
      val x = searchX(input,pruning=true)
      val y = searchY(input,pruning=true)
      assert(x.cost == y.cost)
      if (groups==2) assert(x.cost == searchX(input,pruning=false).cost)
      for (r <- Seq(x,y)) {
        assert(evaluate(input,r.materialized).contains(r.cost))
        assert(r.regions.flatten == input.plan.operators.map(_.id))
        assert(r.regions.toSeq.map(_.size).sum == input.plan.operators.size)
      }
    }
  }
  it should "reject unsupported counts and bad aggregate columns" in {
    intercept[IllegalArgumentException](NativeBranchXYFixtures.compile(0,1))
    intercept[IllegalArgumentException](NativeBranchXYFixtures.compile(1,5))
    intercept[Exception](NativeBranchXYFixtures.compile(1,1,"missing_column"))
  }
}
