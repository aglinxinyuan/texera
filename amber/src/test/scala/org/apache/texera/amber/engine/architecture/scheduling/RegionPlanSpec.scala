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
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.{
  GlobalPortIdentity,
  PhysicalLink,
  PhysicalOp,
  PortIdentity
}
import org.scalatest.flatspec.AnyFlatSpec

class RegionPlanSpec extends AnyFlatSpec {

  private def physicalOpId(opId: String): PhysicalOpIdentity =
    PhysicalOpIdentity(OperatorIdentity(opId), "main")

  private def op(opId: String): PhysicalOp =
    PhysicalOp(
      physicalOpId(opId),
      WorkflowIdentity(0),
      ExecutionIdentity(0),
      OpExecInitInfo.Empty
    )

  private def link(fromOp: String, toOp: String): PhysicalLink =
    PhysicalLink(physicalOpId(fromOp), PortIdentity(0), physicalOpId(toOp), PortIdentity(0))

  private def globalPort(opId: String): GlobalPortIdentity =
    GlobalPortIdentity(physicalOpId(opId), PortIdentity(0), input = true)

  "RegionPlan.getRegion" should "return the region with the given id" in {
    val r0 = Region(RegionIdentity(0), Set(op("a")), Set.empty)
    val r1 = Region(RegionIdentity(1), Set(op("b")), Set.empty)
    val plan = RegionPlan(Set(r0, r1), Set.empty)

    assert(plan.getRegion(RegionIdentity(0)) == r0)
    assert(plan.getRegion(RegionIdentity(1)) == r1)
  }

  it should "throw NoSuchElementException for an unknown region id" in {
    val plan = RegionPlan(Set(Region(RegionIdentity(0), Set(op("a")), Set.empty)), Set.empty)
    assertThrows[NoSuchElementException] {
      plan.getRegion(RegionIdentity(99))
    }
  }

  "RegionPlan.getRegionOfLink" should "return the region whose physicalLinks include the link" in {
    val ab = link("a", "b")
    val r0 = Region(RegionIdentity(0), Set(op("a"), op("b")), Set(ab))
    val r1 = Region(RegionIdentity(1), Set(op("c")), Set.empty)
    val plan = RegionPlan(Set(r0, r1), Set.empty)

    assert(plan.getRegionOfLink(ab) == r0)
  }

  "RegionPlan.getRegionOfPortId" should "find the region whose ports contain the global port id" in {
    val portA = globalPort("a")
    val r0 = Region(RegionIdentity(0), Set(op("a")), Set.empty, ports = Set(portA))
    val r1 = Region(RegionIdentity(1), Set(op("b")), Set.empty)
    val plan = RegionPlan(Set(r0, r1), Set.empty)

    assert(plan.getRegionOfPortId(portA).contains(r0))
  }

  it should "return None when no region claims the port" in {
    val r0 = Region(RegionIdentity(0), Set(op("a")), Set.empty)
    val plan = RegionPlan(Set(r0), Set.empty)

    assert(plan.getRegionOfPortId(globalPort("missing")).isEmpty)
  }

  "RegionPlan.topologicalIterator" should "yield region ids in topological order based on regionLinks" in {
    val r0 = Region(RegionIdentity(0), Set(op("a")), Set.empty)
    val r1 = Region(RegionIdentity(1), Set(op("b")), Set.empty)
    val r2 = Region(RegionIdentity(2), Set(op("c")), Set.empty)
    val plan = RegionPlan(
      regions = Set(r0, r1, r2),
      regionLinks = Set(
        RegionLink(r0.id, r1.id),
        RegionLink(r1.id, r2.id)
      )
    )

    assert(plan.topologicalIterator().toList == List(r0.id, r1.id, r2.id))
  }
}
