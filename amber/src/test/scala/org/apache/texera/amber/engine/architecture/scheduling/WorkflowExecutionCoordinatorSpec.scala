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
import org.apache.texera.amber.core.workflow.PhysicalOp
import org.apache.texera.amber.engine.architecture.controller.execution.WorkflowExecution
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable

class WorkflowExecutionCoordinatorSpec extends AnyFlatSpec {

  private def region(regionId: Long, opId: String): Region = {
    val physicalOp = PhysicalOp(
      PhysicalOpIdentity(OperatorIdentity(opId), "main"),
      WorkflowIdentity(0),
      ExecutionIdentity(0),
      OpExecInitInfo.Empty
    )
    Region(RegionIdentity(regionId), Set(physicalOp), Set.empty)
  }

  "WorkflowExecutionCoordinator.jumpToRegionContainingOperator" should "make the next scheduled region contain the target operator's region" in {
    val firstRegion = region(1, "first")
    val secondRegion = region(2, "second")
    val thirdRegion = region(3, "third")
    val schedule = Schedule(
      Map(
        0 -> Set(firstRegion),
        1 -> Set(secondRegion),
        2 -> Set(thirdRegion)
      )
    )
    val nextRegionLevel: mutable.ArrayBuffer[Option[Int]] = mutable.ArrayBuffer(None)
    val coordinator =
      new WorkflowExecutionCoordinator(
        () =>
          nextRegionLevel(0)
            .orElse(Some(schedule.startingLevel))
            .filter(schedule.levelSets.contains)
            .map { level =>
              nextRegionLevel(0) = Some(level + 1)
              schedule.levelSets(level)
            }
            .getOrElse(Set.empty),
        opId =>
          nextRegionLevel(0) = schedule.levelSets.collectFirst {
            case (level, regions) if regions.exists(_.getOperators.exists(_.id.logicalOpId == opId)) =>
              level
          },
        WorkflowExecution(),
        null,
        null
      )

    assert(coordinator.pullNextRegions == Set(firstRegion))
    assert(coordinator.pullNextRegions == Set(secondRegion))

    coordinator.jumpToRegionContainingOperator(OperatorIdentity("first"))

    assert(coordinator.pullNextRegions == Set(firstRegion))
  }
}
