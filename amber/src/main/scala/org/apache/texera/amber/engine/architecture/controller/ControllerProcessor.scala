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

package org.apache.texera.amber.engine.architecture.controller

import org.apache.texera.amber.core.virtualidentity.ActorVirtualIdentity
import org.apache.texera.amber.core.workflow.WorkflowContext
import org.apache.texera.amber.engine.architecture.common.{
  AkkaActorRefMappingService,
  AkkaActorService,
  AkkaMessageTransferService,
  AmberProcessor
}
import org.apache.texera.amber.engine.architecture.controller.execution.WorkflowExecution
import org.apache.texera.amber.engine.architecture.logreplay.ReplayLogManager
import org.apache.texera.amber.engine.architecture.scheduling.WorkflowExecutionCoordinator
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.MainThreadDelegateMessage
import org.apache.texera.amber.engine.common.ambermessage.WorkflowFIFOMessage

import scala.collection.mutable

class ControllerProcessor(
    workflowContext: WorkflowContext,
    controllerConfig: ControllerConfig,
    actorId: ActorVirtualIdentity,
    outputHandler: Either[MainThreadDelegateMessage, WorkflowFIFOMessage] => Unit
) extends AmberProcessor(actorId, outputHandler) {

  val workflowExecution: WorkflowExecution = WorkflowExecution()
  val workflowScheduler: WorkflowScheduler =
    new WorkflowScheduler(workflowContext, actorId)
  private val nextRegionLevel: mutable.ArrayBuffer[Option[Int]] = mutable.ArrayBuffer(None)
  val workflowExecutionCoordinator: WorkflowExecutionCoordinator = new WorkflowExecutionCoordinator(
    () => {
      val schedule = this.workflowScheduler.getSchedule
      if (schedule == null) {
        Set.empty
      } else {
        if (nextRegionLevel(0).isEmpty) {
          nextRegionLevel(0) = Some(schedule.startingLevel)
        }
        nextRegionLevel(0)
          .filter(schedule.levelSets.contains)
          .map { level =>
            nextRegionLevel(0) = Some(level + 1)
            schedule.levelSets(level)
          }
          .getOrElse(Set.empty)
      }
    },
    opId => {
      val schedule = this.workflowScheduler.getSchedule
      if (schedule != null) {
        nextRegionLevel(0) = schedule.levelSets.collectFirst {
          case (level, regions)
              if regions.exists(_.getOperators.exists(_.id.logicalOpId == opId)) =>
            level
        }
      }
    },
    workflowExecution,
    controllerConfig,
    asyncRPCClient
  )

  private val initializer = new ControllerAsyncRPCHandlerInitializer(this)

  @transient var controllerTimerService: ControllerTimerService = _

  def setupTimerService(controllerTimerService: ControllerTimerService): Unit = {
    this.controllerTimerService = controllerTimerService
  }

  @transient var transferService: AkkaMessageTransferService = _

  def setupTransferService(transferService: AkkaMessageTransferService): Unit = {
    this.transferService = transferService
  }

  @transient var actorService: AkkaActorService = _

  def setupActorService(akkaActorService: AkkaActorService): Unit = {
    this.actorService = akkaActorService
  }

  @transient var actorRefService: AkkaActorRefMappingService = _

  def setupActorRefService(actorRefService: AkkaActorRefMappingService): Unit = {
    this.actorRefService = actorRefService
    this.workflowExecutionCoordinator.setupActorRefService(this.actorRefService)
  }

  @transient var logManager: ReplayLogManager = _

  def setupLogManager(logManager: ReplayLogManager): Unit = {
    this.logManager = logManager
  }

}
