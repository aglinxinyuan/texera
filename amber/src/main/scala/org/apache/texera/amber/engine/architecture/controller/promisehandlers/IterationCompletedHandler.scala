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

package org.apache.texera.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import org.apache.texera.amber.core.WorkflowRuntimeException
import org.apache.texera.amber.core.workflow.GlobalPortIdentity
import org.apache.texera.amber.engine.architecture.controller.{
  ControllerAsyncRPCHandlerInitializer,
  FatalError
}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  IterationCompletedRequest,
  QueryStatisticsRequest
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import org.apache.texera.amber.engine.common.virtualidentity.util.CONTROLLER
import org.apache.texera.amber.util.VirtualIdentityUtils

/** Notify controller that a worker has completed an iteration on an output port.
  *
  * This is different from [[PortCompletedHandler]]: a port can have multiple iterations
  * (e.g., loop execution) before the whole port is fully completed.
  */
trait IterationCompletedHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  override def iterationCompleted(
      msg: IterationCompletedRequest,
      ctx: AsyncRPCContext
  ): Future[EmptyReturn] = {
    controllerInterface
      .controllerInitiateQueryStatistics(QueryStatisticsRequest(scala.Seq(ctx.sender)), CONTROLLER)
      .map { _ =>
        val globalPortId = GlobalPortIdentity(
          VirtualIdentityUtils.getPhysicalOpId(ctx.sender),
          msg.portId
        )

        cp.workflowExecutionCoordinator.getRegionOfPortId(globalPortId) match {
          case Some(region) =>
            // Emit UI-only IterationCompleted phase for this region.
            cp.workflowExecutionCoordinator.markRegionIterationCompletedIfNeeded(region)

            // Keep scheduler running
            cp.workflowExecutionCoordinator
              .coordinateRegionExecutors(cp.actorService)
              .onFailure {
                case err: WorkflowRuntimeException =>
                  sendToClient(FatalError(err, err.relatedWorkerId))
                case other =>
                  sendToClient(FatalError(other, None))
              }
          case None =>
        }

        EmptyReturn()
      }
  }
}
