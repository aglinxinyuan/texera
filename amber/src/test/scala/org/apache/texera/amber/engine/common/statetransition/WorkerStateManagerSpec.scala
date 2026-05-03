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

package org.apache.texera.amber.engine.common.statetransition

import org.apache.texera.amber.core.virtualidentity.ActorVirtualIdentity
import org.apache.texera.amber.engine.architecture.worker.statistics.WorkerState
import org.apache.texera.amber.engine.architecture.worker.statistics.WorkerState._
import org.apache.texera.amber.engine.common.statetransition.StateManager.InvalidTransitionException
import org.scalatest.flatspec.AnyFlatSpec

class WorkerStateManagerSpec extends AnyFlatSpec {

  private val actorId: ActorVirtualIdentity = ActorVirtualIdentity("test-worker")

  private def newManager(initial: WorkerState = UNINITIALIZED): WorkerStateManager =
    new WorkerStateManager(actorId, initial)

  "WorkerStateManager" should "default to the UNINITIALIZED state" in {
    assert(newManager().getCurrentState == UNINITIALIZED)
  }

  it should "honor the explicit initial state when provided" in {
    assert(newManager(READY).getCurrentState == READY)
  }

  // -- Allowed transitions per the documented graph --

  it should "allow UNINITIALIZED -> READY" in {
    val sm = newManager()
    sm.transitTo(READY)
    assert(sm.getCurrentState == READY)
  }

  it should "allow READY -> RUNNING, RUNNING -> PAUSED, PAUSED -> RUNNING, RUNNING -> COMPLETED" in {
    val sm = newManager(READY)
    sm.transitTo(RUNNING)
    sm.transitTo(PAUSED)
    sm.transitTo(RUNNING)
    sm.transitTo(COMPLETED)
    assert(sm.getCurrentState == COMPLETED)
  }

  it should "allow READY -> PAUSED and READY -> COMPLETED directly" in {
    val sm1 = newManager(READY)
    sm1.transitTo(PAUSED)
    assert(sm1.getCurrentState == PAUSED)

    val sm2 = newManager(READY)
    sm2.transitTo(COMPLETED)
    assert(sm2.getCurrentState == COMPLETED)
  }

  // -- Disallowed transitions --

  it should "reject UNINITIALIZED -> RUNNING (must go through READY)" in {
    val sm = newManager()
    intercept[InvalidTransitionException] {
      sm.transitTo(RUNNING)
    }
  }

  it should "treat COMPLETED as a terminal state" in {
    val sm = newManager(COMPLETED)
    intercept[InvalidTransitionException] {
      sm.transitTo(RUNNING)
    }
    intercept[InvalidTransitionException] {
      sm.transitTo(READY)
    }
    // Self-transition is a no-op, not an exception.
    sm.transitTo(COMPLETED)
    assert(sm.getCurrentState == COMPLETED)
  }

  it should "reject transitions into TERMINATED (TERMINATED is absent from the graph)" in {
    Seq(UNINITIALIZED, READY, RUNNING, PAUSED).foreach { from =>
      val sm = newManager(from)
      intercept[InvalidTransitionException] {
        sm.transitTo(TERMINATED)
      }
    }
  }

  it should "reject PAUSED -> COMPLETED (only RUNNING -> COMPLETED is permitted)" in {
    val sm = newManager(PAUSED)
    intercept[InvalidTransitionException] {
      sm.transitTo(COMPLETED)
    }
  }
}
