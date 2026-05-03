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

package org.apache.texera.amber.engine.architecture.messaginglayer

import org.apache.texera.amber.config.ApplicationConfig
import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import org.apache.texera.amber.engine.architecture.common.WorkflowActor.NetworkMessage
import org.apache.texera.amber.engine.common.ambermessage.{
  WorkflowFIFOMessage,
  WorkflowFIFOMessagePayload
}
import org.scalatest.flatspec.AnyFlatSpec

class FlowControlSpec extends AnyFlatSpec {

  private val channelId =
    ChannelIdentity(ActorVirtualIdentity("from"), ActorVirtualIdentity("to"), isControl = false)

  // A non-DataFrame payload so that `WorkflowMessage.getInMemSize` falls through to
  // the 200L default branch — using DataFrame(Array.empty) yields 0 bytes, which
  // would let any message squeeze through even when the configured credit is 0.
  private case class FixedSizePayload() extends WorkflowFIFOMessagePayload
  private val fixedMsgSize = 200L

  private def msg(id: Long): NetworkMessage =
    NetworkMessage(id, WorkflowFIFOMessage(channelId, id, FixedSizePayload()))

  private val maxBytes = ApplicationConfig.maxCreditAllowedInBytesPerChannel

  "FlowControl" should "report full credit and not be overloaded initially" in {
    val fc = new FlowControl()
    assert(fc.getCredit == maxBytes)
    assert(!fc.isOverloaded)
  }

  "FlowControl.getMessagesToSend" should "forward an incoming message when credit is available" in {
    val fc = new FlowControl()
    val out = fc.getMessagesToSend(msg(1L)).toList
    assert(out == List(msg(1L)))
    assert(!fc.isOverloaded)
  }

  it should "stash an incoming message and become overloaded when credit is exhausted" in {
    val fc = new FlowControl()
    // exhaust the receiver-side credit so getCredit drops to 0
    fc.updateQueuedCredit(maxBytes)
    assert(fc.getCredit == 0L)

    val out = fc.getMessagesToSend(msg(1L)).toList
    assert(out.isEmpty)
    assert(fc.isOverloaded)
  }

  it should "drain stashed messages once credit is restored" in {
    val fc = new FlowControl()
    fc.updateQueuedCredit(maxBytes)
    val firstAttempt = fc.getMessagesToSend(msg(1L)).toList
    assert(firstAttempt.isEmpty)
    assert(fc.isOverloaded)

    fc.updateQueuedCredit(0L)
    val drained = fc.getMessagesToSend.toList
    assert(drained == List(msg(1L)))
    assert(!fc.isOverloaded)
  }

  "FlowControl.updateQueuedCredit" should "shrink the available credit" in {
    val fc = new FlowControl()
    fc.updateQueuedCredit(100L)
    assert(fc.getCredit == maxBytes - 100L)
  }

  it should "be relative to the latest call (not cumulative)" in {
    val fc = new FlowControl()
    fc.updateQueuedCredit(100L)
    fc.updateQueuedCredit(50L)
    assert(fc.getCredit == maxBytes - 50L)
  }

  "FlowControl.decreaseInflightCredit" should "adjust the available credit by the amount removed" in {
    val fc = new FlowControl()
    // Subtracting a negative amount equivalently grows the inflight bucket. This
    // exercises the accounting formula `maxBytes - inflightCredit - queuedCredit`
    // without requiring a sized payload to seed inflight.
    fc.decreaseInflightCredit(-100L)
    assert(fc.getCredit == maxBytes - 100L)

    fc.decreaseInflightCredit(100L) // restore inflight to 0
    assert(fc.getCredit == maxBytes)
  }
}
