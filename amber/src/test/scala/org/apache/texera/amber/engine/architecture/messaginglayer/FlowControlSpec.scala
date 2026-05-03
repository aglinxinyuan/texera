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
  WorkflowFIFOMessagePayload,
  WorkflowMessage
}
import org.scalatest.flatspec.AnyFlatSpec

class FlowControlSpec extends AnyFlatSpec {

  private val channelId =
    ChannelIdentity(ActorVirtualIdentity("from"), ActorVirtualIdentity("to"), isControl = false)

  // A non-DataFrame payload so that `WorkflowMessage.getInMemSize` falls through to
  // the 200L default branch — using DataFrame(Array.empty) yields 0 bytes, which
  // would let any message squeeze through even when the configured credit is 0.
  private case class FixedSizePayload() extends WorkflowFIFOMessagePayload

  private def msg(id: Long): NetworkMessage =
    NetworkMessage(id, WorkflowFIFOMessage(channelId, id, FixedSizePayload()))

  // Pin the assumed payload size so the test fails loudly if WorkflowMessage's
  // size accounting changes in a way that would invalidate the credit math below.
  private val msgSize: Long = WorkflowMessage.getInMemSize(msg(0).internalMessage)
  assert(msgSize == 200L)

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

  it should "force new messages through the stash whenever the stash is non-empty" in {
    // While the stash is non-empty, even a new message must be stashed first
    // and then drained in FIFO order — never sent ahead of older stashed work.
    val fc = new FlowControl()
    fc.updateQueuedCredit(maxBytes)
    fc.getMessagesToSend(msg(1L)) // stash msg(1L)
    assert(fc.isOverloaded)

    // Restore enough credit for 2 messages, then push a new one. The branch
    // under test always stashes the new message and then drains FIFO.
    fc.updateQueuedCredit(maxBytes - 2 * msgSize)
    val drained = fc.getMessagesToSend(msg(2L)).toList
    assert(drained == List(msg(1L), msg(2L)))
    assert(!fc.isOverloaded)
  }

  it should "leave isOverloaded true when only some stashed messages can be drained" in {
    val fc = new FlowControl()
    fc.updateQueuedCredit(maxBytes)
    fc.getMessagesToSend(msg(1L))
    fc.getMessagesToSend(msg(2L))
    assert(fc.isOverloaded)

    // Restore credit for exactly one message; the second remains stashed.
    fc.updateQueuedCredit(maxBytes - msgSize)
    val drained = fc.getMessagesToSend.toList
    assert(drained == List(msg(1L)))
    assert(fc.isOverloaded, "stash still has msg(2L), so overloaded must remain true")
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

  "FlowControl.decreaseInflightCredit" should "free credit equal to the acked amount" in {
    val fc = new FlowControl()

    // Send a message through to seed `inflightCredit` with the actual size used
    // by FlowControl's accounting. This avoids passing an invalid (negative)
    // amount to `decreaseInflightCredit`.
    fc.getMessagesToSend(msg(1L)).toList
    assert(fc.getCredit == maxBytes - msgSize)

    fc.decreaseInflightCredit(msgSize)
    assert(fc.getCredit == maxBytes)
  }
}
