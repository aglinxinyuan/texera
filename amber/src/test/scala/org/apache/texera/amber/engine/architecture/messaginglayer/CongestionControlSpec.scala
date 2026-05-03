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

import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import org.apache.texera.amber.engine.architecture.common.WorkflowActor.NetworkMessage
import org.apache.texera.amber.engine.common.ambermessage.{DataFrame, WorkflowFIFOMessage}
import org.scalatest.flatspec.AnyFlatSpec

class CongestionControlSpec extends AnyFlatSpec {

  private val channelId =
    ChannelIdentity(ActorVirtualIdentity("from"), ActorVirtualIdentity("to"), isControl = false)

  private def msg(id: Long): NetworkMessage =
    NetworkMessage(id, WorkflowFIFOMessage(channelId, id, DataFrame(Array.empty)))

  "CongestionControl.canSend" should "be true initially with empty in-transit set" in {
    val cc = new CongestionControl()
    assert(cc.canSend)
  }

  it should "become false once in-transit messages reach the window size" in {
    val cc = new CongestionControl()
    // initial windowSize = 1
    cc.markMessageInTransit(msg(1L))
    assert(!cc.canSend)
  }

  "CongestionControl.ack" should "be a no-op for an unknown message id" in {
    val cc = new CongestionControl()
    cc.markMessageInTransit(msg(1L))
    cc.ack(99L)
    assert(cc.getInTransitMessages.exists(_.messageId == 1L))
  }

  it should "remove an acked in-transit message and allow more sending" in {
    val cc = new CongestionControl()
    cc.markMessageInTransit(msg(1L))
    cc.ack(1L)
    assert(!cc.getInTransitMessages.exists(_.messageId == 1L))
    assert(cc.canSend)
  }

  it should "grow the window via slow start when acked within the ack time limit" in {
    val cc = new CongestionControl()
    cc.markMessageInTransit(msg(1L))
    cc.ack(1L) // immediate ack — well within ackTimeLimit (3s)
    // After the first slow-start ack, windowSize should be at least 2.
    cc.markMessageInTransit(msg(2L))
    assert(
      cc.canSend,
      "window must permit at least one more in-transit message after slow-start ack"
    )
  }

  "CongestionControl.getBufferedMessagesToSend" should "be bounded by remaining window capacity" in {
    val cc = new CongestionControl()
    cc.enqueueMessage(msg(1L))
    cc.enqueueMessage(msg(2L))
    cc.enqueueMessage(msg(3L))
    // initial windowSize = 1, inTransit.size = 0  →  send up to 1
    val first = cc.getBufferedMessagesToSend.toList
    assert(first.size == 1)
    assert(first.head.messageId == 1L)
  }

  it should "return an empty iterable when the window is fully consumed" in {
    val cc = new CongestionControl()
    cc.markMessageInTransit(msg(1L))
    cc.enqueueMessage(msg(2L))
    assert(cc.getBufferedMessagesToSend.isEmpty)
  }

  "CongestionControl.getAllMessages" should "include both in-transit and queued messages" in {
    val cc = new CongestionControl()
    cc.markMessageInTransit(msg(1L))
    cc.enqueueMessage(msg(2L))
    val all = cc.getAllMessages.map(_.messageId).toSet
    assert(all == Set(1L, 2L))
  }

  "CongestionControl.getTimedOutInTransitMessages" should "be empty when no message has been marked in transit" in {
    val cc = new CongestionControl()
    assert(cc.getTimedOutInTransitMessages.isEmpty)
  }

  it should "exclude messages that are still inside the resend time limit" in {
    val cc = new CongestionControl()
    cc.markMessageInTransit(msg(1L))
    // The message was just enqueued, so it is well inside the 60s resend
    // window and must not be reported as timed out.
    assert(cc.getTimedOutInTransitMessages.isEmpty)
  }

  "CongestionControl.enqueueMessage" should "not place the message into the in-transit set on its own" in {
    val cc = new CongestionControl()
    cc.enqueueMessage(msg(1L))
    assert(cc.getInTransitMessages.isEmpty)
    // The message should still surface via getAllMessages (which unions
    // inTransit and toBeSent), proving it was buffered, not dropped.
    assert(cc.getAllMessages.exists(_.messageId == 1L))
  }

  "CongestionControl.getStatusReport" should "include window size, in-transit count, and waiting count" in {
    val cc = new CongestionControl()
    cc.markMessageInTransit(msg(1L))
    cc.enqueueMessage(msg(2L))
    val report = cc.getStatusReport
    assert(report.contains("window size"))
    assert(report.contains("in transit"))
    assert(report.contains("waiting"))
  }
}
