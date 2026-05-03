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

package org.apache.texera.amber.engine.architecture.logreplay

import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  EmbeddedControlMessageIdentity
}
import org.apache.texera.amber.engine.common.ambermessage.{
  DataFrame,
  WorkflowFIFOMessage,
  WorkflowFIFOMessagePayload
}
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable

class LogreplayPrimitivesSpec extends AnyFlatSpec {

  private val workerId = ActorVirtualIdentity("worker-1")
  private val cidA =
    ChannelIdentity(ActorVirtualIdentity("up1"), workerId, isControl = false)
  private val cidB =
    ChannelIdentity(ActorVirtualIdentity("up2"), workerId, isControl = false)

  private case class FixedSizePayload() extends WorkflowFIFOMessagePayload
  private def msg(seq: Long): WorkflowFIFOMessage =
    WorkflowFIFOMessage(cidA, seq, FixedSizePayload())

  // ---------------------------------------------------------------------------
  // ReplayLoggerImpl
  // ---------------------------------------------------------------------------

  "ReplayLoggerImpl.logCurrentStepWithMessage" should "append a ProcessingStep when the channel changes" in {
    val l = new ReplayLoggerImpl()
    l.logCurrentStepWithMessage(0L, cidA, None)
    val drained = l.drainCurrentLogRecords(0L)
    assert(drained.toList == List(ProcessingStep(cidA, 0L)))
  }

  it should "skip a same-channel call with no message" in {
    val l = new ReplayLoggerImpl()
    l.logCurrentStepWithMessage(0L, cidA, None)
    l.drainCurrentLogRecords(0L) // reset
    l.logCurrentStepWithMessage(1L, cidA, None) // same channel, no message
    val drained = l.drainCurrentLogRecords(1L)
    // Should still only carry the trailing ProcessingStep emitted by drain.
    assert(drained.toList == List(ProcessingStep(cidA, 1L)))
  }

  it should "append both a ProcessingStep and a MessageContent when a message is provided" in {
    val l = new ReplayLoggerImpl()
    val m = msg(7L)
    l.logCurrentStepWithMessage(2L, cidA, Some(m))
    val drained = l.drainCurrentLogRecords(2L)
    assert(drained.toList == List(ProcessingStep(cidA, 2L), MessageContent(m)))
  }

  it should "append a ProcessingStep on a channel switch even if no message is provided" in {
    val l = new ReplayLoggerImpl()
    l.logCurrentStepWithMessage(0L, cidA, None)
    l.logCurrentStepWithMessage(1L, cidB, None) // channel change → must record
    val drained = l.drainCurrentLogRecords(1L)
    assert(drained.toList == List(ProcessingStep(cidA, 0L), ProcessingStep(cidB, 1L)))
  }

  "ReplayLoggerImpl.markAsReplayDestination" should "append a ReplayDestination record to the buffer" in {
    val l = new ReplayLoggerImpl()
    val ecm = EmbeddedControlMessageIdentity("checkpoint-1")
    l.markAsReplayDestination(ecm)
    val drained = l.drainCurrentLogRecords(0L)
    // drain emits a trailing ProcessingStep too (lastStep != step), which is OK.
    assert(drained.contains(ReplayDestination(ecm)))
  }

  "ReplayLoggerImpl.drainCurrentLogRecords" should "clear the buffer between drains" in {
    val l = new ReplayLoggerImpl()
    l.logCurrentStepWithMessage(0L, cidA, None)
    val first = l.drainCurrentLogRecords(0L)
    val second = l.drainCurrentLogRecords(0L)
    assert(first.nonEmpty)
    assert(second.isEmpty, "second drain must yield no leftover records")
  }

  it should "append a synthetic ProcessingStep when the requested step differs from lastStep" in {
    val l = new ReplayLoggerImpl()
    l.logCurrentStepWithMessage(0L, cidA, None)
    val drained = l.drainCurrentLogRecords(5L)
    // Two records: the original ProcessingStep at step 0 and the synthetic one at step 5.
    assert(drained.toList == List(ProcessingStep(cidA, 0L), ProcessingStep(cidA, 5L)))
  }

  // ---------------------------------------------------------------------------
  // OrderEnforcer trait
  // ---------------------------------------------------------------------------

  "OrderEnforcer trait" should "be implementable as a custom subclass" in {
    val enf = new OrderEnforcer {
      override var isCompleted: Boolean = false
      override def canProceed(channelId: ChannelIdentity): Boolean = !isCompleted
    }
    assert(enf.canProceed(cidA))
    enf.isCompleted = true
    assert(!enf.canProceed(cidA))
  }

  // ---------------------------------------------------------------------------
  // ReplayOrderEnforcer
  // ---------------------------------------------------------------------------

  /** Stub that exposes a controllable `getStep`. */
  private class StubLogManager(
      handler: Either[
        org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.MainThreadDelegateMessage,
        WorkflowFIFOMessage
      ] => Unit
  ) extends EmptyReplayLogManagerImpl(handler) {
    private var step = 0L
    def setStep(s: Long): Unit = { step = s }
    override def getStep: Long = step
  }

  "ReplayOrderEnforcer" should "be completed immediately when the step queue is empty" in {
    val mgr = new StubLogManager(_ => ())
    val empty = mutable.Queue[ProcessingStep]()
    var fired = false
    val enf = new ReplayOrderEnforcer(mgr, empty, startStep = 0L, () => fired = true)
    assert(enf.isCompleted)
    assert(fired)
  }

  it should "skip log entries whose step is at or below startStep during construction" in {
    val mgr = new StubLogManager(_ => ())
    mgr.setStep(2L)
    val q = mutable.Queue[ProcessingStep](
      ProcessingStep(cidA, 0L),
      ProcessingStep(cidA, 1L),
      ProcessingStep(cidA, 2L),
      ProcessingStep(cidB, 3L)
    )
    val enf = new ReplayOrderEnforcer(mgr, q, startStep = 1L, () => ())
    // After construction, entries with step <= 1L are dropped, so currentChannelId
    // should be cidA (from step 1) — not cidB yet.
    assert(!enf.isCompleted)
    // step is currently 2 → forwardNext consumes step-2 entry on the next canProceed
    val proceeded = enf.canProceed(cidA)
    assert(proceeded, "should be allowed to proceed for cidA at step 2")
  }

  it should "advance to the next channel when canProceed is called at the next step" in {
    val mgr = new StubLogManager(_ => ())
    mgr.setStep(0L)
    val q = mutable.Queue[ProcessingStep](
      ProcessingStep(cidA, 0L),
      ProcessingStep(cidB, 1L)
    )
    val enf = new ReplayOrderEnforcer(mgr, q, startStep = -1L, () => ())
    // At step 0, the head matches and is consumed; currentChannelId becomes cidA.
    assert(enf.canProceed(cidA))
    assert(!enf.canProceed(cidB), "still on cidA until the next step is observed")

    mgr.setStep(1L)
    assert(!enf.canProceed(cidA), "step 1's channel is cidB, not cidA")
    // Once we've called canProceed at step 1, the head was consumed → cidB is current.
    assert(enf.isCompleted, "queue is exhausted, replay must mark completed")
  }

  it should "fire onComplete exactly once even if canProceed is called repeatedly past the end" in {
    val mgr = new StubLogManager(_ => ())
    var fired = 0
    val enf = new ReplayOrderEnforcer(
      mgr,
      mutable.Queue.empty[ProcessingStep],
      startStep = 0L,
      () => fired += 1
    )
    assert(fired == 1)
    enf.canProceed(cidA) // already completed → must not refire
    enf.canProceed(cidB)
    assert(fired == 1)
  }

  // ---------------------------------------------------------------------------
  // ReplayLogRecord shape
  // ---------------------------------------------------------------------------

  "ReplayLogRecord case-class subtypes" should "be Serializable and round-trip via case-class equality" in {
    val a: ReplayLogRecord = MessageContent(WorkflowFIFOMessage(cidA, 1L, DataFrame(Array.empty)))
    val b: ReplayLogRecord = ProcessingStep(cidA, 7L)
    val c: ReplayLogRecord = ReplayDestination(EmbeddedControlMessageIdentity("ecm-1"))
    val d: ReplayLogRecord = TerminateSignal

    Seq(a, b, c, d).foreach(r => assert(r.isInstanceOf[Serializable]))
    assert(b == ProcessingStep(cidA, 7L))
    assert(c == ReplayDestination(EmbeddedControlMessageIdentity("ecm-1")))
    assert(d eq TerminateSignal, "TerminateSignal is a case object — singleton")
  }
}
