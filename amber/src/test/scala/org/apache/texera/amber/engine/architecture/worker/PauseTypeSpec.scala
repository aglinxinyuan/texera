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

package org.apache.texera.amber.engine.architecture.worker

import org.apache.texera.amber.core.virtualidentity.EmbeddedControlMessageIdentity
import org.scalatest.flatspec.AnyFlatSpec

class PauseTypeSpec extends AnyFlatSpec {

  // --- singletons ------------------------------------------------------------

  "PauseType singletons" should "all extend the sealed trait PauseType" in {
    val all: List[PauseType] = List(UserPause, BackpressurePause, OperatorLogicPause)
    all.foreach(p => assert(p.isInstanceOf[PauseType]))
  }

  it should "compare equal to themselves and unequal to each other" in {
    // Widen to PauseType so the compiler doesn't reduce inter-singleton
    // comparisons to constant `false` at compile time.
    val u: PauseType = UserPause
    val b: PauseType = BackpressurePause
    val o: PauseType = OperatorLogicPause
    assert(u == UserPause)
    assert(b == BackpressurePause)
    assert(o == OperatorLogicPause)
    assert(u != b)
    assert(u != o)
    assert(b != o)
  }

  it should "be the same singleton instance per access (object identity)" in {
    assert((UserPause: AnyRef) eq UserPause)
    assert((BackpressurePause: AnyRef) eq BackpressurePause)
    assert((OperatorLogicPause: AnyRef) eq OperatorLogicPause)
  }

  // --- ECMPause --------------------------------------------------------------

  "ECMPause" should "carry the EmbeddedControlMessageIdentity it was constructed with" in {
    val id = EmbeddedControlMessageIdentity("ckpt-1")
    val p = ECMPause(id)
    assert(p.id == id)
  }

  it should "support case-class value equality and hashCode (same id → equal)" in {
    val a = ECMPause(EmbeddedControlMessageIdentity("ckpt-1"))
    val b = ECMPause(EmbeddedControlMessageIdentity("ckpt-1"))
    val c = ECMPause(EmbeddedControlMessageIdentity("ckpt-2"))
    assert(a == b)
    assert(a.hashCode == b.hashCode)
    assert(a != c)
  }

  it should "be a PauseType but not equal to any of the singleton PauseTypes" in {
    val p: PauseType = ECMPause(EmbeddedControlMessageIdentity("ckpt"))
    assert(p.isInstanceOf[PauseType])
    assert(p != UserPause)
    assert(p != BackpressurePause)
    assert(p != OperatorLogicPause)
  }

  // --- pattern matching ------------------------------------------------------

  "PauseType" should "support exhaustive pattern matching that distinguishes each subtype" in {
    def label(p: PauseType): String =
      p match {
        case UserPause          => "user"
        case BackpressurePause  => "backpressure"
        case OperatorLogicPause => "operator-logic"
        case ECMPause(_)        => "ecm"
      }
    assert(label(UserPause) == "user")
    assert(label(BackpressurePause) == "backpressure")
    assert(label(OperatorLogicPause) == "operator-logic")
    assert(label(ECMPause(EmbeddedControlMessageIdentity("x"))) == "ecm")
  }
}
