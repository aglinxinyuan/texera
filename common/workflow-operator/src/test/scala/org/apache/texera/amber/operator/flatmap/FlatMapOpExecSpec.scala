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

package org.apache.texera.amber.operator.flatmap

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple, TupleLike}
import org.scalatest.flatspec.AnyFlatSpec

class FlatMapOpExecSpec extends AnyFlatSpec {

  private val schema: Schema =
    Schema().add(new Attribute("v", AttributeType.INTEGER))

  private def tuple(v: Int): Tuple =
    Tuple.builder(schema).add(new Attribute("v", AttributeType.INTEGER), Integer.valueOf(v)).build()

  "FlatMapOpExec.processTuple" should "delegate to the configured flatMapFunc" in {
    val exec = new FlatMapOpExec()
    exec.setFlatMapFunc(t => Iterator(t, t))

    val out = exec.processTuple(tuple(1), 0).toList
    assert(out.size == 2)
    assert(out.forall(_.asInstanceOf[Tuple] == tuple(1)))
  }

  it should "emit nothing when the flatMapFunc returns an empty iterator" in {
    val exec = new FlatMapOpExec()
    exec.setFlatMapFunc(_ => Iterator.empty)

    assert(exec.processTuple(tuple(1), 0).isEmpty)
  }

  it should "preserve the order of tuples emitted by the flatMapFunc" in {
    val exec = new FlatMapOpExec()
    exec.setFlatMapFunc(t => Iterator(tuple(99), t, tuple(0)))

    val out = exec.processTuple(tuple(7), 0).toList.map(_.asInstanceOf[Tuple])
    assert(out == List(tuple(99), tuple(7), tuple(0)))
  }

  "FlatMapOpExec.setFlatMapFunc" should "overwrite a previously installed function" in {
    val exec = new FlatMapOpExec()
    exec.setFlatMapFunc(_ => Iterator.empty)
    exec.setFlatMapFunc((t: Tuple) => Iterator[TupleLike](t))

    val out = exec.processTuple(tuple(5), 0).toList
    assert(out == List(tuple(5)))
  }

  it should "throw NullPointerException when processTuple is invoked before setFlatMapFunc" in {
    val exec = new FlatMapOpExec()
    assertThrows[NullPointerException] {
      // Iterator construction calls flatMapFunc(tuple) eagerly, so the NPE
      // surfaces here even though processTuple itself returns an iterator.
      exec.processTuple(tuple(1), 0)
    }
  }
}
