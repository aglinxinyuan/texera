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

package org.apache.texera.amber.operator.map

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.scalatest.flatspec.AnyFlatSpec

class MapOpExecSpec extends AnyFlatSpec {

  private val schema: Schema =
    Schema().add(new Attribute("v", AttributeType.INTEGER))

  private def tuple(v: Int): Tuple =
    Tuple.builder(schema).add(new Attribute("v", AttributeType.INTEGER), Integer.valueOf(v)).build()

  private class TestMap extends MapOpExec

  "MapOpExec.processTuple" should "emit exactly one tuple per input by applying the configured mapFunc" in {
    val exec = new TestMap()
    exec.setMapFunc((t: Tuple) => tuple(t.getField[Int]("v") * 2))

    val out = exec.processTuple(tuple(3), 0).toList
    assert(out == List(tuple(6)))
  }

  it should "preserve identity when mapFunc returns the input tuple" in {
    val exec = new TestMap()
    exec.setMapFunc((t: Tuple) => t)

    val input = tuple(7)
    val out = exec.processTuple(input, 0).toList
    assert(out.size == 1)
    assert(out.head.asInstanceOf[Tuple] == input)
  }

  it should "always wrap the result in a single-element iterator" in {
    val exec = new TestMap()
    exec.setMapFunc((_: Tuple) => tuple(0))

    val it = exec.processTuple(tuple(99), 0)
    assert(it.hasNext)
    it.next()
    assert(!it.hasNext)
  }

  "MapOpExec.setMapFunc" should "overwrite a previously installed function" in {
    val exec = new TestMap()
    exec.setMapFunc((_: Tuple) => tuple(1))
    exec.setMapFunc((_: Tuple) => tuple(2))

    val out = exec.processTuple(tuple(0), 0).toList
    assert(out == List(tuple(2)))
  }

  it should "throw NullPointerException when mapFunc is invoked before setMapFunc" in {
    val exec = new TestMap()
    assertThrows[NullPointerException] {
      exec.processTuple(tuple(0), 0).toList
    }
  }
}
