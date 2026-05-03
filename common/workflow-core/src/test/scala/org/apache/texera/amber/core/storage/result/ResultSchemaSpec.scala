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

package org.apache.texera.amber.core.storage.result

import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.scalatest.flatspec.AnyFlatSpec

class ResultSchemaSpec extends AnyFlatSpec {

  // The expected (name, type) layout of runtimeStatisticsSchema, in the order
  // production code declares it. Multiple tests below depend on this list, so
  // it lives here as a single source of truth for the spec.
  private val runtimeStatsLayout: List[(String, AttributeType)] = List(
    "operatorId" -> AttributeType.STRING,
    "time" -> AttributeType.TIMESTAMP,
    "inputTupleCnt" -> AttributeType.LONG,
    "inputTupleSize" -> AttributeType.LONG,
    "outputTupleCnt" -> AttributeType.LONG,
    "outputTupleSize" -> AttributeType.LONG,
    "dataProcessingTime" -> AttributeType.LONG,
    "controlProcessingTime" -> AttributeType.LONG,
    "idleTime" -> AttributeType.LONG,
    "numWorkers" -> AttributeType.INTEGER,
    "status" -> AttributeType.INTEGER
  )

  "ResultSchema.runtimeStatisticsSchema" should "list its columns in the declared order" in {
    val actualNames =
      ResultSchema.runtimeStatisticsSchema.getAttributes.map(_.getName)
    assert(actualNames == runtimeStatsLayout.map(_._1))
  }

  it should "pin every runtime-statistics column to its expected type" in {
    val schema = ResultSchema.runtimeStatisticsSchema
    // Downstream readers deserialize positionally and cast each slot, so each
    // column type matters. Pin all of them, not just a sample.
    runtimeStatsLayout.foreach {
      case (name, expectedType) =>
        assert(
          schema.getAttribute(name).getType == expectedType,
          s"$name expected $expectedType, got ${schema.getAttribute(name).getType}"
        )
    }
  }

  it should "expose a stable name → index mapping for positional readers" in {
    val schema = ResultSchema.runtimeStatisticsSchema
    runtimeStatsLayout.zipWithIndex.foreach {
      case ((name, _), expectedIndex) =>
        assert(
          schema.getIndex(name) == expectedIndex,
          s"$name expected index $expectedIndex, got ${schema.getIndex(name)}"
        )
    }
  }

  it should "throw on lookup of an unknown attribute name" in {
    val schema = ResultSchema.runtimeStatisticsSchema
    val ex = intercept[RuntimeException] {
      schema.getAttribute("not-a-runtime-stats-column")
    }
    assert(ex.getMessage.contains("not-a-runtime-stats-column"))
    assert(!schema.containsAttribute("not-a-runtime-stats-column"))
  }

  it should "have unique column names" in {
    val names = ResultSchema.runtimeStatisticsSchema.getAttributes.map(_.getName)
    assert(names.distinct == names, s"duplicate column names: $names")
  }

  it should "round-trip via toRawSchema → fromRawSchema with stable names and types" in {
    // The cross-language serialization contract that downstream Python /
    // external consumers actually depend on. If a column type drifts so
    // its `AttributeType.name()` no longer round-trips, this fails.
    val original = ResultSchema.runtimeStatisticsSchema
    val raw = original.toRawSchema
    val restored = Schema.fromRawSchema(raw)
    // Names + types must be preserved by the round-trip; column ORDER is not
    // contractually guaranteed by `toRawSchema`'s `Map` return type, so we
    // compare via the (name, type) set instead of full equality.
    assert(
      restored.getAttributes.map(a => a.getName -> a.getType).toSet ==
        original.getAttributes.map(a => a.getName -> a.getType).toSet
    )
    assert(raw.keySet == runtimeStatsLayout.map(_._1).toSet)
    assert(raw == runtimeStatsLayout.map { case (n, t) => n -> t.name() }.toMap)
  }

  it should "be a singleton val (same instance per access)" in {
    // Pin the assumption that consumers can hold references without paying
    // for repeated rebuilds, and that two reads produce structurally-equal
    // schemas at minimum.
    assert(ResultSchema.runtimeStatisticsSchema eq ResultSchema.runtimeStatisticsSchema)
  }

  "ResultSchema.consoleMessagesSchema" should "have a single STRING `message` column" in {
    val schema = ResultSchema.consoleMessagesSchema
    val attrs = schema.getAttributes
    assert(attrs.size == 1)
    assert(attrs.head.getName == "message")
    assert(attrs.head.getType == AttributeType.STRING)
  }

  it should "place `message` at index 0" in {
    assert(ResultSchema.consoleMessagesSchema.getIndex("message") == 0)
  }

  it should "throw on lookup of an unknown attribute name" in {
    val ex = intercept[RuntimeException] {
      ResultSchema.consoleMessagesSchema.getAttribute("not-a-console-column")
    }
    assert(ex.getMessage.contains("not-a-console-column"))
  }

  it should "round-trip to a {message: STRING} raw schema" in {
    val raw = ResultSchema.consoleMessagesSchema.toRawSchema
    assert(raw == Map("message" -> AttributeType.STRING.name()))
  }

  it should "be a singleton val (same instance per access)" in {
    assert(ResultSchema.consoleMessagesSchema eq ResultSchema.consoleMessagesSchema)
  }
}
