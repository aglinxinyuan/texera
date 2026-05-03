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

import org.apache.texera.amber.core.tuple.AttributeType
import org.scalatest.flatspec.AnyFlatSpec

class ResultSchemaSpec extends AnyFlatSpec {

  "ResultSchema.runtimeStatisticsSchema" should "list its columns in the declared order" in {
    val expectedNames = List(
      "operatorId",
      "time",
      "inputTupleCnt",
      "inputTupleSize",
      "outputTupleCnt",
      "outputTupleSize",
      "dataProcessingTime",
      "controlProcessingTime",
      "idleTime",
      "numWorkers",
      "status"
    )
    val actualNames =
      ResultSchema.runtimeStatisticsSchema.getAttributes.map(_.getName)
    assert(actualNames == expectedNames)
  }

  it should "pin every runtime-statistics column to its expected type" in {
    val schema = ResultSchema.runtimeStatisticsSchema
    // Downstream readers deserialize positionally and cast each slot, so each
    // column type matters. Pin all of them, not just a sample.
    assert(schema.getAttribute("operatorId").getType == AttributeType.STRING)
    assert(schema.getAttribute("time").getType == AttributeType.TIMESTAMP)
    assert(schema.getAttribute("inputTupleCnt").getType == AttributeType.LONG)
    assert(schema.getAttribute("inputTupleSize").getType == AttributeType.LONG)
    assert(schema.getAttribute("outputTupleCnt").getType == AttributeType.LONG)
    assert(schema.getAttribute("outputTupleSize").getType == AttributeType.LONG)
    assert(schema.getAttribute("dataProcessingTime").getType == AttributeType.LONG)
    assert(schema.getAttribute("controlProcessingTime").getType == AttributeType.LONG)
    assert(schema.getAttribute("idleTime").getType == AttributeType.LONG)
    assert(schema.getAttribute("numWorkers").getType == AttributeType.INTEGER)
    assert(schema.getAttribute("status").getType == AttributeType.INTEGER)
  }

  "ResultSchema.consoleMessagesSchema" should "have a single STRING `message` column" in {
    val schema = ResultSchema.consoleMessagesSchema
    val attrs = schema.getAttributes
    assert(attrs.size == 1)
    assert(attrs.head.getName == "message")
    assert(attrs.head.getType == AttributeType.STRING)
  }
}
