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

package org.apache.texera.amber.operator.aggregate

import org.apache.texera.amber.core.tuple.AttributeType
import org.scalatest.flatspec.AnyFlatSpec

class AggregationOperationSpec extends AnyFlatSpec {

  private def operation(fn: AggregationFunction): AggregationOperation = {
    val op = new AggregationOperation()
    op.aggFunction = fn
    op.attribute = "src"
    op.resultAttribute = "out"
    op
  }

  "AggregationOperation.getAggregationAttribute" should "preserve the input type for SUM" in {
    val attr = operation(AggregationFunction.SUM).getAggregationAttribute(AttributeType.LONG)
    assert(attr.getName == "out")
    assert(attr.getType == AttributeType.LONG)
  }

  it should "always produce INTEGER for COUNT regardless of input type" in {
    val attr = operation(AggregationFunction.COUNT).getAggregationAttribute(AttributeType.STRING)
    assert(attr.getType == AttributeType.INTEGER)
  }

  it should "always produce DOUBLE for AVERAGE" in {
    val attr = operation(AggregationFunction.AVERAGE).getAggregationAttribute(AttributeType.LONG)
    assert(attr.getType == AttributeType.DOUBLE)
  }

  it should "preserve the input type for MIN" in {
    val attr =
      operation(AggregationFunction.MIN).getAggregationAttribute(AttributeType.TIMESTAMP)
    assert(attr.getType == AttributeType.TIMESTAMP)
  }

  it should "preserve the input type for MAX" in {
    val attr = operation(AggregationFunction.MAX).getAggregationAttribute(AttributeType.DOUBLE)
    assert(attr.getType == AttributeType.DOUBLE)
  }

  it should "always produce STRING for CONCAT" in {
    val attr = operation(AggregationFunction.CONCAT).getAggregationAttribute(AttributeType.STRING)
    assert(attr.getType == AttributeType.STRING)
  }

  it should "throw RuntimeException when aggFunction is null" in {
    val op = new AggregationOperation()
    op.attribute = "src"
    op.resultAttribute = "out"
    // aggFunction left null on purpose
    assertThrows[RuntimeException] {
      op.getAggregationAttribute(AttributeType.INTEGER)
    }
  }
}
