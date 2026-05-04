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

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.scalatest.flatspec.AnyFlatSpec

class AggregationOperationSpec extends AnyFlatSpec {

  // --- helpers ---------------------------------------------------------------

  private def schemaWith(name: String, t: AttributeType): Schema =
    new Schema(new Attribute(name, t))

  private def tupleOf(name: String, t: AttributeType, value: AnyRef): Tuple =
    Tuple.builder(schemaWith(name, t)).add(new Attribute(name, t), value).build()

  private def op(
      func: AggregationFunction,
      attribute: String = "v",
      resultAttribute: String = "r"
  ): AggregationOperation = {
    val o = new AggregationOperation()
    o.aggFunction = func
    o.attribute = attribute
    o.resultAttribute = resultAttribute
    o
  }

  // --- getAggregationAttribute -----------------------------------------------

  "AggregationOperation.getAggregationAttribute" should "preserve the input type for SUM" in {
    val attr = op(AggregationFunction.SUM).getAggregationAttribute(AttributeType.LONG)
    assert(attr.getName == "r")
    assert(attr.getType == AttributeType.LONG)
  }

  it should "produce INTEGER for COUNT regardless of input type" in {
    val attr = op(AggregationFunction.COUNT).getAggregationAttribute(AttributeType.STRING)
    assert(attr.getType == AttributeType.INTEGER)
  }

  it should "produce DOUBLE for AVERAGE regardless of input type" in {
    val attr = op(AggregationFunction.AVERAGE).getAggregationAttribute(AttributeType.LONG)
    assert(attr.getType == AttributeType.DOUBLE)
  }

  it should "preserve the input type for MIN and MAX" in {
    assert(
      op(AggregationFunction.MIN).getAggregationAttribute(AttributeType.INTEGER).getType ==
        AttributeType.INTEGER
    )
    assert(
      op(AggregationFunction.MAX).getAggregationAttribute(AttributeType.TIMESTAMP).getType ==
        AttributeType.TIMESTAMP
    )
  }

  it should "produce STRING for CONCAT" in {
    assert(
      op(AggregationFunction.CONCAT).getAggregationAttribute(AttributeType.STRING).getType ==
        AttributeType.STRING
    )
  }

  it should "throw RuntimeException when aggFunction is null" in {
    val ex = intercept[RuntimeException] {
      op(null).getAggregationAttribute(AttributeType.INTEGER)
    }
    assert(ex.getMessage.contains("Unknown aggregation function"))
  }

  // --- getAggFunc: type validation -------------------------------------------

  "AggregationOperation.getAggFunc" should "throw for non-numeric types on SUM" in {
    val ex = intercept[UnsupportedOperationException] {
      op(AggregationFunction.SUM).getAggFunc(AttributeType.STRING)
    }
    assert(ex.getMessage.contains("Unsupported attribute type for sum"))
  }

  it should "throw for non-numeric types on MIN and MAX" in {
    intercept[UnsupportedOperationException] {
      op(AggregationFunction.MIN).getAggFunc(AttributeType.STRING)
    }
    intercept[UnsupportedOperationException] {
      op(AggregationFunction.MAX).getAggFunc(AttributeType.BOOLEAN)
    }
  }

  it should "throw UnsupportedOperationException when aggFunction is null" in {
    val ex = intercept[UnsupportedOperationException] {
      op(null).getAggFunc(AttributeType.INTEGER)
    }
    assert(ex.getMessage.contains("Unknown aggregation function"))
  }

  // --- getAggFunc: SUM behavior ----------------------------------------------

  "SUM aggregation" should "init at the type's zero, accumulate values, and merge partial sums" in {
    val agg = op(AggregationFunction.SUM).getAggFunc(AttributeType.INTEGER)
    val zero = agg.init().asInstanceOf[Integer]
    assert(zero == 0)
    val t1 = tupleOf("v", AttributeType.INTEGER, Int.box(3))
    val t2 = tupleOf("v", AttributeType.INTEGER, Int.box(5))
    val partial = agg.iterate(agg.iterate(zero, t1), t2)
    assert(partial.asInstanceOf[Integer] == 8)
    val merged = agg.merge(partial, partial)
    assert(merged.asInstanceOf[Integer] == 16)
    assert(agg.finalAgg(merged).asInstanceOf[Integer] == 16)
  }

  // --- getAggFunc: COUNT behavior --------------------------------------------

  "COUNT aggregation" should "treat a null `attribute` as count-all (one per tuple)" in {
    val agg = op(AggregationFunction.COUNT, attribute = null).getAggFunc(AttributeType.INTEGER)
    val t = tupleOf("v", AttributeType.INTEGER, null)
    val out = agg.iterate(agg.iterate(agg.init(), t), t).asInstanceOf[Integer]
    assert(out == 2, "with attribute=null, every tuple — even null-valued — should count")
  }

  it should "count only non-null values when `attribute` is set" in {
    val agg = op(AggregationFunction.COUNT, attribute = "v").getAggFunc(AttributeType.INTEGER)
    val nonNull = tupleOf("v", AttributeType.INTEGER, Int.box(7))
    val nullVal = tupleOf("v", AttributeType.INTEGER, null)
    val out = agg
      .iterate(agg.iterate(agg.iterate(agg.init(), nonNull), nullVal), nonNull)
      .asInstanceOf[Integer]
    assert(out == 2, "two non-null tuples + one null → count == 2")
  }

  // --- getAggFunc: AVERAGE behavior ------------------------------------------

  "AVERAGE aggregation" should "init at (0,0), accumulate sum+count, and yield sum/count" in {
    // averageAgg() returns DistributedAggregation[AveragePartialObj] but is
    // type-erased to Object via getAggFunc, so we cast back here.
    val agg = op(AggregationFunction.AVERAGE).getAggFunc(AttributeType.DOUBLE)
    val zero = agg.init().asInstanceOf[AveragePartialObj]
    assert(zero == AveragePartialObj(0, 0))

    val t1 = tupleOf("v", AttributeType.DOUBLE, java.lang.Double.valueOf(2.0))
    val t2 = tupleOf("v", AttributeType.DOUBLE, java.lang.Double.valueOf(4.0))
    val acc = agg
      .iterate(agg.iterate(zero, t1), t2)
      .asInstanceOf[AveragePartialObj]
    assert(acc == AveragePartialObj(6.0, 2))
    val finalVal = agg.finalAgg(acc).asInstanceOf[java.lang.Double]
    assert(finalVal == 3.0)
  }

  it should "yield null when no non-null values were aggregated" in {
    val agg = op(AggregationFunction.AVERAGE).getAggFunc(AttributeType.DOUBLE)
    val zero = agg.init()
    val finalVal = agg.finalAgg(zero)
    assert(finalVal == null)
  }

  // --- getAggFunc: CONCAT behavior -------------------------------------------

  "CONCAT aggregation" should "concatenate non-empty values with commas and skip null gracefully" in {
    val agg = op(AggregationFunction.CONCAT).getAggFunc(AttributeType.STRING)
    assert(agg.init() == "")
    val t1 = tupleOf("v", AttributeType.STRING, "a")
    val t2 = tupleOf("v", AttributeType.STRING, "b")
    val tNull = tupleOf("v", AttributeType.STRING, null)
    val out =
      agg.iterate(agg.iterate(agg.iterate(agg.init(), t1), tNull), t2)
    assert(out == "a,,b", "null values are emitted as empty between commas")
  }

  it should "merge two non-empty partial strings with a comma" in {
    val agg = op(AggregationFunction.CONCAT).getAggFunc(AttributeType.STRING)
    assert(agg.merge("foo", "bar") == "foo,bar")
    assert(agg.merge("", "bar") == "bar")
    assert(agg.merge("foo", "") == "foo")
    assert(agg.merge("", "") == "")
  }

  // --- getFinal --------------------------------------------------------------

  "AggregationOperation.getFinal" should "rewrite COUNT into a SUM over the result column" in {
    val original = op(AggregationFunction.COUNT, attribute = "src", resultAttribute = "cnt")
    val finalOp = original.getFinal
    assert(finalOp.aggFunction == AggregationFunction.SUM)
    // both attribute and resultAttribute should now point at the partial column
    assert(finalOp.attribute == "cnt")
    assert(finalOp.resultAttribute == "cnt")
    // the original is not mutated
    assert(original.aggFunction == AggregationFunction.COUNT)
    assert(original.attribute == "src")
  }

  it should "leave non-COUNT aggregations' aggFunction unchanged but rebind the attribute" in {
    Seq(
      AggregationFunction.SUM,
      AggregationFunction.AVERAGE,
      AggregationFunction.MIN,
      AggregationFunction.MAX,
      AggregationFunction.CONCAT
    ).foreach { f =>
      val original = op(f, attribute = "src", resultAttribute = "r")
      val finalOp = original.getFinal
      assert(finalOp.aggFunction == f, s"aggFunction must be unchanged for $f")
      assert(finalOp.attribute == "r")
      assert(finalOp.resultAttribute == "r")
    }
  }

  // --- AveragePartialObj -----------------------------------------------------

  "AveragePartialObj" should "expose its sum and count fields and support value equality" in {
    val a = AveragePartialObj(10.0, 4)
    val b = AveragePartialObj(10.0, 4)
    assert(a.sum == 10.0)
    assert(a.count == 4)
    assert(a == b)
  }
}
