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

package org.apache.texera.amber.core.workflow

import org.scalatest.flatspec.AnyFlatSpec

class PartitionInfoSpec extends AnyFlatSpec {

  "PartitionInfo.satisfies" should "hold reflexively for equal partitions" in {
    assert(HashPartition(List("a")).satisfies(HashPartition(List("a"))))
    assert(SinglePartition().satisfies(SinglePartition()))
    assert(OneToOnePartition().satisfies(OneToOnePartition()))
    assert(BroadcastPartition().satisfies(BroadcastPartition()))
  }

  it should "hold for any partition against UnknownPartition" in {
    assert(HashPartition(List("a")).satisfies(UnknownPartition()))
    assert(SinglePartition().satisfies(UnknownPartition()))
    assert(BroadcastPartition().satisfies(UnknownPartition()))
    assert(UnknownPartition().satisfies(UnknownPartition()))
  }

  it should "fail across distinct partition classes" in {
    assert(!HashPartition(List("a")).satisfies(SinglePartition()))
    assert(!SinglePartition().satisfies(BroadcastPartition()))
  }

  it should "fail for HashPartition with different attribute lists" in {
    assert(!HashPartition(List("a")).satisfies(HashPartition(List("b"))))
  }

  "PartitionInfo.merge" should "preserve the partition when merged with itself" in {
    val hash = HashPartition(List("a"))
    assert(hash.merge(hash) == hash)
    assert(SinglePartition().merge(SinglePartition()) == SinglePartition())
  }

  it should "fall back to UnknownPartition when merging different partitions" in {
    assert(HashPartition(List("a")).merge(SinglePartition()) == UnknownPartition())
    assert(SinglePartition().merge(BroadcastPartition()) == UnknownPartition())
  }

  it should "always return UnknownPartition for RangePartition merges" in {
    val range = RangePartition(List("a"), 0, 10).asInstanceOf[RangePartition]
    assert(range.merge(range) == UnknownPartition())
  }

  "RangePartition.apply" should "return an UnknownPartition when no range attributes are provided" in {
    assert(RangePartition(List.empty, 0L, 10L) == UnknownPartition())
  }

  it should "return a RangePartition when at least one range attribute is provided" in {
    val result = RangePartition(List("a"), 0L, 10L)
    assert(result.isInstanceOf[RangePartition])
    val rp = result.asInstanceOf[RangePartition]
    assert(rp.rangeAttributeNames == List("a"))
    assert(rp.rangeMin == 0L)
    assert(rp.rangeMax == 10L)
  }
}
