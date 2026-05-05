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

package org.apache.texera.amber.util.serde

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.texera.amber.core.virtualidentity.{OperatorIdentity, PhysicalOpIdentity}
import org.apache.texera.amber.core.workflow.{GlobalPortIdentity, PortIdentity}
import org.apache.texera.amber.util.serde.GlobalPortIdentitySerde.SerdeOps
import org.scalatest.flatspec.AnyFlatSpec

class PortIdentitySerdeSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // GlobalPortIdentitySerde
  // ---------------------------------------------------------------------------

  private def globalPort(
      logical: String = "op-A",
      layer: String = "main",
      portIdValue: Int = 0,
      internal: Boolean = false,
      input: Boolean = true
  ): GlobalPortIdentity =
    GlobalPortIdentity(
      opId = PhysicalOpIdentity(OperatorIdentity(logical), layer),
      portId = PortIdentity(id = portIdValue, internal = internal),
      input = input
    )

  "GlobalPortIdentitySerde" should "round-trip a default GlobalPortIdentity through serializeAsString → deserializeFromString" in {
    val original = globalPort()
    val restored = GlobalPortIdentitySerde.deserializeFromString(original.serializeAsString)
    assert(restored == original)
  }

  it should "preserve all five fields independently across the round-trip" in {
    // Vary each field individually so a regression that swapped two fields
    // (e.g., isInput / isInternal) would surface here, not as a general
    // round-trip failure.
    val cases = Seq(
      globalPort(logical = "op-A"),
      globalPort(logical = "op-Z"),
      globalPort(layer = "main"),
      globalPort(layer = "extra-layer"),
      globalPort(portIdValue = 0),
      globalPort(portIdValue = 7),
      globalPort(internal = false),
      globalPort(internal = true),
      globalPort(input = true),
      globalPort(input = false)
    )
    cases.foreach { p =>
      val s = p.serializeAsString
      val restored = GlobalPortIdentitySerde.deserializeFromString(s)
      assert(restored == p, s"round-trip mismatch for $p (serialized: $s)")
    }
  }

  it should "produce the documented format for default and non-default values" in {
    // Pin the exact format. If this changes, callers reading existing
    // VFS URIs from disk will break — locking it down forces a deliberate
    // migration story.
    assert(
      globalPort().serializeAsString ==
        "(logicalOpId=op-A,layerName=main,portId=0,isInternal=false,isInput=true)"
    )
    assert(
      globalPort(
        logical = "op-Z",
        layer = "extra-layer",
        portIdValue = 7,
        internal = true,
        input = false
      ).serializeAsString ==
        "(logicalOpId=op-Z,layerName=extra-layer,portId=7,isInternal=true,isInput=false)"
    )
  }

  it should "round-trip identifiers containing dashes and dots (regex non-comma matcher)" in {
    // The deserialization regex uses `[^,]+` for the field body, so any
    // non-comma character is fair game. Cover the realistic counter-
    // examples (dashes, dots) since logical op ids and layer names use
    // both; if the regex were ever tightened to alphanumerics only, this
    // would fail on purpose.
    val p = globalPort(logical = "my.op-with-dashes.v2", layer = "main-1")
    assert(GlobalPortIdentitySerde.deserializeFromString(p.serializeAsString) == p)
  }

  it should "round-trip a negative port id" in {
    // PortIdentity.id is a plain Int; negatives are technically permitted
    // by the type. Pin the round-trip so a future tightening of the
    // numeric regex (e.g. to `\\d+`) breaks this on purpose.
    val p = globalPort(portIdValue = -1)
    assert(GlobalPortIdentitySerde.deserializeFromString(p.serializeAsString) == p)
  }

  it should "throw IllegalArgumentException when the input has the wrong field order" in {
    // The regex pins the documented field order; a swapped order should
    // not silently parse with confused values.
    val swapped = "(layerName=main,logicalOpId=op-A,portId=0,isInternal=false,isInput=true)"
    intercept[IllegalArgumentException] {
      GlobalPortIdentitySerde.deserializeFromString(swapped)
    }
  }

  it should "throw IllegalArgumentException when the input has trailing content past the closing paren" in {
    val withTrailing =
      "(logicalOpId=op-A,layerName=main,portId=0,isInternal=false,isInput=true) extra"
    intercept[IllegalArgumentException] {
      GlobalPortIdentitySerde.deserializeFromString(withTrailing)
    }
  }

  it should "throw IllegalArgumentException when a field body is empty" in {
    // `[^,]+` requires at least one character, so an empty layerName
    // (`,layerName=,`) must fail to match.
    val emptyLayer = "(logicalOpId=op-A,layerName=,portId=0,isInternal=false,isInput=true)"
    intercept[IllegalArgumentException] {
      GlobalPortIdentitySerde.deserializeFromString(emptyLayer)
    }
  }

  it should "throw IllegalArgumentException on a completely malformed string" in {
    val ex = intercept[IllegalArgumentException] {
      GlobalPortIdentitySerde.deserializeFromString("not even close")
    }
    assert(ex.getMessage.contains("not even close"))
  }

  it should "throw IllegalArgumentException when a required field is missing" in {
    // Drop isInput.
    val malformed = "(logicalOpId=op-A,layerName=main,portId=0,isInternal=false)"
    intercept[IllegalArgumentException] {
      GlobalPortIdentitySerde.deserializeFromString(malformed)
    }
  }

  it should "throw NumberFormatException when portId is non-numeric" in {
    // The regex matches (`[^,]+`) but `.toInt` fails. NumberFormatException
    // extends IllegalArgumentException; assert the more specific type so a
    // regression that swallowed/rewrapped it is visible.
    val malformed = "(logicalOpId=op-A,layerName=main,portId=NaN,isInternal=false,isInput=true)"
    intercept[NumberFormatException] {
      GlobalPortIdentitySerde.deserializeFromString(malformed)
    }
  }

  it should "throw IllegalArgumentException when a boolean field is non-boolean" in {
    // `String.toBoolean` is strict: only \"true\" / \"false\" (case-insensitive)
    // pass; anything else throws IllegalArgumentException.
    val malformed = "(logicalOpId=op-A,layerName=main,portId=0,isInternal=maybe,isInput=true)"
    intercept[IllegalArgumentException] {
      GlobalPortIdentitySerde.deserializeFromString(malformed)
    }
  }

  it should "produce a string with no underscore (compatibility with VFS URI parsing)" in {
    // The custom format exists specifically to avoid the underscore separator
    // used by `PortIdentityKeySerializer.portIdToString`, which would clash
    // with the VFS URI parser. Pin the no-underscore invariant for every
    // field combination — including a layerName containing dashes, which
    // is the realistic counter-example most likely to slip in.
    val s = globalPort(logical = "my-op", layer = "extra-layer", internal = true).serializeAsString
    assert(!s.contains("_"), s"serialized form must be underscore-free: $s")
  }

  // ---------------------------------------------------------------------------
  // PortIdentityKeySerializer.portIdToString (companion, not the Jackson class)
  // ---------------------------------------------------------------------------

  "PortIdentityKeySerializer.portIdToString" should "format a PortIdentity as `id_internal`" in {
    assert(PortIdentityKeySerializer.portIdToString(PortIdentity(0, internal = false)) == "0_false")
    assert(PortIdentityKeySerializer.portIdToString(PortIdentity(7, internal = true)) == "7_true")
  }

  // ---------------------------------------------------------------------------
  // PortIdentityKeySerializer + PortIdentityKeyDeserializer (Jackson wiring)
  // ---------------------------------------------------------------------------
  //
  // These tests use a real ObjectMapper with the serde pair registered as a
  // module — same wiring JSONUtils.objectMapper performs in production —
  // so a regression in either direction surfaces here.

  private def newMapper(): ObjectMapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    val mod = new SimpleModule()
    mod.addKeySerializer(classOf[PortIdentity], new PortIdentityKeySerializer())
    mod.addKeyDeserializer(classOf[PortIdentity], new PortIdentityKeyDeserializer())
    m.registerModule(mod)
    m
  }

  "PortIdentity Jackson key (de)serialization" should "round-trip a Map[PortIdentity, String] via the registered module" in {
    val mapper = newMapper()
    val original = Map(
      PortIdentity(0, internal = false) -> "a",
      PortIdentity(1, internal = true) -> "b"
    )
    val json = mapper.writeValueAsString(original)
    // Verify the JSON keys match the documented `id_internal` format.
    assert(json.contains("\"0_false\""))
    assert(json.contains("\"1_true\""))
    val tref = mapper.getTypeFactory
      .constructMapType(classOf[java.util.HashMap[_, _]], classOf[PortIdentity], classOf[String])
    val restored: java.util.Map[PortIdentity, String] = mapper.readValue(json, tref)
    import scala.jdk.CollectionConverters._
    assert(restored.asScala.toMap == original)
  }

  it should "round-trip an empty Map[PortIdentity, V] without invoking the (de)serializer" in {
    val mapper = newMapper()
    val original = Map.empty[PortIdentity, String]
    val json = mapper.writeValueAsString(original)
    val tref = mapper.getTypeFactory
      .constructMapType(classOf[java.util.HashMap[_, _]], classOf[PortIdentity], classOf[String])
    val restored: java.util.Map[PortIdentity, String] = mapper.readValue(json, tref)
    assert(restored.isEmpty)
  }

  "PortIdentityKeyDeserializer.deserializeKey" should "throw NumberFormatException for a non-integer id" in {
    val d = new PortIdentityKeyDeserializer
    intercept[NumberFormatException] {
      d.deserializeKey("notAnInt_false", null)
    }
  }

  it should "throw IllegalArgumentException for a non-boolean internal flag" in {
    val d = new PortIdentityKeyDeserializer
    intercept[IllegalArgumentException] {
      d.deserializeKey("0_notABool", null)
    }
  }

  it should "throw NumberFormatException when the underscore separator is missing and the whole string is non-numeric" in {
    // `key.split("_")` on a separator-less non-numeric string yields a
    // single-element array, and `parts(0).toInt` fires first → NFE.
    val d = new PortIdentityKeyDeserializer
    intercept[NumberFormatException] {
      d.deserializeKey("missingSeparator", null)
    }
  }

  it should "throw ArrayIndexOutOfBoundsException when only the id is provided (no `_internal` suffix)" in {
    // Different separator-missing path: `\"5\".split(\"_\")` yields
    // [\"5\"], parts(0).toInt = 5 succeeds, then parts(1) reads past the
    // end. Pin this failure mode explicitly so a future safer parser
    // breaks the spec on purpose (and the safer error type is chosen
    // consciously).
    val d = new PortIdentityKeyDeserializer
    intercept[ArrayIndexOutOfBoundsException] {
      d.deserializeKey("5", null)
    }
  }
}
