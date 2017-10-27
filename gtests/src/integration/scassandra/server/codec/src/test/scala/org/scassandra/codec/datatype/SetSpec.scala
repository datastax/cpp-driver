/*
 * Copyright (C) 2016 Christopher Batey and Dogan Narinc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.scassandra.codec.datatype

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scassandra.codec.ProtocolVersion
import scodec.Attempt.Failure
import scodec.bits.ByteVector

class SetSpec extends DataTypeSpec with TableDrivenPropertyChecks {
  val protocolVersions = Table("Protocol Version", ProtocolVersion.versions:_*)

  forAll(protocolVersions) { (protocolVersion: ProtocolVersion) =>
    implicit val protocol: ProtocolVersion = protocolVersion
    val codec = DataType.Set(DataType.Varchar).codec
    val nestedCodec = DataType.Set(DataType.Set(DataType.Varchar)).codec

    val expectedBytes = if(protocol.version < 3) {
      ByteVector(
        0, 3, // number of elements
        0, 3, 111, 110, 101, // one
        0, 3, 116, 119, 111, // two
        0, 5, 116, 104, 114, 101, 101 // three
      )
    } else {
      ByteVector(
        0, 0, 0, 3, // number of elements
        0, 0, 0, 3, 111, 110, 101, // one
        0, 0, 0, 3, 116, 119, 111, // two
        0, 0, 0, 5, 116, 104, 114, 101, 101 // three
      )
    }

    "codec with " + protocol must "encode set<varchar> from a Set" in {
      codec.encode(Set("one", "two", "three")).require.bytes shouldEqual expectedBytes
    }

    it must "encode empty Set properly" in {
      val zeros = if (protocol.version < 3) {
        ByteVector(0, 0)
      } else {
        ByteVector(0, 0, 0, 0)
      }

      codec.encode(Set()).require.bytes shouldEqual zeros
    }

    val expectedNestedBytes = if(protocol.version < 3) {
      ByteVector(
        0, 2, // number of elements
        0, 19, // byte length of first list
        0, 3, // number of elements
        0, 3, 111, 110, 101, // one
        0, 3, 116, 119, 111, // two
        0, 5, 116, 104, 114, 101, 101, // three
        0, 19, // byte length of second list
        0, 3, // elements in list
        0, 4, 102, 111, 117, 114, // four
        0, 4, 102, 105, 118, 101, // five
        0, 3, 115, 105, 120 // six
      )
    } else {
      ByteVector(
        0, 0, 0, 2, // number of elements
        0, 0, 0, 27, // byte length of first list
        0, 0, 0, 3, // number of elements
        0, 0, 0, 3, 111, 110, 101, // one
        0, 0, 0, 3, 116, 119, 111, // two
        0, 0, 0, 5, 116, 104, 114, 101, 101, // three
        0, 0, 0, 27, // byte length of second list
        0, 0, 0, 3, // elements in list
        0, 0, 0, 4, 102, 111, 117, 114, // four
        0, 0, 0, 4, 102, 105, 118, 101, // five
        0, 0, 0, 3, 115, 105, 120 // six
      )
    }

    it must "encode set<set<varchar>> from a Set" in {
      nestedCodec.encode(Set(Set("one", "two", "three"), Set("four", "five", "six"))).require.bytes shouldEqual expectedNestedBytes
    }

    it must "fail to encode type that is not a Set" in {
      codec.encode("0x01") should matchPattern { case Failure(_) => }
      codec.encode("1235") should matchPattern { case Failure(_) => }
      codec.encode(BigDecimal("123.67")) should matchPattern { case Failure(_) => }
      codec.encode(true) should matchPattern { case Failure(_) => }
      codec.encode(false) should matchPattern { case Failure(_) => }
    }

    it must "encode and decode back as Set" in {
      encodeAndDecode(codec, Set())
      encodeAndDecode(codec, Set("one", "two", "three"))
      encodeAndDecode(codec, List("one", "two", "three"), Set("one", "two", "three"))
      encodeAndDecode(nestedCodec, Set(Set("one", "two", "three"), Set("four", "five", "six")))
      encodeAndDecode(nestedCodec, Set(Set("one", "two", "three"), List("four", "five", "six")), Set(Set("one", "two", "three"), Set("four", "five", "six")))

      val nestedTupleCodec = DataType.Set(DataType.Tuple(DataType.Int, DataType.Text)).codec
      encodeAndDecode(nestedTupleCodec, Map(1 -> "one", 2 -> "two"), Set(List(1, "one"), List(2, "two")))
    }
  }
}
