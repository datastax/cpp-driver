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

import scodec.Attempt.Failure
import scodec.Err.{General, InsufficientBits}
import scodec.SizeBound
import scodec.bits.ByteVector

class TupleSpec extends DataTypeSpec {

  "codec" must "encode tuple<int,text> from a List" in {
    val tupleType = DataType.Tuple(DataType.Int, DataType.Text)

    tupleType.codec.encode(List(55, "Hello World")).require shouldEqual ByteVector(
      0, 0, 0, 4,     // int length
      0, 0, 0, 0x37,  // 55
      0, 0, 0, 0xb,   // length of 'Hello World'
      0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x57, 0x6f, 0x72, 0x6c, 0x64 // Hello World
    ).bits
  }

  it must "encode null values in a List" in {
    val tupleType = DataType.Tuple(DataType.Int, DataType.Int, DataType.Text)

    tupleType.codec.encode(List(55, null, "Hello World")).require shouldEqual ByteVector(
      0, 0, 0, 4,             // int length
      0, 0, 0, 0x37,          // 55
      0xFF, 0xFF, 0xFF, 0xFF, // -1, null indicator
      0, 0, 0, 0xb,           // length of 'Hello World'
      0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x57, 0x6f, 0x72, 0x6c, 0x64 // Hello World
    ).bits
  }

  it must "fail to encode when List length does not match tuple length" in {
    val tupleType = DataType.Tuple(DataType.Int, DataType.Text, DataType.Int)

    tupleType.codec.encode(List(55, "Hello")) should matchPattern { case Failure(General("List of size 2 does not match number of expected codec elements for Tuple(List(Int, Text, Int))", Nil)) => }
  }

  it must "encode scala Tuples" in {
    val tupleType = DataType.Tuple(DataType.Int, DataType.Tinyint)

    tupleType.codec.encode((5, 3)).require shouldEqual ByteVector(
      0, 0, 0, 4, // int length
      0, 0, 0, 5, // 5
      0, 0, 0, 1, // tinyint length
      3           // 3
    ).bits
  }

  it must "fail decode if invalid value identifier" in {
    val tupleType = DataType.Tuple(DataType.Int)

    // -3, only -1 (null) and >= 0 should be allowed.
    val invalidBytes = ByteVector(0xFF, 0xFF, 0xFF, 0xFD).bits

    tupleType.codec.decode(invalidBytes) should matchPattern { case Failure(General("Invalid [value] identifier -3", Nil)) => }
  }

  it must "fail decode if value is unset" in {
    val tupleType = DataType.Tuple(DataType.Int)

    // -2, only -1 (null) and >= 0 should be allowed.
    val invalidBytes = ByteVector(0xFF, 0xFF, 0xFF, 0xFE).bits

    tupleType.codec.decode(invalidBytes) should matchPattern { case Failure(General("Found UNSET [value] in data, this is unexpected", Nil)) => }
  }

  it must "fail to decode if a value cannot be decoded with the given type" in {
    val tupleType = DataType.Tuple(DataType.Int, DataType.Int)

    // invalid because it expects <Int, Int> but we provide <Int, Smallint>
    val invalidBytes = ByteVector(
      0, 0, 0, 4,   // int length
      8, 6, 7, 5,   // 8675
      0, 0, 0, 2,   // smallint length
      0, 0x37       // 55 as smallint
    ).bits

    // Should fail because the second tuple value is not wide enough (16 instead of 32)
    tupleType.codec.decode(invalidBytes) should matchPattern { case Failure(InsufficientBits(32, 16, _)) => }
  }

  it must "calculate sizes based on number of elements" in {
    // SizeBound should be a function of number of tuple elements with the formula being
    // min = elements*int length(4)*8
    // max = elements*int length(4)*8 + elements*Int.MaxValue*8

    def forCount(elementCount: Int) = {
      SizeBound.bounded(elementCount * 4 * 8, (elementCount * 4 * 8) + elementCount * (Int.MaxValue.toLong*8))
    }

    DataType.Tuple(DataType.Int).codec.sizeBound shouldEqual forCount(1)
    DataType.Tuple(DataType.Int, DataType.Text).codec.sizeBound shouldEqual forCount(2)
    DataType.Tuple(DataType.Int, DataType.Float, DataType.Bigint).codec.sizeBound shouldEqual forCount(3)
  }

  it must "encode and decode back to input" in {
    encodeAndDecode(DataType.Tuple(DataType.Int).codec, List(4844))
    encodeAndDecode(DataType.Tuple(DataType.Int, DataType.Text).codec, List(55, "Hello"))
    encodeAndDecode(DataType.Tuple(DataType.Int, DataType.Text, DataType.List(DataType.Float)).codec, List(55, "Hello", List(1.2f, 2.3f, 3.4f)))
    encodeAndDecode(DataType.Tuple(DataType.Int, DataType.Text, DataType.Tuple(DataType.Float, DataType.Int)).codec, List(55, "Hello", List(1.2f, 5)))
    // Technically not valid CQL, but the protocol doesn't say you can't have empty tuples.
    encodeAndDecode(DataType.Tuple().codec, Nil)
  }
}
