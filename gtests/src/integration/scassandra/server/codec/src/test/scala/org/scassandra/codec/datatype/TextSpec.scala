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
import scodec.Err.General
import scodec.bits.ByteVector

class TextSpec extends DataTypeSpec {

  val codec = DataType.Text.codec

  "codec" must "encode a string as UTF-8" in {
    codec.encode("hello").require.bytes shouldEqual ByteVector(104, 101, 108, 108, 111)
  }

  it must "encode empty string to empty bytes" in {
    codec.encode("").require.bytes shouldEqual ByteVector.empty
  }

  it must "encode any non-String as its toString representation" in {
    codec.encode(BigDecimal("123.67")).require.bytes shouldEqual ByteVector(49, 50, 51, 46, 54, 55)
    codec.encode(true).require.bytes shouldEqual ByteVector(116, 114, 117, 101)
  }

  it must "fail when encoding Iterable" in {
    codec.encode(Nil) should matchPattern { case Failure(General("Unsure how to encode List()", Nil)) => }
    codec.encode(Set(1)) should matchPattern { case Failure(General("Unsure how to encode Set(1)", Nil)) => }
  }

  it must "fail when encoding Map" in {
    codec.encode(Map()) should matchPattern { case Failure(General("Unsure how to encode Map()", Nil)) => }
  }

  it must "encode UTF-8 text" in {
    codec.encode("hello ⌦ utf➑").require.bytes shouldEqual ByteVector(
      0x68, 0x65, 0x6c, 0x6c, 0x6f, // hello
      0x20, // space
      0xe2, 0x8c, 0xa6, // backspace
      0x20, // space
      0x75, 0x74, 0x66, // utf
      0xe2, 0x9e, 0x91 // 8-ball
    )
  }

  it must "encode and decode back to input as string" in {
    encodeAndDecode(codec, "hello")
    encodeAndDecode(codec, "")
    encodeAndDecode(codec, 123, "123")
    encodeAndDecode(codec, BigDecimal("123.67"), "123.67")
    encodeAndDecode(codec, "hello ⌦ utf➑")
  }
}
