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
import scodec.bits.ByteVector

class FloatSpec extends DataTypeSpec {

  val codec = DataType.Float.codec

  "codec" must "encode Number as float format" in {
    codec.encode(BigDecimal("123")).require.bytes shouldEqual ByteVector(66, -10, 0, 0)
    codec.encode(123).require.bytes shouldEqual ByteVector(66, -10, 0, 0)
    codec.encode(123.67).require.bytes shouldEqual ByteVector(66, -9, 87, 10)
    codec.encode(123.67f).require.bytes shouldEqual ByteVector(66, -9, 87, 10)
  }

  it must "encode String that can be parsed as a Double as float format" in {
    codec.encode("123.67").require.bytes shouldEqual ByteVector(66, -9, 87, 10)
  }

  it must "fail to encode value that doesn't map to a float" in {
    codec.encode("hello") should matchPattern { case Failure(_) => }
    codec.encode(true) should matchPattern { case Failure(_) => }
    codec.encode(false) should matchPattern { case Failure(_) => }
    codec.encode(List()) should matchPattern { case Failure(_) => }
    codec.encode(Map()) should matchPattern { case Failure(_) => }
  }

  it must "encode and decode back as Float" in {
    encodeAndDecode(codec, 123.67f)
    encodeAndDecode(codec, 123.00, 123.00f)
    encodeAndDecode(codec, 123.67, 123.67f)
    encodeAndDecode(codec, BigDecimal("123"), 123.00f)
    encodeAndDecode(codec, "123.67", 123.67f)
  }
}
