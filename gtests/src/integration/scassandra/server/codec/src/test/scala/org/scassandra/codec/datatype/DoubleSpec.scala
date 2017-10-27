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

class DoubleSpec extends DataTypeSpec {

  val codec = DataType.Double.codec

  "codec" must "encode Number as double format" in {
    codec.encode(BigDecimal("123")).require.bytes shouldEqual ByteVector(64, 94, -64, 0, 0, 0, 0, 0)
    codec.encode(123).require.bytes shouldEqual ByteVector(64, 94, -64, 0, 0, 0, 0, 0)
    codec.encode(123.67).require.bytes shouldEqual ByteVector(64, 94, -22, -31, 71, -82, 20, 123)
    codec.encode(123.67f).require.bytes shouldEqual ByteVector(64, 94, -22, -31, 64, 0, 0, 0)
  }

  it must "encode String that can be parsed as a Double as double format" in {
    codec.encode("123.67").require.bytes shouldEqual ByteVector(64, 94, -22, -31, 71, -82, 20, 123)
  }

  it must "fail to encode value that doesn't map to a double" in {
    codec.encode("hello") should matchPattern { case Failure(_) => }
    codec.encode(true) should matchPattern { case Failure(_) => }
    codec.encode(false) should matchPattern { case Failure(_) => }
    codec.encode(List()) should matchPattern { case Failure(_) => }
    codec.encode(Map()) should matchPattern { case Failure(_) => }
  }

  it must "encode and decode back as Double" in {
    encodeAndDecode(codec, 123.00)
    encodeAndDecode(codec, 123.67)
    encodeAndDecode(codec, BigDecimal("123"), 123.0)
    // There will be some precision loss when dealing with float.
    encodeAndDecode(codec, 123.67f, 123.66999816894531)
    encodeAndDecode(codec, "123.67", 123.67)
  }
}
