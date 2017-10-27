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

import org.scassandra.codec.datatype.DataType.Decimal
import scodec.Attempt.Failure
import scodec.bits.ByteVector

class DecimalSpec extends DataTypeSpec {

  val codec = Decimal.codec

  "codec" must "encode BigDecimal as decimal format" in {
    // Scale of -6
    // Unscaled value of 123
    // 123 * 10 ^ (-1 * -6) = 123000000 = 12.3E+7
    codec.encode(BigDecimal("12.3E+7")).require.bytes shouldEqual ByteVector(-1, -1, -1, -6, 123)
    // Scale of 8
    // Unscaled value of 123
    // 123 * 10 ^ (-1 * 8) = 0.00000123 = 12.3E-7
    codec.encode(BigDecimal("12.3E-7")).require.bytes shouldEqual ByteVector(0, 0, 0, 8, 123)
  }

  it must "encode String that can be parsed as a BigDecimal as decimal format" in {
    codec.encode("12.3E-7").require.bytes shouldEqual ByteVector(0, 0, 0, 8, 123)
  }

  it must "encode Number that can be converted to a BigDecimal as decimal format" in {
    // 10 * 10 ^ -1 = 1
    codec.encode(1).require.bytes shouldEqual ByteVector(0, 0, 0, 1, 0xa)
    codec.encode(1.0).require.bytes shouldEqual ByteVector(0, 0, 0, 1, 0xa)
    codec.encode(BigInt(1)).require.bytes shouldEqual ByteVector(0, 0, 0, 1, 0xa)
  }

  it must "fail to encode value that doesn't map to a decimal" in {
    codec.encode("hello") should matchPattern { case Failure(_) => }
    codec.encode(true) should matchPattern { case Failure(_) => }
    codec.encode(false) should matchPattern { case Failure(_) => }
    codec.encode(List()) should matchPattern { case Failure(_) => }
    codec.encode(Map()) should matchPattern { case Failure(_) => }
  }

  it must "encode and decode back to input as BigDecimal" in {
    encodeAndDecode(codec, BigDecimal("12.3E+7"))
    encodeAndDecode(codec, BigDecimal("12.3E-7"))
    encodeAndDecode(codec, "12.3E-7", BigDecimal("12.3E-7"))
    encodeAndDecode(codec, 1, BigDecimal(1))
    encodeAndDecode(codec, 1.0, BigDecimal(1))
    encodeAndDecode(codec, BigInt(1), BigDecimal(1))
  }

}
