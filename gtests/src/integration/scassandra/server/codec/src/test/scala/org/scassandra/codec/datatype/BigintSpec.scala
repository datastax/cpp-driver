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

class BigintSpec extends DataTypeSpec {
  val codec = DataType.Bigint.codec
  val exampleNumber: Number = BigDecimal("123000000000")
  val exampleNumberBytes = ByteVector(0, 0, 0, 28, -93, 95, 14, 0)

  "codec" must "encode Number as long" in {
    codec.encode(exampleNumber).require.bytes shouldEqual exampleNumberBytes
  }

  it must "fail to encode a string representation that doesn't map to long" in {
    codec.encode("hello") should matchPattern { case Failure(_) => }
  }

  it must "fail to encode type that is not a String or Number" in {
    codec.encode(true) should matchPattern { case Failure(_) => }
    codec.encode(false) should matchPattern { case Failure(_) => }
    codec.encode(List()) should matchPattern { case Failure(_) => }
    codec.encode(Map()) should matchPattern { case Failure(_) => }
  }

  it must "encode and decode back as Long" in {
    codec.encode(codec, 8675L)
    codec.encode(codec, exampleNumber.longValue())
    codec.encode(codec, 8675, 8675L)
  }
}
