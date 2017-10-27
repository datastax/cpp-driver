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

class IntSpec extends DataTypeSpec {

  val codec = DataType.Int.codec
  val example: Number = BigDecimal("123")
  val exampleBytes = ByteVector(0, 0, 0, 123)
  val naturalType: Number = 123

  "codec" must "encode Number as int format" in {
    codec.encode(example).require.bytes shouldEqual exampleBytes
    codec.encode(example.intValue()).require.bytes shouldEqual exampleBytes
  }

  it must "encode String that can be parsed as a Int as int format" in {
    codec.encode("123").require.bytes shouldEqual exampleBytes
  }

  it must "fail to encode value that doesn't map to an int" in {
    codec.encode("hello") should matchPattern { case Failure(_) => }
    codec.encode(true) should matchPattern { case Failure(_) => }
    codec.encode(false) should matchPattern { case Failure(_) => }
    codec.encode(List()) should matchPattern { case Failure(_) => }
    codec.encode(Map()) should matchPattern { case Failure(_) => }
  }

  it must "encode and decode back as natural type" in {
    encodeAndDecode(codec, example, naturalType)
  }

}
