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

class BooleanSpec extends DataTypeSpec {

  val codec = DataType.Boolean.codec

  "codec" must "encode Boolean values" in {
    codec.encode(true).require.bytes shouldEqual ByteVector(1)
    codec.encode(false).require.bytes shouldEqual ByteVector(0)
  }

  it must "encode string representations of true into true" in {
    codec.encode("TRUE").require.bytes shouldEqual ByteVector(1)
    codec.encode("true").require.bytes shouldEqual ByteVector(1)
  }

  it must "encode string representations of false into false" in {
    codec.encode("FALSE").require.bytes shouldEqual ByteVector(0)
    codec.encode("false").require.bytes shouldEqual ByteVector(0)
  }

  it must "fail to encode string representations that don't map to a boolean value" in {
    codec.encode("123") should matchPattern { case Failure(_) => }
    codec.encode("hello") should matchPattern { case Failure(_) => }
    codec.encode(BigDecimal("123.67")) should matchPattern { case Failure(_) => }
    codec.encode(List()) should matchPattern { case Failure(_) => }
    codec.encode(Map()) should matchPattern { case Failure(_) => }
  }

  it must "encode and decode back to input as boolean" in {
    encodeAndDecode(codec, true)
    encodeAndDecode(codec, "true", true)
    encodeAndDecode(codec, false)
    encodeAndDecode(codec, "false", false)
  }
}
