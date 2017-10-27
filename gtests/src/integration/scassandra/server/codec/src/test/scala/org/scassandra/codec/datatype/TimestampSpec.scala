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

class TimestampSpec extends DataTypeSpec {

  val codec = DataType.Timestamp.codec

  "codec" must "encode number as timestamp format" in {
    codec.encode(1436466253234L).require.bytes shouldEqual ByteVector(0, 0, 1, 78, 116, 15, -115, -78)
  }

  it must "encode String that can be parsed as a timestamp as timestamp format" in {
    codec.encode("1436466253234").require.bytes shouldEqual ByteVector(0, 0, 1, 78, 116, 15, -115, -78)
  }

  it must "fail to enocde value that doesn't map to a timestamp" in {
    codec.encode("hello") should matchPattern { case Failure(_) => }
    codec.encode(true) should matchPattern { case Failure(_) => }
    codec.encode(false) should matchPattern { case Failure(_) => }
    codec.encode(List()) should matchPattern { case Failure(_) => }
    codec.encode(Map()) should matchPattern { case Failure(_) => }
  }

  it must "encode and decode back as Long" in {
    encodeAndDecode(codec, 1436466253234L)
    encodeAndDecode(codec, "1436466253234", 1436466253234L)
  }
}
