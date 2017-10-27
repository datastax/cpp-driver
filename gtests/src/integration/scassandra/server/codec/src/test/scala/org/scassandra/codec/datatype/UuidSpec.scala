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

import java.util.UUID

import scodec.Attempt.Failure
import scodec.bits.ByteVector

class UuidSpec extends DataTypeSpec {

  val codec = DataType.Uuid.codec
  val uuidStr = "59ad61d0-c540-11e2-881e-b9e6057626c4"
  val uuid = UUID.fromString(uuidStr)
  val uuidBytes = ByteVector(89, -83, 97, -48, -59, 64, 17, -30, -120, 30, -71, -26, 5, 118, 38, -60)

  "codec" must "encode UUID as uuid format" in {
    codec.encode(uuid).require.bytes shouldEqual uuidBytes
  }

  it must "encode String that can be parsed as a UUID as String format" in {
    codec.encode(uuidStr).require.bytes shouldEqual uuidBytes
  }

  it must "fail to encode value that doesn't map to a decimal" in {
    codec.encode(BigDecimal("123.67")) should matchPattern { case Failure(_) => }
    codec.encode("hello") should matchPattern { case Failure(_) => }
    codec.encode(true) should matchPattern { case Failure(_) => }
    codec.encode(false) should matchPattern { case Failure(_) => }
    codec.encode(List()) should matchPattern { case Failure(_) => }
    codec.encode(Map()) should matchPattern { case Failure(_) => }
  }

  it must "encode and decode back to input as UUID" in {
    encodeAndDecode(codec, uuid)
    encodeAndDecode(codec, uuidStr, uuid)
  }

}
