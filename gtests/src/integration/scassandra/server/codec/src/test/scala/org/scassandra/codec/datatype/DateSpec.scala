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

class DateSpec extends BigintSpec {
  override val codec = DataType.Date.codec
  override val exampleNumber: Number = 4294967295L
  override val exampleNumberBytes = ByteVector(0xFF, 0xFF, 0xFF, 0xFF)

  it must "fail to encode a number > 2^32-1" in {
    codec.encode(4294967296L) should matchPattern { case Failure(_) => }
  }

  it must "fail to encode a negative number" in {
    codec.encode(-1) should matchPattern { case Failure(_) => }
  }
}