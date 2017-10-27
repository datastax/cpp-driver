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

class TimeSpec extends BigintSpec {
  override val codec = DataType.Time.codec

  it must "fail to encode a number > 86399999999999" in {
    codec.encode(86400000000000L) should matchPattern { case Failure(_) => }
  }

  it must "fail to encode a negative number" in {
    codec.encode(-1) should matchPattern { case Failure(_) => }
  }
}
