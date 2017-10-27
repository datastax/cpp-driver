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
package org.scassandra.codec.messages

import org.scassandra.codec.{CodecSpec, ProtocolVersion, Supported}
import scodec.Codec

class SupportedSpec extends CodecSpec {

  "Supported.codec" when {
    withProtocolVersions { (protocolVersion: ProtocolVersion) =>
      implicit val p = protocolVersion
      implicit val codec = Codec[Supported]

      "encode and decode empty Supported map" in {
        encodeAndDecode(Supported())
      }

      "encode and decode Supported map" in {
        encodeAndDecode(Supported(Map[String,List[String]](
          "hello" -> List("world", "moon", "sun"),
          "goodbye" -> List("solar system")
        )))
      }
    }
  }

}
