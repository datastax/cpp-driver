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

import org.scassandra.codec._
import org.scassandra.codec.datatype.DataType
import scodec.Attempt.Successful
import scodec.Codec
import scodec.bits.ByteVector

class QuerySpec extends CodecSpec {

  "Query.codec" when {
    withProtocolVersions { (protocolVersion: ProtocolVersion) =>
      implicit val p = protocolVersion
      val codec = Codec[Query]

      "include query string" in {
        encodeAndDecode(codec, Query("select * from system.local"))
      }
    }
  }

  "QueryParameters.codec" when {

    withProtocolVersions { (protocolVersion: ProtocolVersion) =>
      implicit val p = protocolVersion
      val codec = Codec[QueryParameters]


      if(protocolVersion.version == 1) {
        "skip flags even if set" in {
          // we expect flags and their associated values to be truncated for protocol V1.
          val parametersWithPaging = QueryParameters(Consistency.TWO, pageSize = Some(50))
          val expected = parametersWithPaging.copy(pageSize = None)
          val encoded = codec.encode(parametersWithPaging).require
          val decoded = codec.decodeValue(encoded) should matchPattern {
            case Successful(`expected`) =>
          }
        }
      } else {
        // Test each parameter individually as their presence is based on flag parsing code which could be buggy.
        "include query parameters with names" in {
          encodeAndDecode(codec, QueryParameters(values = Some(QueryValue("a", "a", DataType.Text) :: Nil)))
        }

        "include query parameters without names" in {
          encodeAndDecode(codec, QueryParameters(values = Some(QueryValue("a", DataType.Text) :: Nil)))
        }

        "include skipMetadata" in {
          encodeAndDecode(codec, QueryParameters(skipMetadata = true))
          encodeAndDecode(codec, QueryParameters(skipMetadata = false))
        }

        "include pageSize" in {
          encodeAndDecode(codec, QueryParameters(pageSize = Some(5000)))
        }

        "include pagingState" in {
          encodeAndDecode(codec, QueryParameters(pagingState = Some(ByteVector(0x00))))
        }

        "include serialConsistency" in {
          encodeAndDecode(codec, QueryParameters(serialConsistency = Some(Consistency.LOCAL_SERIAL)))
        }

        "include timestamp" in {
          encodeAndDecode(codec, QueryParameters(timestamp = Some(8675309)))
        }
      }
    }
  }
}
