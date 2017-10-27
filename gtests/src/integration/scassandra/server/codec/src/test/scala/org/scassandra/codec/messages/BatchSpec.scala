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
import scodec.Codec
import scodec.bits.ByteVector

class BatchSpec extends CodecSpec {

  withProtocolVersions { (protocolVersion: ProtocolVersion) =>
    implicit val p = protocolVersion
    implicit val codec = Codec[Batch]

    "with simple statements, no timestamp or serial consistency" in {
      encodeAndDecode(
        Batch(BatchType.UNLOGGED, List(
          SimpleBatchQuery("insert into ks.tbl (k, c, v) values (1, 2, 0)"),
          SimpleBatchQuery("insert into ks.tbl (k, c, v) values (1, 2, 1)"),
          SimpleBatchQuery("insert into ks.tbl (k, c, v) values (1, 2, 2)")
        ), Consistency.ALL)
      )
    }

    "with prepared statements, no timestamp or serial consistency" in {
      encodeAndDecode(
        Batch(BatchType.LOGGED, List(
          PreparedBatchQuery(ByteVector(8, 6, 7, 5), List[Value](Bytes(ByteVector(2)))),
          SimpleBatchQuery("insert into ks.tbl (k, c, v) values (1, 2, 1)")
        ), Consistency.ALL)
      )
    }

    if(protocolVersion.version < 3) {
      "strip timestamp and serial consistency" in {
        val input = Batch(BatchType.UNLOGGED, List(
          SimpleBatchQuery("insert into ks.tbl (k, c, v) values (1, 2, 0)"),
          SimpleBatchQuery("insert into ks.tbl (k, c, v) values (1, 2, 1)"),
          SimpleBatchQuery("insert into ks.tbl (k, c, v) values (1, 2, 2)")
        ), Consistency.ALL, serialConsistency = Some(Consistency.SERIAL), timestamp = Some(8675))
        val expected = input.copy(serialConsistency = None, timestamp = None)
        encodeAndDecode(input, expected)
      }
    } else {
      "with timestamp and serial consistency" in {
        encodeAndDecode(
          Batch(BatchType.LOGGED, List(
            PreparedBatchQuery(ByteVector(8, 6, 7, 5), List[Value](Bytes(ByteVector(2)))),
            SimpleBatchQuery("insert into ks.tbl (k, c, v) values (1, 2, 1)")
          ), Consistency.ALL, Some(Consistency.LOCAL_SERIAL), timestamp = Some(309))
        )
      }
    }
  }
}
