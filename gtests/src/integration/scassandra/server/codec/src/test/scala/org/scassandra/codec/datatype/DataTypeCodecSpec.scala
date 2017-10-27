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

import org.scassandra.codec._
import scodec.Attempt.Failure
import scodec.bits.ByteVector

class DataTypeCodecSpec extends CodecSpec {

  val bytesText = ByteVector(0x00, 0x0A)
  val bytesVarchar = ByteVector(0x00, 0x0D)
  val bytesDate = ByteVector(0x00, 0x11)

  "DataType.codec" when {

    withProtocolVersions { (protocolVersion: ProtocolVersion) =>
      val codec = DataType.codec(protocolVersion)

      // Validate the behavior of the 'Text' datatype.  In protocol versions 3+ Text was silently removed from the
      // spec.   However, CQL still supports the 'text' column type, which is silently aliased to 'varchar'.  For
      // convenience, we simply alias the opcode of 'Text' to 'Varchar'.
      if (protocolVersion.version < 3) {
        "return Text with Text opcode" in {
          val data = codec.encode(DataType.Text).require.bytes
          data shouldEqual bytesText
        }
      } else {
        "alias Text and return with Varchar opcode" in {
          val data = codec.encode(DataType.Text).require.bytes
          data shouldEqual bytesVarchar
        }
      }

      if (protocolVersion.version >= 4) {
        "support Date type" in {
          val data = codec.encode(DataType.Date).require.bytes
          data shouldEqual bytesDate
        }
      } else {
        "not support type added later in the spec" in {
          codec.encode(DataType.Date) should matchPattern {
            case Failure(_) =>
          }
        }
      }
    }
  }
}
