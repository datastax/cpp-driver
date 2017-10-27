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

class ErrorMessageSpec extends CodecSpec {

  "ErrorMessage.codec" when {
    withProtocolVersions { (protocolVersion: ProtocolVersion) =>
      implicit val p = protocolVersion
      val codec = Codec[ErrorMessage]

      val messages = List(
        ServerError("server error"),
        ProtocolError("protocol error"),
        AuthenticationError("bad credentials error"),
        Unavailable("unavailable exception", Consistency.TWO, 5, 3),
        Overloaded("overloaded error"),
        IsBootstrapping("is bootstrapping error"),
        TruncateError("truncate error"),
        WriteTimeout("write timeout", Consistency.ALL, 0, 1, "CAS"),
        ReadTimeout("read timeout", Consistency.THREE, 2, 3, dataPresent=true),
        ReadFailure("read failure", Consistency.ANY, 2, 3, 1, dataPresent=false),
        FunctionFailure("function failure", "ks", "fun", "a" :: "b" :: Nil),
        WriteFailure("write failure", Consistency.EACH_QUORUM, 1, 1, 1, "BATCH"),
        SyntaxError("syntax error"),
        Unauthorized("unauthorized"),
        Invalid("invalid"),
        ConfigError("config error"),
        AlreadyExists("already exists", "ks", "tbl"),
        Unprepared("hello", ByteVector(1))
      )

      messages.foreach { msg =>
        "property identify codec for " + msg + " and encode and decode" in {
          encodeAndDecode(codec, msg)
        }
      }
    }

  }

}
