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
package org.scassandra.codec

import scodec.Codec
import scodec.codecs._

/**
  * Defines a Frame, the top level payload in the spec.  See section one of the spec.
  *
  * A Frame is composed of a header which indicates information about the body, and the message body itself.
  *
  * @param header Header of the message
  * @param message The message itself.
  */
case class Frame(
  header: FrameHeader,
  message: Message
)

// TODO: Support warnings, custom payload, and tracing.

object Frame {
  /**
    * Codec for [[Frame]], reads header and then uses the [[FrameHeader]] to determine the [[Message]].
    */
  implicit val codec: Codec[Frame] = {
    ("header" | FrameHeader.codec) flatPrepend { header =>
    ("message" | Message.codec(header.opcode)(header.version.version)).hlist
  }}.as[Frame]
}