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
  * Indicates the direction of a frame.
  */
sealed trait MessageDirection

/**
  * Represents the 'request' direction (0).
  */
case object Request extends MessageDirection

/**
  * Represents the 'response' direction (1).
  */
case object Response extends MessageDirection

object MessageDirection {
  /**
    * 1 bit representation of the direction of a frame.  If 0, it's a request, else response.
    */
  implicit val codec: Codec[MessageDirection] = mappedEnum(bool,
    Request -> false,
    Response -> true
  )
}

/**
  * Represents the first byte in the frame, which indicates the [[ProtocolVersion]] and [[MessageDirection]]
  * @param direction direction of the frame
  * @param version protocol version of the frame
  */
sealed case class ProtocolFlags(
  direction: MessageDirection,
  version: ProtocolVersion
) {

  /**
    * Knowing the direction and version, we can provide a [[Codec]] for parsing the header.  This codec starts by
    * providing these flags and passes them to [[FrameHeader.withProtocol]] to parse the rest of [[FrameHeader]].
    */
  lazy val headerCodec: Codec[FrameHeader] = version.headerCodec(direction)
}

object ProtocolFlags {
  /**
    * Represents the first byte in the frame, with the 1st bit representing the [[MessageDirection]] and the remaining
    * 7 bits representing the [[ProtocolVersion]].
    */
  implicit val codec: Codec[ProtocolFlags] = {
    ("direction" | MessageDirection.codec) ::
    ("version"   | ProtocolVersion.codec)
  }.as[ProtocolFlags]
}

/**
  * Indicates what extra content is in the frame beyond the [[Message]].
  * @param warning Whether or not warnings are present.
  * @param customPayload Whether or not custom payload is present.
  * @param tracing Whether or not tracing information is present.
  * @param compression Whether or not the frame body is compressed.
  */
sealed case class HeaderFlags(
  warning: Boolean = false,
  customPayload: Boolean = false,
  tracing: Boolean = false,
  compression: Boolean = false
)

/**
  * A version of [[HeaderFlags]] where all bits are disabled.
  */
object EmptyHeaderFlags extends HeaderFlags()

object HeaderFlags {
  /**
    * [[Codec]] for parsing [[HeaderFlags]] ignores first 4 bits and parses the remaining into the fields present on
    * header flags.
    */
  implicit val codec: Codec[HeaderFlags] = {
    ("reserved"      | ignore(4)) ::
    ("warning"       | bool)      ::
    ("customPayload" | bool)      ::
    ("tracing"       | bool)      ::
    ("compression"   | bool)
  }.dropUnits.as[HeaderFlags]
}

/**
  * Represents the Frame Header of a native protocol [[Frame]].
  * @param version The version and direction of the frame.
  * @param flags header flags indicating additional content and whether or not the body is compressed.
  * @param stream The stream id uniquely identifying a [[Frame]] in flight.
  * @param opcode distinguishes the [[Message]] contained int he [[Frame]].
  * @param length The length of the body of the [[Frame]].
  */
case class FrameHeader (
  version: ProtocolFlags,
  flags: HeaderFlags = EmptyHeaderFlags,
  stream: Int = 0,
  opcode: Int = ErrorMessage.opcode,
  length: Long = 0
)

object FrameHeader {

  private[codec] def withProtocol(protocol: ProtocolFlags) = {
    ("flags"   | HeaderFlags.codec)              ::
    ("stream"  | protocol.version.streamIdCodec) ::
    ("opcode"  | uint8)                          ::
    ("length"  | uint32)
  }

  /**
    * [[Codec]] that parses [[FrameHeader]] by first inspecting the [[ProtocolFlags]] and then uses the
    * [[ProtocolVersion]] to determine how to parse the rest of the [[FrameHeader]].
    */
  implicit val codec: Codec[FrameHeader] = {
    ProtocolFlags.codec.consume { protocolFlags =>
      protocolFlags.headerCodec
    }(_.version)
  }
}
