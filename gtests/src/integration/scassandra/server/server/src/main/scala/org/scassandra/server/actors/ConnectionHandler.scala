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
package org.scassandra.server.actors

import akka.actor.{Actor, ActorLogging, ActorRef, ActorRefFactory, Props}
import akka.io.Tcp
import akka.io.Tcp.{ConnectionClosed, Received, ResumeReading, Write}
import akka.pattern.{ask, pipe}
import akka.util.ByteString.ByteString1C
import akka.util.{ByteString, Timeout}
import org.scassandra.codec._
import scodec.bits.ByteVector

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

class ConnectionHandler(tcpConnection: ActorRef,
                        queryHandlerFactory: (ActorRefFactory) => ActorRef,
                        batchHandlerFactory: (ActorRefFactory, ActorRef) => ActorRef,
                        registerHandlerFactory: (ActorRefFactory) => ActorRef,
                        optionsHandlerFactory: (ActorRefFactory) => ActorRef,
                        prepareHandler: ActorRef,
                        executeHandler: ActorRef) extends Actor with ActorLogging {

  import AkkaScodecInterop._

  implicit val timeout: Timeout = 1 seconds

  val system = context.system
  import system.dispatcher

  // Used to prevent repeated attempts at sending same error message.
  var errorMessage: Option[Message] = None

  // Extracted to handle full messages
  val cqlMessageHandler = context.actorOf(Props(classOf[NativeProtocolMessageHandler],
    queryHandlerFactory,
    batchHandlerFactory,
    registerHandlerFactory,
    optionsHandlerFactory,
    prepareHandler,
    executeHandler
  ))

  override def preStart(): Unit = tcpConnection ! ResumeReading

  override def receive: Receive = buffering(Buffer(ByteString(), Start))

  def buffering(buffer: Buffer): Receive = {
    case Received(data) =>
      val newBuffer = handle(buffer.copy(data = buffer.data ++ data))
      tcpConnection ! ResumeReading
      context become buffering(newBuffer)

    case c: ConnectionClosed =>
      log.info(s"Client disconnected: $c")
      context stop self

    // Message generated from another actor to be sent back to the tcp connection.
    case ProtocolResponse(requestHeader, message) =>
      message.toBytes(requestHeader.stream)(requestHeader.version.version) match {
        case Success(bytes) => tcpConnection ! Write(bytes.toByteString)
        case Failure(t) => {
          // In the failure case, send a protocol error back to the client, however
          // if the message we tried to serialize was the error itself, log an error
          // and abort.
          errorMessage match {
            case Some(`message`) =>
              log.error("Failure writing error, closing connection to client", t)
              tcpConnection ! Tcp.Abort
              context stop self
            case _ =>
              val error = ProtocolError(t.getMessage)
              errorMessage = Some(error)
              log.error(t, "Failure getting message payload to write.")
              self ! ProtocolResponse(requestHeader, error)
          }
        }
      }

    // Forward any TCP commands to the underlying connection.
    // This allows those with a reference to this actor to perform
    // commands such as Close (immediate close), ConfirmedClose (half close),
    // and Abort (RST) the connection.
    case command: Tcp.Command =>
      (tcpConnection ? command).pipeTo(sender())
  }

  @tailrec
  private[this] def handle(buffer: Buffer): Buffer = advanceState(buffer) match {
    // Continue if the state changed, otherwise terminate
    case Success(b) => if (buffer == b) b else handle(b)
    case Failure(UnsupportedProtocolException(v)) =>
      // we can't really send the correct stream back as it hasn't been parsed because we don't know how to parse
      // this protocol, so instead we assume stream 0.
      log.warning(s"Received protocol version $v, currently only versions 1,2,3 and 4 supported so sending an unsupported protocol error to get the driver to use an older version of the protocol.")
      self ! ProtocolResponse(FrameHeader(ProtocolFlags(Request, ProtocolVersionV4)), ProtocolError(s"Invalid or unsupported protocol version"))
      // Reset the buffer as any remaining data we won't be able to appropriately parse.
      Buffer(ByteString(), Start)
    case Failure(x) =>
      log.error(x, "Exception parsing message")
      // TODO send some kind of protocol error, this is tricky because we have no context of the state
      // of the session lifecycle here.   Perhaps forward the error on to the protocol handler to
      // handle this.
      sender() ! Tcp.Abort
      context stop self
      buffer
  }

  private[this] def inspectProtocolVersion(input: ByteVector): Try[(ProtocolFlags, ByteVector)] = {
    next[ProtocolFlags](input).flatMap {
      case ((ProtocolFlags(_, UnsupportedProtocolVersion(v)), _)) =>
        Failure(UnsupportedProtocolException(v))
      case x => Success(x)
    }
  }

  private[this] def advanceState(buffer: Buffer): Try[Buffer] = buffer.state match {
    case Start =>
      // At the beginning of a frame parse the first byte into protocol flags, which will indicate
      // the protocol version, and move to the AwaitHeader state.
      updateState(buffer, 1, inspectProtocolVersion, AwaitHeader)
    case AwaitHeader(flags) =>
      // Protocol version has been parsed, attempt to parse the header and move to the AwaitBody state.
      updateState(buffer, flags.version.headerLength-1, next(flags.headerCodec, _), AwaitBody)
    case AwaitBody(header) =>
      // Header has been parsed, attempt to parse the message body and move back to the Start state.
      updateState(buffer, header.length, next(Message.codec(header.opcode)(header.version.version), _), (m: Message) => {
        // When the frame has been parsed, forward it on to the protocol handler.
        // We use forward so the sender appears as the tcp connection actor instead of this actor.
        val frame = Frame(header, m)
        cqlMessageHandler ! ProtocolMessage(frame)
        Start
      })
  }

  private[this] def updateState[T](buffer: Buffer, requiredLength: Long, f: ByteVector => Try[(T, ByteVector)], g: T => ParsingState): Try[Buffer] = {
    if (buffer.data.length >= requiredLength) {
      f(buffer.data.toByteVector).map { d =>
        val parsingState = g(d._1)
        // Take the remaining data after what was required.  It's possible the remaining data in the frame is unused.
        Buffer(buffer.data.drop(requiredLength.toInt), parsingState)
      }
    } else {
      // Does not meet length requirement, return input.
      Success(buffer)
    }
  }
}

case class Buffer(data: ByteString, state: ParsingState)

sealed trait ParsingState
case object Start extends ParsingState
case class AwaitHeader(protocolFlags: ProtocolFlags) extends ParsingState
case class AwaitBody(frameHeader: FrameHeader) extends ParsingState

case class UnsupportedProtocolException(version: Int) extends Exception(s"Unsupported Protocol Version $version")

/**
  * A modified version of akka-scodec.  This is used in place of akka-scodec because we need JDK6 support.  As
  * akka-scodec uses Akka 2.4 and default methods, this was not an option.
  *
  * The primary modification is the usage of reflection in EnrichedByteVector.toByteString.  This is used instead of
  * PrivacyHelper, which uses default interface methods.
  *
  * License from akka-scodec:
  *
  * Copyright (c) 2015, Michael Pilquist
  * All rights reserved.
  *
  * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
  *
  * 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
  * 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
  * 3. Neither the name of the scodec team nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.
  *
  * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
  */
object AkkaScodecInterop {

  // ByteString1C's constructor is private, use reflection to resolve it and make it accessible, this can be a
  // problem depending on the JDK security.
  // TODO: Fallback to ByteString() if permission failure.
  private [this] lazy val byte1Ctor = {
    val ctor = classOf[ByteString1C].getDeclaredConstructor(classOf[Array[Byte]])
    ctor.setAccessible(true)
    ctor
  }

  implicit class EnrichedByteString(val value: ByteString) extends AnyVal {
    def toByteVector: ByteVector = ByteVector.viewAt((idx: Long) => value(idx.toInt), value.size.toLong)
  }

  implicit class EnrichedByteVector(val value: ByteVector) extends AnyVal {
    def toByteString: ByteString = byte1Ctor.newInstance(value.toArray)
  }
}
