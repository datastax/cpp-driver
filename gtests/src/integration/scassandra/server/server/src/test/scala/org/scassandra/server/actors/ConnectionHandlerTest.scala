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

import akka.actor.ActorSystem
import akka.io.Tcp.{Received, ResumeReading, Write}
import akka.testkit._
import org.scalatest._
import org.scassandra.codec._
import scodec.Codec
import scodec.bits.ByteVector

import scala.concurrent.duration._
import scala.language.postfixOps

class ConnectionHandlerTest extends TestKit(ActorSystem("ConnectionHandlerTest")) with ProtocolActorTest with Matchers
  with ImplicitSender with FunSuiteLike with BeforeAndAfter {

  import AkkaScodecInterop._

  var testActorRef : TestActorRef[ConnectionHandler] = null

  var tcpConnectionTestProbe : TestProbe = null
  var queryHandlerTestProbe : TestProbe = null
  var batchHandlerTestProbe : TestProbe = null
  var registerHandlerTestProbe : TestProbe = null
  var optionsHandlerTestProbe : TestProbe = null
  var prepareHandlerTestProbe : TestProbe = null
  var executeHandlerTestProbe : TestProbe = null

  before {
    tcpConnectionTestProbe = TestProbe()
    queryHandlerTestProbe = TestProbe()
    registerHandlerTestProbe = TestProbe()
    prepareHandlerTestProbe = TestProbe()
    executeHandlerTestProbe = TestProbe()
    optionsHandlerTestProbe = TestProbe()
    batchHandlerTestProbe = TestProbe()
    testActorRef = TestActorRef(new ConnectionHandler(
      self,
      (_) => queryHandlerTestProbe.ref,
      (_, _) => batchHandlerTestProbe.ref,
      (_) => registerHandlerTestProbe.ref,
      (_) => optionsHandlerTestProbe.ref,
      prepareHandlerTestProbe.ref,
      executeHandlerTestProbe.ref
    ))

    // Ignore all 'ResumeReading' messages.
    ignoreMsg {
      case ResumeReading => true
      case _ => false
    }

    receiveWhile(10 milliseconds) {
      case msg @ _ =>
    }
  }

  test("Should do nothing if not a full message") {
    val requestHeader = FrameHeader(ProtocolFlags(Request, ProtocolVersionV4), EmptyHeaderFlags, 0, Query.opcode, 5)

    // A request header expecting a 5 byte body + 1 byte for the body.
    val partialMessage = (Codec[FrameHeader].encode(requestHeader).require.toByteVector ++ ByteVector(0x00)).toByteString

    testActorRef ! Received(partialMessage)

    queryHandlerTestProbe.expectNoMsg()
  }

  test("Should handle query message coming in two parts") {
    implicit val protocolVersion = ProtocolVersionV2
    val message = Startup()
    val bytes = message.toBytes(0, Request).get.toByteString
    testActorRef ! Received(bytes)

    val queryString = "select * from people"
    val query = Query(queryString)

    val queryMessage = query.toBytes(5, Request).get.toByteString

    val (queryMessageFirstHalf, queryMessageSecondHalf) = queryMessage.splitAt(queryMessage.length / 2)

    testActorRef ! Received(queryMessageFirstHalf)
    queryHandlerTestProbe.expectNoMsg()
    
    testActorRef ! Received(queryMessageSecondHalf)
    queryHandlerTestProbe.expectMsgPF() {
      case ProtocolMessage(Frame(_, `query`)) => true
    }
  }

  test("Should handle two cql messages in the same data message") {
    implicit val protocolVersion = ProtocolVersionV4
    val startupMessage = Startup()
    val queryMessage = Query("select * from people")

    val twoMessages = startupMessage.toBytes(0, Request).get ++ queryMessage.toBytes(1, Request).get

    testActorRef ! Received(twoMessages.toByteString)

    queryHandlerTestProbe.expectMsgPF() {
      case ProtocolMessage(Frame(_, `queryMessage`)) => true
    }
  }

  test("Should send unsupported version if unknown protocol version") {
    implicit val protocolVersion = UnsupportedProtocolVersion(5)
    val startupMessage = Startup()

    testActorRef ! Received(startupMessage.toBytes(0, Request).get.toByteString)

    receiveOne(3 seconds) match {
      case Write(bytes, _) => {
        val frame = Codec[Frame].decodeValue(bytes.toByteVector.bits).require
        frame match {
          case Frame(_, ProtocolError("Invalid or unsupported protocol version")) => ()
          case x => fail(s"Received unexpected message $x instead of ProtocolError")
        }
      }
      case x => fail(s"Received $x instead of a Write")
    }
  }
}
