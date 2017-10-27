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
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{BeforeAndAfter, FunSpecLike, Matchers}
import org.scassandra.codec._
import org.scassandra.codec.messages.{BatchType, SimpleBatchQuery}
import scodec.bits.ByteVector

import scala.concurrent.duration._
import scala.language.postfixOps

class NativeProtocolMessageHandlerTest extends TestKit(ActorSystem("NativeProtocolMessageHandlerTest"))
  with ProtocolActorTest with Matchers with ImplicitSender with FunSpecLike with BeforeAndAfter with TableDrivenPropertyChecks {

  var testActorRef: TestActorRef[NativeProtocolMessageHandler] = null
  var queryHandlerTestProbe: TestProbe = null
  var batchHandlerTestProbe: TestProbe = null
  var registerHandlerTestProbe: TestProbe = null
  var optionsHandlerTestProbe: TestProbe = null
  var prepareHandlerTestProbe: TestProbe = null
  var executeHandlerTestProbe: TestProbe = null

  before {
    queryHandlerTestProbe = TestProbe()
    registerHandlerTestProbe = TestProbe()
    prepareHandlerTestProbe = TestProbe()
    executeHandlerTestProbe = TestProbe()
    optionsHandlerTestProbe = TestProbe()
    batchHandlerTestProbe = TestProbe()
    testActorRef = TestActorRef(new NativeProtocolMessageHandler(
      (_) => queryHandlerTestProbe.ref,
      (_,_) => batchHandlerTestProbe.ref,
      (_) => registerHandlerTestProbe.ref,
      (_) => optionsHandlerTestProbe.ref,
      prepareHandlerTestProbe.ref,
      executeHandlerTestProbe.ref
    ))
  }

  val protocolVersions = Table("Protocol Version", ProtocolVersion.versions:_*)

  describe("NativeProtocolMessageHandler") {
    forAll (protocolVersions) { (protocolVersion: ProtocolVersion) =>
      implicit val protocol: ProtocolVersion = protocolVersion

      def sendStartup() = {
        testActorRef ! protocolMessage(Startup())

        expectMsgPF() {
          case ProtocolResponse(_, Ready) => true
        }
      }

      describe("with " + protocolVersion + " messages") {
        it("should send ready message when startup message sent") {
          testActorRef ! protocolMessage(Startup())

          expectMsgPF() {
            case ProtocolResponse(_, Ready) => true
          }
        }

        it("should send back error if query before ready message") {
          testActorRef ! protocolMessage(Query("select * from system.local"))

          expectMsgPF() {
            case ProtocolResponse(_, ProtocolError("Query sent before Startup message")) => true
          }
        }

        it("should do nothing if an unrecognised opcode") {
          sendStartup()

          testActorRef ! protocolMessage(Supported())

          expectNoMsg(100 millisecond)
        }

        it("should forward Query to QueryHandler") {
          sendStartup()

          val msg = protocolMessage(Query("select * from system.local"))
          testActorRef ! msg

          queryHandlerTestProbe.expectMsg(msg)
        }

        it("should forward Register to RegisterHandler") {
          sendStartup()

          val msg = protocolMessage(Register("TOPOLOGY_CHANGE" :: Nil))
          testActorRef ! msg

          registerHandlerTestProbe.expectMsg(msg)
        }

        it("should allow Options messages before Startup") {
          val msg = protocolMessage(Options)
          testActorRef ! msg

          optionsHandlerTestProbe.expectMsg(msg)
        }

        it("should forward Options to OptionsHandler") {
          sendStartup()

          val msg = protocolMessage(Options)
          testActorRef ! msg

          optionsHandlerTestProbe.expectMsg(msg)
        }


        it("should forward Prepare to PrepareHandler") {
          sendStartup()

          val msg = protocolMessage(Prepare("select * from system.local"))
          testActorRef ! msg

          prepareHandlerTestProbe.expectMsg(msg)
        }

        it("should forward Execute to ExecuteHandler") {
          sendStartup()

          val msg = protocolMessage(Execute(ByteVector(1)))
          testActorRef ! msg

          executeHandlerTestProbe.expectMsg(msg)
        }

        it("should forward Batch to BatchHandler") {
          sendStartup()

          val msg = protocolMessage(Batch(BatchType.UNLOGGED, SimpleBatchQuery("query") :: SimpleBatchQuery("query2") :: Nil))
          testActorRef ! msg

          batchHandlerTestProbe.expectMsg(msg)
        }
      }
    }
  }
}
