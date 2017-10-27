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
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import org.scalatest.{FunSuiteLike, Matchers}
import org.scassandra.codec.{Ready, Register}
import org.scassandra.server.RegisterHandler

class RegisterHandlerTest extends TestKit(ActorSystem("TestSystem")) with ProtocolActorTest with ImplicitSender with FunSuiteLike with Matchers {
  test("Should send Ready message on any Register message") {
    val underTest = TestActorRef(new RegisterHandler)

    underTest ! protocolMessage(Register("TOPOLOGY_CHANGE" :: Nil))

    expectMsgPF() {
      case ProtocolResponse(_, Ready) => true
    }
  }
}
