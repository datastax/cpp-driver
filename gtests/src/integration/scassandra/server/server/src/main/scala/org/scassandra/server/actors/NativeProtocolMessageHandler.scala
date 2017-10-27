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

import akka.actor.{ActorRef, ActorRefFactory}
import org.scassandra.codec._

/*
 * Expects full native protocol messages.
 */
class NativeProtocolMessageHandler(queryHandlerFactory: (ActorRefFactory) => ActorRef,
                        batchHandlerFactory: (ActorRefFactory, ActorRef) => ActorRef,
                        registerHandlerFactory: (ActorRefFactory) => ActorRef,
                        optionsHandlerFactory: (ActorRefFactory) => ActorRef,
                        prepareHandler: ActorRef,
                        executeHandler: ActorRef) extends ProtocolActor {

  private val optionsHandler = optionsHandlerFactory(context)

  override def receive: Receive = {
    case message@ProtocolMessage(frame) =>
      frame.message match {
        case Options =>
          log.debug("Received options message. Sending to OptionsHandler")
          optionsHandler forward message
        case Startup(_) =>
          val queryHandler = queryHandlerFactory(context)
          val batchHandler = batchHandlerFactory(context, prepareHandler)
          val registerHandler = registerHandlerFactory(context)
          write(Ready, frame.header)
          context become initialized(queryHandler, batchHandler, registerHandler)
        case _ =>
          log.error(s"Received message $frame before Startup.  Sending error.")
          write(ProtocolError("Query sent before Startup message"), frame.header)
      }
  }

  def initialized(queryHandler: ActorRef, batchHandler: ActorRef, registerHandler: ActorRef): Receive = {
    case message@ProtocolMessage(frame) =>
      frame.message match {
        case query: Query =>
          queryHandler forward message
        case batch: Batch =>
          log.debug("Received batch message.  Sending to BatchHandler")
          batchHandler forward message
        case register: Register =>
          log.debug("Received register message. Sending to RegisterHandler")
          registerHandler forward message
        case Options =>
          log.debug("Received options message. Sending to OptionsHandler")
          optionsHandler forward message
        case prepare: Prepare =>
          log.debug("Received prepare message. Sending to PrepareHandler")
          prepareHandler forward message
        case execute: Execute =>
          log.debug("Received execute message. Sending to ExecuteHandler")
          executeHandler forward message
        case _ =>
          log.warning(s"Received currently unhandled message ${frame.message}, discarding.")
      }
  }
}
