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
package org.scassandra.server

import akka.actor.{ActorRef, Actor}
import com.typesafe.scalalogging.LazyLogging

class ServerReadyListener extends Actor with LazyLogging {
  var serverReadyReceiver: ActorRef = null
  var alreadyReady = false

  def receive = {
        case OnServerReady =>
          serverReadyReceiver = sender
          if (alreadyReady) {
            logger.info("OnServerReady - ServerReady message already received. Sending ServerReady.")
            serverReadyReceiver ! ServerReady
          }
        case ServerReady =>
          if (serverReadyReceiver != null) {
            logger.info("ServerReady - Forwarding ServerReady to listener.")
            serverReadyReceiver ! ServerReady
          } else {
            logger.info("ServerReady - No listener yet.")
            alreadyReady = true
          }
        case msg @ _ => logger.info(s"Received unknown message $msg")
  }
}

case object ServerReady

case object OnServerReady
