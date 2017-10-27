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
package org.scassandra.server.priming.routes

import akka.actor.{ActorContext, ActorRef}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import org.scassandra.server.actors._
import spray.routing.HttpService

import org.scassandra.server.priming.json._
import org.scassandra.server.priming.cors.CorsSupport

import scala.concurrent.{ExecutionContext, Future}

trait CurrentRoute extends HttpService with LazyLogging with CorsSupport {

  val tcpServer: ActorRef

  implicit val dispatcher: ExecutionContext
  implicit val timeout: Timeout

  import PrimingJsonImplicits._

  val currentRoute = pathPrefix("current") {
    cors {
      path("connections" ~ Slash.? ~ Segment.? ~ Slash.? ~ IntNumber.?) { (host: Option[String], port: Option[Int]) =>
        get {
          complete {
            (tcpServer ? GetClientConnections(host, port)).mapTo[ClientConnections]
          }
        } ~
        delete {
          parameters('type.as[String] ? "close") { closeType =>
            val request = closeType match {
              case "reset" => ResetClientConnections.apply _
              case "halfclose" => ConfirmedCloseClientConnections.apply _
              case _ => CloseClientConnections.apply _
            }

            complete((tcpServer ? request(host, port)).mapTo[ClosedConnections])
          }
        }
      } ~
      path("listener") {
        put {
          complete((tcpServer ? AcceptNewConnections).mapTo[AcceptNewConnectionsEnabled])
        } ~
        delete {
          parameters('after.as[Int] ? 0) { after =>
            complete((tcpServer ? RejectNewConnections(after)).mapTo[RejectNewConnectionsEnabled])
          }
        }
      }
    }
  }

}
