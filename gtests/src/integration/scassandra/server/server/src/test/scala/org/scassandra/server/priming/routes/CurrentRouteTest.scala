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

import akka.actor.ActorRef
import akka.testkit.TestActor.{AutoPilot, KeepRunning}
import akka.testkit.TestProbe
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{FunSpec, Matchers}
import org.scassandra.server.actors._
import org.scassandra.server.priming.json.PrimingJsonImplicits
import spray.testkit.ScalatestRouteTest

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

class CurrentRouteTest extends FunSpec with ScalatestRouteTest with CurrentRoute with Matchers with LazyLogging {
  override implicit def actorRefFactory = system

  import PrimingJsonImplicits._

  val _1connections = ClientConnection("127.0.0.1", 9042) :: ClientConnection("127.0.0.1", 9043) :: Nil
  val _2connections = ClientConnection("127.0.0.2", 9044) :: Nil
  val allConnections = _1connections ++ _2connections

  val all = (c: ClientConnection) => true
  val filterHost = (hOpt: Option[String]) => hOpt.map((h: String) => (c: ClientConnection) => c.host == h).getOrElse(all)
  val filterPort = (pOpt: Option[Int]) => pOpt.map((p: Int) => (c: ClientConnection) => c.port == p).getOrElse(all)
  val filter = (hOpt: Option[String], pOpt: Option[Int]) => allConnections.filter(filterHost(hOpt)).filter(filterPort(pOpt))

  lazy val serverActor = TestProbe()
  serverActor.setAutoPilot(new AutoPilot {
    override def run(sender: ActorRef, msg: Any): AutoPilot = msg match {
      case GetClientConnections(host, port) =>
        sender ! ClientConnections(filter(host, port))
        KeepRunning
      case c: SendCommandToClient =>
        sender ! ClosedConnections(filter(c.host, c.port), c.description)
        KeepRunning
      case AcceptNewConnections =>
        sender ! AcceptNewConnectionsEnabled(false)
        KeepRunning
      case RejectNewConnections(x) =>
        sender ! RejectNewConnectionsEnabled(true)
        KeepRunning
    }
  })

  override implicit val tcpServer: ActorRef = serverActor.ref
  override implicit val timeout = Timeout(5 seconds)
  override implicit val dispatcher = ExecutionContext.Implicits.global

  it("Should get all client connections") {
    Get("/current/connections") ~> currentRoute ~> check {
      val response = responseAs[ClientConnections]
      response.connections should equal (allConnections)
    }
  }

  it("Should get all client connections by ip") {
    Get("/current/connections/127.0.0.1") ~> currentRoute ~> check {
      val response = responseAs[ClientConnections]
      response.connections should equal (_1connections)
    }
  }

  it("Should get connection by ip and port") {
    Get("/current/connections/127.0.0.1/9042") ~> currentRoute ~> check {
      val response = responseAs[ClientConnections]
      response.connections should equal (_1connections.filter(_.port == 9042))
    }
  }

  it("Should close all client connections") {
    Delete("/current/connections") ~> currentRoute ~> check {
      val response = responseAs[ClosedConnections]
      response.connections should equal (allConnections)
      response.operation should equal ("close")
    }
  }

  it("Should halfclose all client connections") {
    Delete("/current/connections?type=halfclose") ~> currentRoute ~> check {
      val response = responseAs[ClosedConnections]
      response.connections should equal (allConnections)
      response.operation should equal ("halfclose")
    }
  }

  it("Should reset all client connections") {
    Delete("/current/connections?type=reset") ~> currentRoute ~> check {
      val response = responseAs[ClosedConnections]
      response.connections should equal (allConnections)
      response.operation should equal ("reset")
    }
  }

  it("Should close client connections by ip") {
    Delete("/current/connections/127.0.0.1") ~> currentRoute ~> check {
      val response = responseAs[ClosedConnections]
      response.connections should equal (_1connections)
      response.operation should equal ("close")
    }
  }

  it("Should close client connection by ip and port") {
    Delete("/current/connections/127.0.0.1/9042") ~> currentRoute ~> check {
      val response = responseAs[ClosedConnections]
      response.connections should equal (_1connections.filter(_.port == 9042))
      response.operation should equal ("close")
    }
  }

  it("Should enable listening.") {
    Put("/current/listener") ~> currentRoute ~> check {
      val response = responseAs[AcceptNewConnectionsEnabled]
      response.changed should equal(false)
    }
  }

  it("Should disable listening.") {
    Delete("/current/listener") ~> currentRoute ~> check {
      val response = responseAs[RejectNewConnectionsEnabled]
      response.changed should equal(true)
    }
  }
}
