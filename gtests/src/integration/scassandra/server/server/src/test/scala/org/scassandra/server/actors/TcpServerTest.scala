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

import java.net.InetSocketAddress

import akka.actor._
import akka.io.Tcp._
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import akka.util.Timeout
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, FunSuiteLike, Matchers}
import org.scassandra.server.ServerReadyListener
import org.scassandra.server.priming.ActivityLog
import org.scassandra.server.priming.batch.PrimeBatchStore
import org.scassandra.server.priming.prepared.PrimePreparedStore
import org.scassandra.server.priming.query.PrimeQueryStore

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, _}
import scala.language.postfixOps

class TcpServerTest extends TestKit(ActorSystem("TcpServerTest")) with Matchers with ImplicitSender with FunSuiteLike with BeforeAndAfter with BeforeAndAfterAll {

  implicit val atMost: Duration = 1 seconds
  implicit val timeout: Timeout = 1 seconds

  val _1addresses = Set(60000, 60001, 60002).map(new InetSocketAddress("127.0.0.1", _))
  val _2addresses = Set(60000, 62555).map(new InetSocketAddress("127.0.0.2", _))
  val _3addresses = Set(60000, 1665).map(new InetSocketAddress("127.0.0.3", _))
  val allAddresses = _1addresses ++ _2addresses ++ _3addresses

  var manager: TestProbe = _
  var tcpConnection: TestProbe = _
  var underTest: TestActorRef[TcpServer] = _
  val activityLog = new ActivityLog
  val remote = new InetSocketAddress("127.0.0.1", 8047)

  before {
    manager = TestProbe()
    tcpConnection = TestProbe()
    underTest = TestActorRef(new TcpServer("localhost", 8047, new PrimeQueryStore, new PrimePreparedStore, new PrimeBatchStore, system.actorOf(Props(classOf[ServerReadyListener])), activityLog, Some(manager.ref)))
    val remote = new InetSocketAddress("127.0.0.1", 8047)

    manager.expectMsgType[Bind]
    manager.send(underTest, Bound(remote))
    manager.expectMsg(ResumeAccepting(1))

    allAddresses.foreach { local =>
      tcpConnection.send(underTest, Connected(local, remote))
      expectRegister()
      manager.expectMsg(ResumeAccepting(1))
    }
  }

  private def expectRegister() = {
    var gotRegister = false
    var gotResumeReading = false
    tcpConnection.receiveN(2).foreach {
      case x: Register if gotRegister => fail("Got multiple Register messages")
      case x: Register => gotRegister = true
      case ResumeReading if gotResumeReading => fail("Got multiple ResumeReading messages")
      case ResumeReading => gotResumeReading = true
      case x => fail(s"Got unknown message $x")
    }
  }

  after {
    underTest.stop()
  }

  override def afterAll() {
    system.shutdown()
  }

  def assertResponse(request: Any, expectedAddresses: Set[InetSocketAddress]): Unit = {
    val future = (underTest ? request).mapTo[ClientConnections]

    Await.result(future, atMost).connections should contain theSameElementsAs expectedAddresses.map(a => ClientConnection(a.getAddress.getHostAddress, a.getPort))
  }

  def assertCommandResponse(request: SendCommandToClient, expectedAddresses: Set[InetSocketAddress]): Unit = {
    val future = (underTest ? request).mapTo[ClosedConnections]

    // Expect a close command for each address and respond to it.
    expectedAddresses.foreach { a =>
      val command = tcpConnection.expectMsg(request.command)
      tcpConnection.reply(command.event)
    }

    Await.result(future, atMost).connections should contain theSameElementsAs expectedAddresses.map(a => ClientConnection(a.getAddress.getHostAddress, a.getPort))
  }

  test("Should record connections with the ActivityLog") {
    activityLog.retrieveConnections().size should equal(allAddresses.size)
  }

  test("Should retrieve all connections") {
    assertResponse(GetClientConnections(), allAddresses)
  }

  test("Should close all connections") {
    assertCommandResponse(CloseClientConnections(), allAddresses)
  }

  test("Should confirm close one connection") {
    assertCommandResponse(ConfirmedCloseClientConnections(Some("127.0.0.1"), Some(60002)), _1addresses.filter(_.getPort == 60002))
  }

  test("Should reset specific connections by address") {
    assertCommandResponse(ResetClientConnections(Some("127.0.0.2")), _2addresses)
  }

  test("Should retrieve all connections by address") {
    assertResponse(GetClientConnections(Some("127.0.0.1")), _1addresses)
  }

  test("Should retrieve all connections by port") {
    assertResponse(GetClientConnections(None, Some(60000)), allAddresses.filter(_.getPort == 60000))
  }

  test("Should retrieve specific connection by address and port") {
    assertResponse(GetClientConnections(Some("127.0.0.3"), Some(60000)), _3addresses.filter(_.getPort == 60000))
  }

  test("Should retrieve empty connections if nothing matches") {
    assertResponse(GetClientConnections(Some("127.0.0.7")), Set())
    assertResponse(GetClientConnections(None, Some(1234)), Set())
    assertResponse(GetClientConnections(Some("127.0.0.7"), Some(1234)), Set())
  }

  test("Should disable accepting new connections if enabled") {
    val future = (underTest ? RejectNewConnections()).mapTo[RejectNewConnectionsEnabled]

    Await.result(future, atMost).changed should equal (true)
    manager.expectMsg(ResumeAccepting(0))

    val future2 = (underTest ? RejectNewConnections()).mapTo[RejectNewConnectionsEnabled]
    Await.result(future2, atMost).changed should equal (false)
    manager.expectNoMsg()
  }

  test("Should disable accepting new connections after count") {
    val future = (underTest ? RejectNewConnections(2)).mapTo[RejectNewConnectionsEnabled]

    Await.result(future, atMost).changed should equal (true)
    manager.expectMsg(ResumeAccepting(1))

    tcpConnection.send(underTest, Connected(new InetSocketAddress("127.0.0.4", 1000), remote))
    expectRegister()
    manager.expectMsg(ResumeAccepting(1))
    tcpConnection.send(underTest, Connected(new InetSocketAddress("127.0.0.4", 1001), remote))
    expectRegister()
    // After two new connections should indicate to stop accepting new ones.
    manager.expectMsg(ResumeAccepting(0))
  }

  test("Should enable accepting new connections if disabled") {
    val future = (underTest ? RejectNewConnections()).mapTo[RejectNewConnectionsEnabled]

    Await.result(future, atMost).changed should equal (true)
    manager.expectMsg(ResumeAccepting(0))

    val future2 = (underTest ? AcceptNewConnections).mapTo[AcceptNewConnectionsEnabled]
    Await.result(future2, atMost).changed should equal (true)
    manager.expectMsg(ResumeAccepting(1))

    val future3 = (underTest ? AcceptNewConnections).mapTo[AcceptNewConnectionsEnabled]
    Await.result(future3, atMost).changed should equal (false)
    manager.expectNoMsg()
  }
}
