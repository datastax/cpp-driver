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
import akka.io.{IO, Tcp}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import org.scassandra.server.priming.ActivityLog
import org.scassandra.server.priming.batch.PrimeBatchStore
import org.scassandra.server.priming.prepared.PreparedStoreLookup
import org.scassandra.server.priming.query.PrimeQueryStore
import org.scassandra.server.{RegisterHandler, ServerReady, Shutdown}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

class TcpServer(listenAddress: String, port: Int,
                primedResults: PrimeQueryStore,
                primePrepareStore: PreparedStoreLookup,
                primeBatchStore: PrimeBatchStore,
                serverReadyListener: ActorRef,
                activityLog: ActivityLog,
                manager : Option[ActorRef]) extends Actor with ActorLogging {

  def this(listenAddress: String, port: Int,
           primedResults: PrimeQueryStore,
           primePrepareStore: PreparedStoreLookup,
           primeBatchStore: PrimeBatchStore,
           serverReadyListener: ActorRef,
           activityLog: ActivityLog) {
    this(listenAddress, port, primedResults, primePrepareStore, primeBatchStore, serverReadyListener, activityLog, None)
  }

  import akka.io.Tcp._
  import context.{dispatcher, system}

  // whether or not to accept connections.
  var acceptConnections = true

  // In how many new connections to start disabling accepting new connections.  Don't disable if < 0
  var disableCounter = -1

  implicit val timeout: Timeout = 10 seconds

  val AddressRE = "(.*):(\\d+)$".r

  val preparedHandler = context.actorOf(Props(classOf[PrepareHandler], primePrepareStore, activityLog))
  val executeHandler = context.actorOf(Props(classOf[ExecuteHandler], primePrepareStore, activityLog, preparedHandler))

  override def preStart(): Unit = {
    manager.getOrElse(IO(Tcp)) ! Bind(self, new InetSocketAddress(listenAddress, port), pullMode=true)
  }

  def receive = {
    case b @ Bound(localAddress) =>
      log.info(s"Port $port ready for Cassandra binary connections. ${sender()}")
      serverReadyListener ! ServerReady
      sender ! ResumeAccepting(batchSize = 1)
      context.become(listening(sender()))

    case CommandFailed(_: Bind) =>
      log.error(s"Unable to bind to port $port for Cassandra binary connections. Is it in use?")
      context stop self
      context.system.shutdown()
  }

  def listening(listener: ActorRef): Receive = {
    case c @ Connected(remote, local) =>
      activityLog.recordConnection()
      val handler = context.actorOf(Props(classOf[ConnectionHandler], sender(),
        (af: ActorRefFactory) => af.actorOf(Props(classOf[QueryHandler], primedResults, activityLog)),
        (af: ActorRefFactory, prepareHandler: ActorRef) => af.actorOf(Props(classOf[BatchHandler],
          activityLog, prepareHandler, primeBatchStore, primePrepareStore)),
        (af: ActorRefFactory) => af.actorOf(Props(classOf[RegisterHandler])),
        (af: ActorRefFactory) => af.actorOf(Props(classOf[OptionsHandler])),
        preparedHandler,
        executeHandler),
      name = s"${remote.getAddress.getHostAddress}:${remote.getPort}")
      log.debug(s"Sending register with connection handler $handler")
      sender ! Register(handler)

      if(!acceptConnections) {
        log.warning(s"Got $c but did not expect a new connection since accepting connections is disabled.")
      }

      // Decrement state change counter and check whether or not we should keep listening enabled or not.
      disableCounter -= 1
      checkEnablement(listener)

    case GetClientConnections(hostOption, portOption) => {
      val matchingClients = findChildren(hostOption, portOption).map(actorToConnection)
      val response = ClientConnections(matchingClients)
      sender ! response
    }

    case c: SendCommandToClient => {
      val children = findChildren(c.host, c.port)

      // Close each found connection.
      val responses = children.map { child =>
        (child ? c.command).mapTo[Event].map { c => child }
      }

      // Once all connections closed map to ClientConnections response
      Future.sequence(responses).map(actor => ClosedConnections(actor.map(actorToConnection), c.description)).pipeTo(sender())
    }

    case AcceptNewConnections => {
      val response = AcceptNewConnectionsEnabled(!acceptConnections)
      disableCounter = -1
      if(!acceptConnections) {
        acceptConnections = true
        log.debug("toggling to enable new connections.")
        checkEnablement(listener)
      }
      sender ! response
    }

    case RejectNewConnections(after) => {
      val response = RejectNewConnectionsEnabled(acceptConnections)
      if(acceptConnections) {
        disableCounter = after
        checkEnablement(listener)
      }
      sender ! response
    }
    case Shutdown => {
      val requestor = sender
      context.become {
        case u@Unbound => {
          requestor ! u
          // Kill self after unbound
          self ! PoisonPill
        }
      }
      listener ! Unbind
    }

  }

  private def checkEnablement(listener: ActorRef) = {
    if(acceptConnections) {
      if(disableCounter == 0) {
        log.debug("toggling to disable new connections.")
        listener ! ResumeAccepting(batchSize = 0)
        acceptConnections = false
      } else {
        listener ! ResumeAccepting(batchSize = 1)
      }
    }
  }

  private def actorToConnection(actor: ActorRef): ClientConnection = actor.path.name match {
    case AddressRE(address, addressPort) => {
      ClientConnection(address, addressPort.toInt)
    }
  }

  private def findChildren(hostOption: Option[String], portOption: Option[Int]): List[ActorRef] = {
    val validClients = context.children.collect {
      case actor: ActorRef if actor.path.name.matches(AddressRE.regex) => actor
    }.toList

    def getAddress(actor: ActorRef): (String, Int) = {
      actor.path.name match {
        case AddressRE(address, addressPort) => {
          (address, addressPort.toInt)
        }
      }
    }

    (hostOption, portOption) match {
      case (Some(host), Some(exPort)) => validClients.find{c => val x = getAddress(c); x._1 == host && x._2 == exPort}.toList
      case (Some(host), None) => validClients.filter{c => val x = getAddress(c); x._1 == host}
      case (None, Some(exPort)) => validClients.filter{c => val x = getAddress(c); x._2 == exPort}
      case (None, None) => validClients
    }
  }
}

case class GetClientConnections(host: Option[String] = None, port: Option[Int] = None)
case class ClientConnection(host: String, port: Int)

trait Connections {
  val connections: List[ClientConnection]
}

case class ClientConnections(connections: List[ClientConnection]) extends Connections
case class ClosedConnections(connections: List[ClientConnection], operation: String) extends Connections

trait SendCommandToClient {
  val host: Option[String]
  val port: Option[Int]
  val command: Tcp.CloseCommand
  val description: String
}

case class CloseClientConnections(host: Option[String] = None, port: Option[Int] = None) extends SendCommandToClient {
  override val command = Tcp.Close
  override val description = "close"
}

case class ResetClientConnections(host: Option[String] = None, port: Option[Int] = None) extends SendCommandToClient {
  override val command = Tcp.Abort
  override val description = "reset"
}

case class ConfirmedCloseClientConnections(host: Option[String] = None, port: Option[Int] = None) extends SendCommandToClient {
  override val command = Tcp.ConfirmedClose
  override val description = "halfclose"
}

case class RejectNewConnections(after: Int = 0)
case object AcceptNewConnections
case class RejectNewConnectionsEnabled(changed: Boolean)
case class AcceptNewConnectionsEnabled(changed: Boolean)
