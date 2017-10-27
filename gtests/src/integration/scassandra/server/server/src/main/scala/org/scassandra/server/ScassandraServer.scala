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

import akka.actor.{PoisonPill, Props, Actor, ActorRef}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import org.scassandra.server.actors.TcpServer
import org.scassandra.server.priming.{PrimingServer, ActivityLog}
import org.scassandra.server.priming.batch.PrimeBatchStore
import org.scassandra.server.priming.prepared.{CompositePreparedPrimeStore, PrimePreparedMultiStore, PrimePreparedPatternStore, PrimePreparedStore}
import org.scassandra.server.priming.query.PrimeQueryStore

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/**
  * Used to wait on startup of listening http and tcp interfaces.
  * @param timeout Up to how long to wait for startup before timing out.
  */
case class AwaitStartup(timeout: Timeout)

/**
  * Used to shutdown the server by first unbinding the priming and tcp server listeners
  * and then sending a {@link PoisonPill} to itself.
  * @param timeout Up to how long to wait for shutdown before timing out.
  */
case class ShutdownServer(timeout: Timeout)

/**
  * Sent to {@link PrimingServer} and {@link TcpServer} instances to indicate that they should
  * unbind their listeners and then subsequently shutdown.
  */
case object Shutdown

class ScassandraServer(val primedResults: PrimeQueryStore, val binaryListenAddress: String, val binaryPortNumber: Int, val adminListenAddress: String, val adminPortNumber: Int) extends Actor with LazyLogging {

  val primePreparedStore = new PrimePreparedStore
  val primePreparedPatternStore = new PrimePreparedPatternStore
  val primePreparedMultiStore = new PrimePreparedMultiStore
  val primeBatchStore = new PrimeBatchStore
  val activityLog = new ActivityLog
  val preparedLookup = new CompositePreparedPrimeStore(primePreparedStore, primePreparedPatternStore, primePreparedMultiStore)
  val primingReadyListener = context.actorOf(Props(classOf[ServerReadyListener]), "PrimingReadyListener")
  val tcpReadyListener = context.actorOf(Props(classOf[ServerReadyListener]), "TcpReadyListener")
  val tcpServer = context.actorOf(Props(classOf[TcpServer], binaryListenAddress, binaryPortNumber, primedResults, preparedLookup, primeBatchStore, tcpReadyListener, activityLog), "BinaryTcpListener")
  val primingServer = context.actorOf(Props(classOf[PrimingServer], adminListenAddress, adminPortNumber, primedResults, primePreparedStore,
    primePreparedPatternStore, primePreparedMultiStore, primeBatchStore, primingReadyListener, activityLog, tcpServer), "PrimingServer")

  override def receive: Receive = {
    case AwaitStartup(timeout) => {
      implicit val t: Timeout = timeout
      import context.dispatcher

      // Create a future that completes when both listeners ready.
      val f = Future.sequence(
        primingReadyListener ? OnServerReady ::
        tcpReadyListener ? OnServerReady ::
        Nil
      )

      f pipeTo sender
    }
    case ShutdownServer(timeout) => {
      implicit val t: Timeout = timeout
      import context.dispatcher

      // Send shutdown message to each sender and on complete send a PoisonPill to self.
      val f = Future.sequence(
        primingServer ? Shutdown ::
        tcpServer ? Shutdown ::
        Nil
      ).map { _ => self ? PoisonPill }

      f pipeTo sender
    }
  }
}
