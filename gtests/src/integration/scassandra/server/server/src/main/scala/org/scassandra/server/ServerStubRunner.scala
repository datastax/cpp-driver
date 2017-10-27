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

import akka.actor._
import akka.pattern.{ask, gracefulStop}
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import org.scassandra.server.priming.query.PrimeQueryStore

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

object ServerStubRunner extends LazyLogging {
  def main(args: Array[String]) {
    val binaryListenAddress = ScassandraConfig.binaryListenAddress
    val binaryPortNumber = ScassandraConfig.binaryPort
    val adminListenAddress = ScassandraConfig.adminListenAddress
    val adminPortNumber = ScassandraConfig.adminPort
    logger.info(s"Using binary port to $binaryPortNumber and admin port to $adminPortNumber")
    val ss = new ServerStubRunner(binaryListenAddress, binaryPortNumber, adminListenAddress, adminPortNumber, ScassandraConfig.startupTimeout)
    ss.start()

    // wait indefinitely or until interrupted.
    val obj = new Object()
    obj.synchronized { obj.wait(); }
  }

  lazy val actorSystem: ActorSystem = {
    ActorSystem(s"Scassandra")
  }
}

/**
 * Constructor used by the Java Client so not using any Scala types like Duration.
 */
class ServerStubRunner( val binaryListenAddress: String = "localhost",
                        val binaryPortNumber: Int = 8042,
                        val adminListenAddress: String = "localhost",
                        val adminPortNumber: Int = 8043,
                        val startupTimeoutSeconds: Long = 10) extends LazyLogging {
  import ServerStubRunner.actorSystem

  import ExecutionContext.Implicits.global

  // TODO: This is only used by integration tests, move into Actor.
  val primedResults = new PrimeQueryStore()

  var scassandra: ActorRef = _

  def start() = this.synchronized {
    scassandra = actorSystem.actorOf(Props(classOf[ScassandraServer], primedResults, binaryListenAddress,
      binaryPortNumber, adminListenAddress, adminPortNumber))
  }

  def shutdown() = this.synchronized {
    val stopped = gracefulStop(scassandra, startupTimeoutSeconds + 1 seconds, ShutdownServer(startupTimeoutSeconds seconds))
    Await.result(stopped, startupTimeoutSeconds + 1 seconds)
  }

  def awaitStartup() = this.synchronized {
    implicit val timeout: Timeout = startupTimeoutSeconds seconds
    val startup = (scassandra ? AwaitStartup(timeout))
    startup.onFailure { case t: Throwable =>
      logger.error("Failure or timeout starting server", t)
    }
    Await.result(startup, startupTimeoutSeconds + 1 seconds)
  }
}

