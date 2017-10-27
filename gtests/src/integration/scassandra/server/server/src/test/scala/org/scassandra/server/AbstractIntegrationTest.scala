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

import java.net.{ConnectException, Socket}
import java.util.concurrent.TimeUnit

import com.datastax.driver.core._
import com.datastax.driver.core.policies.FallthroughRetryPolicy
import dispatch.Defaults._
import dispatch._
import io.netty.channel.EventLoopGroup
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite, Matchers}
import org.scassandra.codec.datatype.DataType
import org.scassandra.server.actors._
import org.scassandra.server.priming._
import org.scassandra.server.priming.json._
import org.scassandra.server.priming.prepared.{PrimePreparedSingle, ThenPreparedSingle, WhenPrepared}
import org.scassandra.server.priming.query.{PrimeQuerySingle, Then, When}
import spray.json._

object PrimingHelper {

  import PrimingJsonImplicits._
  val DefaultHost = "http://localhost:8043/"

  def primeQuery(when: When, rows: List[Map[String, Any]], result: ResultJsonRepresentation = Success, columnTypes: Map[String, DataType] = Map()): String = {
    primeQuery(when, Then(Some(rows), Some(result), Some(columnTypes)))
  }

  def primeQuery(when: When, thenDo: Then) : String = {
    val prime = PrimeQuerySingle(when, thenDo).toJson
    println("Sending JSON: " + prime.toString)
    val svc = url(s"${DefaultHost}prime-query-single") <<
      prime.toString() <:<
      Map("Content-Type" -> "application/json")

    val response = Http(svc OK as.String)
    response()
  }

  def primePreparedStatement(query: WhenPrepared, thenDo: ThenPreparedSingle) = {
    val prime = PrimePreparedSingle(query, thenDo).toJson
    println("Sending JSON: " + prime.toString)
    val svc = url(s"${DefaultHost}prime-prepared-single") <<
      prime.toString <:<
      Map("Content-Type" -> "application/json")

    val response = Http(svc OK as.String)
    response()
  }

  def getRecordedPreparedStatements() = {
    val svc = url(s"${DefaultHost}prepared-statement-execution")
    val response = Http(svc OK as.String)
    val body: String = response()
    println("Recorded prepared statement executions:"  + body)
    JsonParser(body).convertTo[List[PreparedStatementExecution]]
  }

  def getRecordedQueries(): List[Query] = {
    val svc = url(s"${DefaultHost}query")
    val response = Http(svc OK as.String)
    val body: String = response()
    println("Recorded queries executions:"  + body)
    JsonParser(body).convertTo[List[Query]]
  }

  def getRecordedBatchExecutions(): List[BatchExecution] = {
    val svc = url(s"${DefaultHost}batch-execution")
    val response = Http(svc OK as.String)
    val body: String = response()
    JsonParser(body).convertTo[List[BatchExecution]]
  }

  def clearQueryPrimes(): String = {
    val svc = url(s"${DefaultHost}prime-query-single").DELETE
    Http(svc OK as.String)(dispatch.Defaults.executor)()
  }

  def clearRecordedQueries(): String = {
    val svc = url(s"${DefaultHost}query").DELETE
    Http(svc OK as.String)(dispatch.Defaults.executor)()
  }

  def clearRecordedBatchExecutions(): String = {
    val svc = url(s"${DefaultHost}batch-execution").DELETE
    Http(svc OK as.String)(dispatch.Defaults.executor)()
  }

  def enableListener(): Boolean = {
    val svc = url(s"${DefaultHost}current/listener").PUT
    val response = Http(svc OK as.String)(dispatch.Defaults.executor)()
    JsonParser(response).convertTo[AcceptNewConnectionsEnabled].changed
  }

  def disableListener(after: Int = 0): Boolean = {
    val svc = url(s"${DefaultHost}current/listener?after=$after").DELETE
    val response = Http(svc OK as.String)(dispatch.Defaults.executor)()
    JsonParser(response).convertTo[RejectNewConnectionsEnabled].changed
  }

  def getConnections(): List[ClientConnection] = {
    val svc = url(s"${DefaultHost}current/connections").GET
    val response = Http(svc OK as.String)(dispatch.Defaults.executor)()
    JsonParser(response).convertTo[ClientConnections].connections
  }

  def closeConnection(connection: ClientConnection, operation: String): ClosedConnections = {
    val svc = url(s"${DefaultHost}current/connections/${connection.host}/${connection.port}?type=$operation").DELETE
    val response = Http(svc OK as.String)(dispatch.Defaults.executor)()
    JsonParser(response).convertTo[ClosedConnections]
  }
}

abstract class AbstractIntegrationTest(clusterConnect: Boolean = true) extends FunSuite with Matchers with BeforeAndAfter with BeforeAndAfterAll {
  var serverThread: ServerStubRunner = null

  val closeQuickly = new NettyOptions() {
    override def onClusterClose(eventLoopGroup: EventLoopGroup) {
      //Shutdown immediately, since we close cluster when finished, we know nothing new coming through.
      eventLoopGroup.shutdownGracefully(0, 1000, TimeUnit.MILLISECONDS).syncUninterruptibly
    }
  }
  var cluster: Cluster = _
  var session: Session = _

  def prime(when: When, rows: List[Map[String, Any]], result: ResultJsonRepresentation = Success, columnTypes: Map[String, DataType] = Map()) = {
    PrimingHelper.primeQuery(when, rows, result, columnTypes)
  }

  def prime(when: When, thenDo: Then) = {
    PrimingHelper.primeQuery(when, thenDo)
  }

  def startServerStub() = {
    serverThread = new ServerStubRunner()
    serverThread.start()
    serverThread.awaitStartup()
  }

  def stopServerStub() = {
    serverThread.shutdown()
    Thread.sleep(3000)
  }

  def priming() = {
    serverThread.primedResults
  }

  def builder() = {
    Cluster.builder()
      .addContactPoint(ConnectionToServerStub.ServerHost)
      .withPort(ConnectionToServerStub.ServerPort)
      .withRetryPolicy(FallthroughRetryPolicy.INSTANCE)
      .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE))
      .withNettyOptions(closeQuickly)
  }

  override def beforeAll() {
    println("Trying to start server")
    // First ensure nothing else is running on the port we are trying to connect to
    var somethingAlreadyRunning = true

    try {
      ConnectionToServerStub()
      println(s"Succesfully connected to ${ConnectionToServerStub.ServerHost}:${ConnectionToServerStub.ServerPort}. There must be something running.")
    } catch {
      case ce: ConnectException =>
        println(s"No open connection found on ${ConnectionToServerStub.ServerHost}:${ConnectionToServerStub.ServerPort}. Starting the server.")
        somethingAlreadyRunning = false

    }

    if (somethingAlreadyRunning) {
      fail("There must be a server already running")
    }

    // Then start the server
    startServerStub()

    if (clusterConnect) {
      cluster = builder().build()
      session = cluster.connect("mykeyspace")
    }
  }

  override def afterAll() {
    stopServerStub()

    if(cluster != null) {
      cluster.close()
    }
  }
}

object ConnectionToServerStub {
  val ServerHost = "localhost"
  val ServerPort = 8042

  def apply() = {
    val socket = new Socket(ServerHost, ServerPort)
    socket.setSoTimeout(1000)
    socket
  }
}
