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
package org.scassandra.server.e2e.verification

import com.datastax.driver.core.{Cluster, HostDistance}
import dispatch.Defaults._
import dispatch._
import org.scalatest.concurrent.ScalaFutures
import org.scassandra.server.priming.Connection
import org.scassandra.server.priming.json.PrimingJsonImplicits
import org.scassandra.server.{AbstractIntegrationTest, ConnectionToServerStub}
import spray.json.JsonParser

class ConnectionVerificationTest extends AbstractIntegrationTest with ScalaFutures {

  import PrimingJsonImplicits._

  before {
    val svc = url("http://localhost:8043/prime-query-single").DELETE
    val response = Http(svc OK as.String)
    response()

    val deleteActivityUrl = url("http://localhost:8043/connection").DELETE
    val deleteResponse = Http(deleteActivityUrl OK as.String)
    deleteResponse()
  }

  test("Test verification of connection for a single java driver") {

    val cluster = Cluster.builder()
      .addContactPoint(ConnectionToServerStub.ServerHost)
      .withPort(ConnectionToServerStub.ServerPort)
      .build()
    cluster.connect()
    val svc: Req = url("http://localhost:8043/connection")
    val response = Http(svc OK as.String)
    response()

    whenReady(response) {
      result =>
        val connectionList = JsonParser(result).convertTo[List[Connection]]
        // The java driver establishes 1 control + core connections.
        connectionList.size should equal(1 + cluster
          .getConfiguration
          .getPoolingOptions
          .getCoreConnectionsPerHost(HostDistance.LOCAL))
    }
  }

  test("Test verification of connection when there has been no connections") {
    val svc: Req = url("http://localhost:8043/connection")
    val response = Http(svc OK as.String)

    whenReady(response) {
      result =>
        val connectionList = JsonParser(result).convertTo[List[Connection]]
        connectionList.size should equal(0)
    }
  }
}
