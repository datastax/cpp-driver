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
package org.scassandra.server.e2e

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.HostDistance.LOCAL
import com.datastax.driver.core.exceptions.NoHostAvailableException
import org.scassandra.server.{ConnectionToServerStub, AbstractIntegrationTest}
import org.scassandra.server.PrimingHelper._

class CurrentTest extends AbstractIntegrationTest {

  test("Should retrieve correct connections.") {
    val expectedConnections = 1 + cluster.getConfiguration.getPoolingOptions.getCoreConnectionsPerHost(LOCAL)
    val connections = getConnections()

    connections should have size expectedConnections
    all (connections) should have ('host ("127.0.0.1"))
  }

  test("Should be able to close connection.") {
    val connections = getConnections()
    val report = closeConnection(connections.head, "reset")

    report.operation should be ("reset")
    report.connections should have size 1

    // Connection in report should reflect the one we closed.
    all (report.connections) should have ('port (connections.head.port))

    // Connection should not be present in connection list.
    all (getConnections()) should not have 'port (connections.head.port)
  }

  test("Should be able to disable and reenable listener") {
    enableListener() should equal (false) // already enabled.
    disableListener() should equal (true)
    disableListener() should equal (false) // already disabled.

    val builder = Cluster.builder()
      .addContactPoint(ConnectionToServerStub.ServerHost)
      .withPort(ConnectionToServerStub.ServerPort)

    cluster = builder.build()
    try {
      cluster.connect()
      fail("Should not have been able to connect.")
    } catch {
      case nhae: NoHostAvailableException => // expected.
    } finally {
      cluster.close()
    }

    cluster = builder.build()

    try {
      enableListener() should equal(true)
      val session = cluster.connect()
      // Host should have been able to connect.
      session.getState.getConnectedHosts should have size 1
    } finally {
      cluster.close()
    }
  }
}
