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

import com.datastax.driver.core.policies.ConstantReconnectionPolicy
import com.datastax.driver.core.{Cluster, HostDistance, PoolingOptions, SocketOptions}
import org.scassandra.server.{AbstractIntegrationTest, ConnectionToServerStub}

class HeartbeatVerificationTest extends AbstractIntegrationTest(false) {

  test("Should remain connected after heartbeat interval + read timeout.") {
    // Configures a cluster with a heartbeat interval of 1 second, a read timeout
    // of 2 seconds (to provoke quick heartbeat timeouts), and a reconnection
    // timeout of 60 seconds to delay reconnection.
    val cluster = Cluster.builder()
      .addContactPoint(ConnectionToServerStub.ServerHost)
      .withPort(ConnectionToServerStub.ServerPort)
      .withPoolingOptions(new PoolingOptions()
        .setCoreConnectionsPerHost(HostDistance.LOCAL, 1)
        .setHeartbeatIntervalSeconds(1))
      .withSocketOptions(new SocketOptions().setReadTimeoutMillis(2000))
      .withReconnectionPolicy(new ConstantReconnectionPolicy(60000))
      .build()
    try {
      val session = cluster.connect()

      // Wait for some time after heartbeat interval + read timeout and
      // ensure that we are still connected.
      Thread.sleep(5000)
      session.getState.getConnectedHosts.size() should equal(1)
    } finally {
      cluster.close()
    }
  }
}
