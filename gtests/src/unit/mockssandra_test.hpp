/*
  Copyright (c) DataStax, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#ifndef MOCKSSANDRA_TEST_HPP
#define MOCKSSANDRA_TEST_HPP

#include "mockssandra.hpp"

#include "cassandra.h"
#include "connector.hpp"

#include "loop_test.hpp"

namespace mockssandra {

/**
 * A test that create a simple mockssandra cluster.
 */
class SimpleClusterTest : public LoopTest {
public:
  /**
   * Construct a cluster with a specified number of nodes and request handler.
   *
   * @param num_nodes The number of node in the cluster.
   * @param handler The request handler.
   */
  explicit SimpleClusterTest(size_t num_nodes = 1,
                             const mockssandra::RequestHandler* handler = NULL);

  /**
   * Test setup method. This remembers the current state of log level.
   */
  virtual void SetUp();

  /**
   * Test tear down method. This restores the previous log level state and stops
   * all cluster nodes.
   */
  virtual void TearDown();

  /**
   * Set the log level for the test. The log level will be restored
   * to its previous state at the end of each test.
   *
   * @param log_level The log level.
   */
  void set_log_level(CassLogLevel log_level);

  /**
   * Setup the cluster to use SSL and return a connection settings object with
   * a SSL context, a SSL certificate, and hostname resolution enabled.
   *
   * @return A connection settings object setup to use SSL.
   */
  cass::ConnectionSettings use_ssl();

  /**
   * Start a specific node.
   *
   * @param node The node to start.
   */
  void start(size_t node);

  /**
   * Stop a specific node.
   *
   * @param node The node to stop.
   */
  void stop(size_t node);

  /**
   * Start all nodes in the cluster.
   */
  void start_all();

  /**
   * Stop all nodes in the cluster.
   */
  void stop_all();

  /**
   * Setup the cluster so that connections to the cluster close immediately
   * after connection.
   */
  void use_close_immediately();

protected:
  mockssandra::SimpleCluster cluster_;
  CassLogLevel saved_log_level_;
};

} // namespace mockssandra

#endif // MOCKSSANDRA_TEST_HPP
