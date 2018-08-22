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

#ifndef UNIT_TEST_HPP
#define UNIT_TEST_HPP

#include "cassandra.h"
#include "connector.hpp"
#include "mockssandra.hpp"

#include <gtest/gtest.h>
#include <uv.h>

#define PROTOCOL_VERSION CASS_HIGHEST_SUPPORTED_PROTOCOL_VERSION
#define PORT 9042
#define WAIT_FOR_TIME 5 * 1000 * 1000 // 5 seconds
#define DEFAULT_NUM_NODES 1

class Unit : public testing::Test {
public:
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
   * Create the default simple request handler for use with mockssandra.
   *
   * @return Simple request handler instance.
   */
  static const mockssandra::RequestHandler* simple();
  /**
   * Create the default authentication request handler for use with mockssandra.
   *
   * @return Simple request handler instance.
   */
  static const mockssandra::RequestHandler* auth();

  /**
   * Setup the cluster to use SSL and return a connection settings object with
   * a SSL context, a SSL certificate, and hostname resolution enabled.
   *
   * @param cluster Mockssandra cluster to apply SSL settings to
   * @return A connection settings object setup to use SSL.
   */
  cass::ConnectionSettings use_ssl(mockssandra::Cluster* cluster);

private:
  CassLogLevel saved_log_level_;
};

#endif // UNIT_TEST_HPP
