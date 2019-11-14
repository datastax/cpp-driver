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

#ifndef __DSE_INTEGRATION_HPP__
#define __DSE_INTEGRATION_HPP__
#include "integration.hpp"

#include "dse.h"

#include "dse_objects.hpp"
#include "dse_pretty_print.hpp"
#include "dse_values.hpp"

#undef SKIP_TEST_VERSION
#define SKIP_TEST_VERSION(server_version_string, version_string) \
  SKIP_TEST("Unsupported for DataStax Enterprise Version "       \
            << server_version_string << ": Server version " << version_string << "+ is required")

#undef CHECK_VERSION
#define CHECK_VERSION(version)                                              \
  if (!Options::is_dse()) {                                                 \
    SKIP_TEST("DataStax Enterprise Version " << #version << " is required") \
  } else if (this->server_version_ < #version) {                            \
    SKIP_TEST_VERSION(this->server_version_.to_string(), #version)          \
  }

#undef CHECK_VALUE_TYPE_VERSION
#define CHECK_VALUE_TYPE_VERSION(type)                                                     \
  if (this->server_version_ < type::supported_server_version()) {                          \
    SKIP_TEST_VERSION(this->server_version_.to_string(), type::supported_server_version()) \
  }

// Macros to use for grouping DSE integration tests together
#define DSE_INTEGRATION_TEST(test_case, test_name) INTEGRATION_TEST(DSE, test_case, test_name)
#define DSE_INTEGRATION_TEST_F(test_case, test_name) INTEGRATION_TEST_F(DSE, test_case, test_name)
#define DSE_INTEGRATION_TYPED_TEST_P(test_case, test_name) \
  INTEGRATION_TYPED_TEST_P(DSE, test_case, test_name)
#define DSE_INTEGRATION_DISABLED_TEST_F(test_case, test_name) \
  INTEGRATION_DISABLED_TEST_F(DSE, test_case, test_name)
#define DSE_INTEGRATION_DISABLED_TYPED_TEST_P(test_case, test_name) \
  INTEGRATION_DISABLED_TYPED_TEST_P(DSE, test_case, test_name)

/**
 * Extended class to provide common integration test functionality for DSE
 * tests
 */
class DseIntegration : public Integration {
public:
  using Integration::connect;

  DseIntegration();

  virtual void SetUp();

  /**
   * Establish the session connection using provided cluster object.
   *
   * @param cluster Cluster object to use when creating session connection
   */
  virtual void connect(dse::Cluster cluster);

  /**
   * Create the cluster configuration and establish the session connection using
   * provided cluster object.
   */
  virtual void connect();

  /**
   * Get the default DSE cluster configuration
   *
   * @return DSE Cluster object (default)
   */
  virtual Cluster default_cluster();

protected:
  /**
   * Configured DSE cluster
   */
  dse::Cluster dse_cluster_;
  /**
   * Connected database DSE session
   */
  dse::Session dse_session_;

  /**
   * Create the graph using the specified replication strategy
   *
   * @param graph_name Name of the graph to create
   * @param replication_strategy Replication strategy to apply to graph
   * @param duration Maximum duration to wait for a traversal to evaluate
   * @see replication_factor_
   */
  void create_graph(const std::string& graph_name, const std::string& replication_strategy,
                    const std::string& duration);

  /**
   * Create the graph using the test name and default replication strategy
   *
   * @param duration Maximum duration to wait for a traversal to evaluate
   *                 (default: PT30S; 30 seconds)
   * @see test_name_
   * @see replication_strategy_
   */
  void create_graph(const std::string& duration = "PT30S");

  /**
   * Populate the graph with the classic graph structure examples used in the
   * TinkerPop documentation.
   *
   * @see http://tinkerpop.apache.org/docs/3.1.0-incubating/#intro
   *
   * @param graph_name Name of the graph to initialize
   */
  void populate_classic_graph(const std::string& graph_name);
};

#endif //__DSE_INTEGRATION_HPP__
