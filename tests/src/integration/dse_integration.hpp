/*
  Copyright (c) 2014-2016 DataStax

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

/**
 * Extended class to provide common integration test functionality for DSE
 * tests
 */
class DseIntegration : public Integration {
public:

  DseIntegration();

  virtual void SetUp();

  /**
   * Establish the session connection using provided cluster object.
   *
   * @param cluster Cluster object to use when creating session connection
   */
  virtual void connect(Cluster cluster);

  /**
   * Create the cluster configuration and establish the session connection using
   * provided cluster object.
   */
  virtual void connect();

protected:
  /**
   * Connected database DSE session
   */
  DseSession dse_session_;

  /**
   * Create the graph using the specified replication strategy
   *
   * @param graph_name Name of the graph to create
   * @param replication_strategy Replication strategy to apply to graph
   * @param duration Maximum duration to wait for a traversal to evaluate
   * @see replication_factor_
   */
  void DseIntegration::create_graph(const std::string& graph_name,
    const std::string& replication_strategy,
    const std::string& duration);

  /**
   * Create the graph using the test name and default replication strategy
   *
   * @param duration Maximum duration to wait for a traversal to evaluate
   *                 (default: PT30S; 30 seconds)
   * @see test_name_
   * @see replication_strategy_
   */
  void DseIntegration::create_graph(const std::string& duration = "PT30S");
};

#endif //__DSE_INTEGRATION_HPP__
