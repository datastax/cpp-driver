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

#ifndef __SIMULACRON_INTEGRATION_HPP__
#define __SIMULACRON_INTEGRATION_HPP__
#include "integration.hpp"
#include "simulacron_cluster.hpp"

#include <uv.h>

// Macros to use for grouping Simulacron integration tests together
#define SIMULACRON_TEST_NAME(test_name) Integration##_##simulacron##_##test_name
#define SIMULACRON_INTEGRATION_TEST_F(test_case, test_name) \
  INTEGRATION_TEST_F(simulacron, test_case, test_name)
#define SIMULACRON_INTEGRATION_TYPED_TEST_P(test_case, test_name) \
  INTEGRATION_TYPED_TEST_P(simulacron, test_case, test_name)

#define CHECK_SIMULACRON_AVAILABLE \
  if (!sc_) { \
    return; \
  }

#define SKIP_TEST_IF_SIMULACRON_UNAVAILABLE \
  if (!sc_) { \
    SKIP_TEST("Simulacron is unavailable"); \
  }

/**
 * Base class to provide common integration test functionality for tests against
 * Simulacron; simulated DSE (and Cassandra)
 */
class SimulacronIntegration : public Integration {
public:
  SimulacronIntegration();
  ~SimulacronIntegration();
  static void SetUpTestCase();
  void SetUp();
  void TearDown();

protected:
  /**
   * Simulacron cluster (manager) instance
   */
  static SharedPtr<test::SimulacronCluster> sc_;
  /**
   * Setting to determine if Simulacron cluster should be started. True if
   * Simulacroncluster should be started; false otherwise.
   * (DEFAULT: true)
   */
  bool is_sc_start_requested_;
  /**
   * Setting to determine if Simulacron cluster is being used for the entire
   * test case or if it should be re-initialized per test. True if for the
   * test case; false otherwise.
   * (DEFAULT: false)
   */
  bool is_sc_for_test_case_;

  /**
   * Get the default cluster configuration
   *
   * @return Cluster object (default)
   */
  virtual Cluster default_cluster();

  /**
   * Default start procedures for the Simulacron cluster (based on the number of
   * nodes in the standard two data center configuration for the test harness)
   */
  void default_start_sc();

  /**
   * Perform the start procedures for the Simulacron cluster with the given
   * data center configuration
   *
   * @param data_center_nodes Data center(s) to create in the Simulacron cluster
   *                          (default: 1 data center with 1 node)
   */
  void start_sc(const std::vector<unsigned int>& data_center_nodes = SimulacronCluster::DEFAULT_DATA_CENTER_NODES);

  /**
   * Execute a mock query at a given consistency level
   *
   * @param consistency Consistency level to execute mock query at
   * @return Result object
   * @see prime_mock_query Primes all nodes with a successful mock query
   * @see prime_mock_query_with_error Primes the given node with with an error
   *                                  for the mock query and primes the
   *                                  remaining nodes with a successful mock
   *                                  query
   */
  virtual test::driver::Result execute_mock_query(CassConsistency consistency = CASS_CONSISTENCY_ONE);

  /**
   * Prime the successful mock query on the given node
   *
   * @param node Node to apply the successful mock query on; if node == 0 the
   *             successful mock query will be applied to all nodes in the
   *             Simulacron cluster
   *             (DEFAULT: 0 - Apply mock query with success to all nodes)
   */

  void prime_mock_query(unsigned int node = 0);
  /**
   * Prime the mock query with a result on the given node while priming the
   * remaining nodes in the Simulacron cluster with a successful mock query
   *
   * @param result Result to apply to the given node
   * @param node Node to apply the result on; if node == 0 the mock query with
   *             result will be applied to all nodes in the Simulacron cluster
   *             (DEFAULT: 0 - Apply mock query with result to all nodes)
   */
  void prime_mock_query_with_result(prime::Result* result,
                                    unsigned int node = 0);

private:
  /**
   * Flag to determine if the Simulacron cluster instance has already been
   * started
   */
  static bool is_sc_started_;
};

#endif //__SIMULACRON_INTEGRATION_HPP__
