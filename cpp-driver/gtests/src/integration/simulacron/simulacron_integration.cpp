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

#include "simulacron_integration.hpp"

#include "priming_requests.hpp"
#include "win_debug.hpp"

// Initialize the static member variables
SharedPtr<test::SimulacronCluster> SimulacronIntegration::sc_ = NULL;
bool SimulacronIntegration::is_sc_started_ = false;

SimulacronIntegration::SimulacronIntegration()
    : is_sc_start_requested_(true)
    , is_sc_for_test_case_(false) {}

SimulacronIntegration::~SimulacronIntegration() {}

void SimulacronIntegration::SetUpTestCase() {
  try {
    sc_ = new test::SimulacronCluster();
  } catch (SimulacronCluster::Exception scce) {
    TEST_LOG_ERROR(scce.what());
  }
}

void SimulacronIntegration::SetUp() {
  CHECK_SIMULACRON_AVAILABLE;

  // Initialize the Simulacron cluster instance
  if (is_sc_start_requested_) {
    // Start the SC
    default_start_sc();

    // Generate the default contact points
    contact_points_ = sc_->cluster_contact_points();
  }

  // Determine if the session connection should be established
  if (is_session_requested_) {
    if (is_sc_started_) {
      connect();
    } else {
      TEST_LOG_ERROR("Connection to Simulacron Cluster Aborted: SC has not been started");
    }
  }
}

void SimulacronIntegration::TearDown() {
  CHECK_SIMULACRON_AVAILABLE;

  session_.close();

  // Reset the Simulacron cluster (if not being used for the entire test case)
  if (!is_sc_for_test_case_) {
    sc_->remove_cluster();
    is_sc_started_ = false;
  }
}

test::driver::Cluster SimulacronIntegration::default_cluster() {
  return Integration::default_cluster().with_connection_heartbeat_interval(0);
}

void SimulacronIntegration::default_start_sc() {
  // Create the data center nodes vector and start the SC
  std::vector<unsigned int> data_center_nodes;
  if (number_dc1_nodes_ > 0) data_center_nodes.push_back(number_dc1_nodes_);
  if (number_dc2_nodes_ > 0) data_center_nodes.push_back(number_dc2_nodes_);
  start_sc(data_center_nodes);
}

void SimulacronIntegration::start_sc(
    const std::vector<unsigned int>&
        data_center_nodes /*= SimulacronCluster::DEFAULT_DATA_CENTER_NODES*/) {
  // Ensure the SC is only started once (process handling)
  if (!is_sc_started_) {
    // Create and start the SC
    MemoryLeakListener::disable();
    sc_->create_cluster(data_center_nodes, is_with_vnodes_);
    MemoryLeakListener::enable();
    is_sc_started_ = true;
  }
}

test::driver::Result
SimulacronIntegration::execute_mock_query(CassConsistency consistency /*= CASS_CONSISTENCY_ONE*/) {
  return session_.execute("mock query", consistency, false, false);
}

void SimulacronIntegration::prime_mock_query(unsigned int node /*= 0*/) {
  prime::Success success = prime::Success();
  success.with_rows(
      prime::Rows().add_row(prime::Row().add_column("SUCCESS", CASS_VALUE_TYPE_BOOLEAN, "TRUE")));
  prime_mock_query_with_result(&success, node);
}

void SimulacronIntegration::prime_mock_query_with_result(prime::Result* result,
                                                         unsigned int node /*= 0*/) {
  // Create the mock query
  prime::Request mock_query = prime::Request().with_query("mock query").with_result(result);

  // Prime the mock query with the given error
  sc_->prime_query(mock_query, node);
}
