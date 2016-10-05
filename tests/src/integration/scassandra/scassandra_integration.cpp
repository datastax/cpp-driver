/*
  Copyright (c) 2016 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include "scassandra_integration.hpp"
#include "priming_requests.hpp"
#include "win_debug.hpp"

// Initialize the static member variables
SharedPtr<test::SCassandraCluster> SCassandraIntegration::scc_ = NULL;
bool SCassandraIntegration::is_scc_started_ = false;
const PrimingRequest SCassandraIntegration::mock_query_ = PrimingRequest::builder()
  .with_query("mock query")
  .with_rows(PrimingRows::builder()
    .add_row(PrimingRow::builder()
      .add_column("SUCCESS", CASS_VALUE_TYPE_BOOLEAN, "TRUE")
    )
  );

SCassandraIntegration::SCassandraIntegration()
  : is_scc_start_requested_(true)
  , is_scc_for_test_case_(true) {
}

SCassandraIntegration::~SCassandraIntegration() {
}

void SCassandraIntegration::SetUpTestCase() {
  try {
    scc_ = new test::SCassandraCluster();
  } catch (SCassandraCluster::Exception scce) {
    FAIL() << scce.what();
  }
}

void SCassandraIntegration::SetUp() {
  CHECK_SCC_AVAILABLE;

  // Initialize the SCassandra cluster instance
  if (is_scc_start_requested_) {
    // Start the SCC
    default_start_scc();

    // Prime the system tables (local and peers))
    scc_->prime_system_tables();

    // Generate the default contact points
    contact_points_ = scc_->cluster_contact_points();
  }

  // Determine if the session connection should be established
  if (is_session_requested_) {
    if (is_scc_started_) {
      connect();
    } else {
      LOG_ERROR("Connection to SCassandra Cluster Aborted: SCC has not been started");
    }
  }
}

//void SCassandraIntegration::TearDownTestCase() {
//  CHECK_SCC_AVAILABLE;
//
//  scc_->destroy_cluster();
//  is_scc_started_ = false;
//}

void SCassandraIntegration::TearDown() {
  CHECK_SCC_AVAILABLE;

  session_.close();

  // Reset the SCassandra cluster (if not being used for the entire test case)
  scc_->reset_cluster();
  if (!is_scc_for_test_case_) {
    scc_->destroy_cluster();
    is_scc_started_ = false;
  }
}

test::driver::Cluster SCassandraIntegration::default_cluster() {
  return Integration::default_cluster()
    .with_connection_heartbeat_interval(0);
}

void SCassandraIntegration::default_start_scc() {
  // Create the data center nodes vector and start the SCC
  std::vector<unsigned int> data_center_nodes;
  data_center_nodes.push_back(number_dc1_nodes_);
  data_center_nodes.push_back(number_dc2_nodes_);
  start_scc(data_center_nodes);
}

void SCassandraIntegration::start_scc(std::vector<unsigned int>
  data_center_nodes /*= SCassandraCluster::DEFAULT_DATA_CENTER_NODES*/) {
  // Ensure the SCC is only started once (process handling)
  if (!is_scc_started_) {
    // Create and start the SCC
    MemoryLeakListener::disable();
    scc_->create_cluster(data_center_nodes);
    scc_->start_cluster();
    MemoryLeakListener::enable();
    is_scc_started_ = true;
  }
}

test::driver::Result SCassandraIntegration::execute_mock_query(
  CassConsistency consistency /*= CASS_CONSISTENCY_ONE*/) {
  return session_.execute("mock query", consistency, false, false);
}

void SCassandraIntegration::prime_mock_query(unsigned int node /*= 0*/) {
  // Create the mock query
  PrimingRequest mock_query = mock_query_;
  mock_query.with_result(PrimingResult::SUCCESS);

  // Determine if this is targeting a particular node
  if (node > 0) {
    scc_->prime_query(node, mock_query);
  } else {
    scc_->prime_query(mock_query);
  }
}

void SCassandraIntegration::prime_mock_query_with_error(PrimingResult result,
  unsigned int node /*= 0*/) {
  // Create the mock query
  PrimingRequest mock_query = mock_query_;
  mock_query.with_result(result);

  // Determine if this is targeting a particular node
  if (node > 0) {
    // Send the simulated error to the SCassandra node
    scc_->prime_query(node, mock_query);

    // Update the primed query to be successful on the other node
    std::vector<unsigned int> nodes = scc_->nodes();
    for (std::vector<unsigned int>::iterator iterator = nodes.begin();
      iterator != nodes.end(); ++iterator) {
      unsigned int current_node = *iterator;
      if (current_node != node) {
        prime_mock_query(current_node);
      }
    }
  } else {
    scc_->prime_query(mock_query);
  }
}
