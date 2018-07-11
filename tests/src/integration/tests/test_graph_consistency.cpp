/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include "dse_integration.hpp"
#include "options.hpp"

#define GRAPH_READ_QUERY \
  "g.V().limit(1);"

#define GRAPH_WRITE_QUERY \
  "graph.addVertex(label, 'person', 'name', 'michael', 'age', 27);"

/**
 * Graph consistency integration tests
 *
 * @dse_version 5.0.0
 */
class GraphConsistencyTest : public DseIntegration {
public:
  void SetUp() {
    CHECK_VERSION(5.0.0);

    // Call the parent setup function
    dse_workload_.push_back(CCM::DSE_WORKLOAD_GRAPH);
    number_dc1_nodes_ = 3;
    replication_factor_ = 3; // Force RF=3 instead of default calculated RF=2
    is_ccm_start_node_individually_ = true;
    DseIntegration::SetUp();

    // Create the graph
    create_graph();
    CHECK_FAILURE;
    populate_classic_graph(test_name_);
    CHECK_FAILURE;
  }

  /**
   * Execute a graph read query with the specified read consistencies
   *
   * @param consistency Read consistency to apply to statement
   * @param is_assert True if error code for future should be asserted; false
   *                  otherwise
   * @return DSE graph result from the executed query
   */
  dse::GraphResultSet execute_read_query(CassConsistency consistency,
    bool assert_ok = true) {
    // Initialize the graph options
    dse::GraphOptions graph_options;
    graph_options.set_name(test_name_);
    graph_options.set_read_consistency(consistency);

    // Execute the graph query and return the result set
    return dse_session_.execute(GRAPH_READ_QUERY, graph_options, assert_ok);
  }

  /**
   * Execute a graph write query with the specified write consistencies
   *
   * @param consistency Write consistency to apply to statement
   * @param is_assert True if error code for future should be asserted; false
   *                  otherwise
   * @return DSE graph result from the executed query
   */
  dse::GraphResultSet execute_write_query(CassConsistency consistency,
                                          bool assert_ok = true) {
    // Initialize the graph options
    dse::GraphOptions graph_options;
    graph_options.set_name(test_name_);
    graph_options.set_write_consistency(consistency);

    // Execute the graph query and return the result set
    return dse_session_.execute(GRAPH_WRITE_QUERY, graph_options, assert_ok);
  }

  /**
   * Helper method to stop a node during the graph consistency test to ensure
   * schema operations have been propagated through the cluster before a node
   * is stopped.
   *
   * @param node Node that should be stopped
   */
  bool stop_node(unsigned int node) {
    // Determine if schema operations should be triggered across cluster nodes
    if (!propagate_schema_) {
      TEST_LOG("Performing Graph Query to Propagate Schema Across Cluster: "
        << "Waiting 10s");
      execute_read_query(CASS_CONSISTENCY_ONE);
      msleep(10000);
      propagate_schema_ = true;
    }

    // Stop the requested node
    return DseIntegration::stop_node(node);
  }

  /**
   * Validate the write query using the result set
   *
   * @param result_set Write query result set to validate
   */
  void validate_write_query(dse::GraphResultSet result_set) {
    // Get the vertex from the result set
    ASSERT_EQ(CASS_OK, result_set.error_code());
    dse::GraphResult result = result_set.next();
    dse::GraphVertex vertex = result.vertex();
    CHECK_FAILURE;

    // Validate the properties on the vertex
    ASSERT_EQ("person", vertex.label().value<std::string>());
    dse::GraphResult properties = vertex.properties();
    ASSERT_EQ(2u, properties.member_count());
    for (size_t i = 0; i < 2; ++i) {
      dse::GraphResult property = properties.member(i);
      ASSERT_EQ(DSE_GRAPH_RESULT_TYPE_ARRAY, property.type());
      ASSERT_EQ(1u, property.element_count());
      property = property.element(0);
      ASSERT_EQ(DSE_GRAPH_RESULT_TYPE_OBJECT, property.type());
      ASSERT_EQ(2u, property.member_count());

      // Ensure the name is "michael" and the age is 27
      bool property_asserted = false;
      if (properties.key(i).compare("name") == 0) {
        for (size_t k = 0; k < 2; ++k) {
          if (property.key(k).compare("value") == 0) {
            ASSERT_EQ("michael", property.member(k).value<std::string>());
            property_asserted = true;
            break;
          }
        }
      } else {
        for (size_t k = 0; k < 2; ++k) {
          if (property.key(k).compare("value") == 0) {
            ASSERT_EQ(Integer(27), property.member(k).value<Integer>());
            property_asserted = true;
            break;
          }
        }
      }
      ASSERT_TRUE(property_asserted);
    }
  }

private:
  /**
   * Flag to determine if a node stop was previously requested
   */
  bool propagate_schema_;
};

/**
 * Perform a read graph query against a 3 node cluster
 *
 * This test will execute a read graph query against an established graph with
 * all appropriate read consistencies when all node available in the cluster.
 *
 * Consistency levels options applied:
 *   - CASS_CONSISTENCY_ONE
 *   - CASS_CONSISTENCY_TWO
 *   - CASS_CONSISTENCY_THREE
 *   - CASS_CONSISTENCY_ALL
 *   - CASS_CONSISTENCY_QUORUM
 *
 * @jira_ticket CPP-375
 * @test_category dse:graph
 * @since 1.0.0
 * @expected_result Graph read will succeed for all consistency levels applied
 */
DSE_INTEGRATION_TEST_F(GraphConsistencyTest, Read) {
  CHECK_VERSION(5.0.0);
  CHECK_FAILURE;

  // Perform the read graph query
  ASSERT_EQ(CASS_OK, execute_read_query(CASS_CONSISTENCY_ONE).error_code());
  ASSERT_EQ(CASS_OK, execute_read_query(CASS_CONSISTENCY_TWO).error_code());
  ASSERT_EQ(CASS_OK, execute_read_query(CASS_CONSISTENCY_THREE).error_code());
  ASSERT_EQ(CASS_OK, execute_read_query(CASS_CONSISTENCY_ALL).error_code());
  ASSERT_EQ(CASS_OK, execute_read_query(CASS_CONSISTENCY_QUORUM).error_code());
}

/**
 * Perform a read graph query against a 3 node cluster with 1 node down
 *
 * This test will execute a read graph query against an established graph with
 * all appropriate read consistencies when one node is down in a three node
 * cluster.
 *
 * Consistency levels options applied:
 *   - CASS_CONSISTENCY_ONE
 *   - CASS_CONSISTENCY_TWO
 *   - CASS_CONSISTENCY_QUORUM
 *   Expected to fail:
 *     - CASS_CONSISTENCY_ALL
 *     - CASS_CONSISTENCY_THREE
 *
 * @jira_ticket CPP-375
 * @test_category dse:graph
 * @since 1.0.0
 * @expected_result Graph read will succeed for all consistency levels applied
 *                  and will expect failure to occur for ALL and THREE
 *                  consistency levels
 */
DSE_INTEGRATION_TEST_F(GraphConsistencyTest, ReadOneNodeDown) {
  CHECK_VERSION(5.0.0);
  CHECK_FAILURE;

  // Perform the read graph query with one node down
  stop_node(1);
  ASSERT_EQ(CASS_OK, execute_read_query(CASS_CONSISTENCY_ONE).error_code());
  ASSERT_EQ(CASS_OK, execute_read_query(CASS_CONSISTENCY_TWO).error_code());
  ASSERT_EQ(CASS_OK, execute_read_query(CASS_CONSISTENCY_QUORUM).error_code());

  // Perform the read graph query with consistency levels expected to fail
  dse::GraphResultSet result_set = execute_read_query(CASS_CONSISTENCY_ALL,
                                                      false);
  ASSERT_NE(CASS_OK, result_set.error_code());
  ASSERT_TRUE(contains(result_set.error_message(), "Cannot achieve consistency level") ||
              contains(result_set.error_message(), "Operation timed out"));
  result_set = execute_read_query(CASS_CONSISTENCY_THREE, false);
  ASSERT_NE(CASS_OK, result_set.error_code());
}

/**
 * Perform a write graph query against a 3 node cluster
 *
 * This test will execute a write graph query against an established graph with
 * all appropriate write consistencies when all node available in the cluster.
 *
 * Consistency levels options applied:
 *   - CASS_CONSISTENCY_ANY
 *   - CASS_CONSISTENCY_ONE
 *   - CASS_CONSISTENCY_TWO
 *   - CASS_CONSISTENCY_THREE
 *   - CASS_CONSISTENCY_ALL
 *   - CASS_CONSISTENCY_QUORUM
 *
 * @jira_ticket CPP-375
 * @test_category dse:graph
 * @since 1.0.0
 * @expected_result Graph write will succeed for all consistency levels applied
 */
DSE_INTEGRATION_TEST_F(GraphConsistencyTest, Write) {
  CHECK_VERSION(5.0.0);
  CHECK_FAILURE;

  // Perform the write graph query
  validate_write_query(execute_write_query(CASS_CONSISTENCY_ANY));
  CHECK_FAILURE;
  validate_write_query(execute_write_query(CASS_CONSISTENCY_ONE));
  CHECK_FAILURE;
  validate_write_query(execute_write_query(CASS_CONSISTENCY_TWO));
  CHECK_FAILURE;
  validate_write_query(execute_write_query(CASS_CONSISTENCY_THREE));
  CHECK_FAILURE;
  validate_write_query(execute_write_query(CASS_CONSISTENCY_ALL));
  CHECK_FAILURE;
  validate_write_query(execute_write_query(CASS_CONSISTENCY_QUORUM));
  CHECK_FAILURE;
}

/**
 * Perform a write graph query against a 3 node cluster with 1 node down
 *
 * This test will execute a write graph query against an established graph with
 * all appropriate write consistencies when one node is down in a three node
 * cluster.
 *
 * Consistency levels options applied:
 *   - CASS_CONSISTENCY_ANY
 *   - CASS_CONSISTENCY_ONE
 *   - CASS_CONSISTENCY_TWO
 *   - CASS_CONSISTENCY_QUORUM
 *   Expected to fail:
 *     - CASS_CONSISTENCY_ALL
 *     - CASS_CONSISTENCY_THREE
 *
 * @jira_ticket CPP-375
 * @test_category dse:graph
 * @since 1.0.0
 * @expected_result Graph write will succeed for all consistency levels applied
 *                  and will expect failure to occur for ALL and THREE
 *                  consistency levels
 */
DSE_INTEGRATION_TEST_F(GraphConsistencyTest, WriteOneNodeDown) {
  CHECK_VERSION(5.0.0);
  CHECK_FAILURE;

  // Perform the write graph query with one node down
  stop_node(1);
  validate_write_query(execute_write_query(CASS_CONSISTENCY_ANY));
  CHECK_FAILURE;
  validate_write_query(execute_write_query(CASS_CONSISTENCY_ONE));
  CHECK_FAILURE;
  validate_write_query(execute_write_query(CASS_CONSISTENCY_TWO));
  CHECK_FAILURE;
  validate_write_query(execute_write_query(CASS_CONSISTENCY_QUORUM));
  CHECK_FAILURE;

  // Perform the write graph query with consistency levels expected to fail
  dse::GraphResultSet result_set =
    execute_write_query(CASS_CONSISTENCY_ALL, false);
  ASSERT_NE(CASS_OK, result_set.error_code());
  ASSERT_TRUE(contains(result_set.error_message(), "Cannot achieve consistency level") ||
              contains(result_set.error_message(), "Operation timed out"));
  result_set = execute_write_query(CASS_CONSISTENCY_THREE, false);
  ASSERT_NE(CASS_OK, result_set.error_code());
}
