/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include "dse_integration.hpp"

#include "values/dse_date_range.hpp"

/**
 * DSE type graph (w/ geotypes) integration tests
 *
 * @dse_version 5.0.0
 */
template <class C>
class DseTypesGraphTest : public DseIntegration {
public:
  /**
   * DSE type values
   */
  static const std::vector<C> values_;

  void SetUp() {
    CHECK_VERSION(5.0.0);
    // Call the parent setup function
    dse_workload_.push_back(CCM::DSE_WORKLOAD_GRAPH);
    DseIntegration::SetUp();
  }
};
TYPED_TEST_CASE_P(DseTypesGraphTest);

/**
 * Perform insert using a graph array
 *
 * This test will perform multiple inserts using a graph statement with the
 * parameterized type values statically assigned against a single node cluster.
 *
 * @jira_ticket CPP-400
 * @test_category dse:graph
 * @test_category dse:geospatial
 * @since 1.0.0
 * @dse_version 5.0.0
 * @expected_result DSE values are inserted and validated via graph
 *                  operations using a graph array (attached to a graph object)
 */
DSE_INTEGRATION_TYPED_TEST_P(DseTypesGraphTest, GraphArray) {
  CHECK_VALUE_TYPE_VERSION(TypeParam);

  const std::vector<TypeParam>& values = DseTypesGraphTest<TypeParam>::values_;

  // Iterate over all the values in the geotype and add them to a graph array
  dse::GraphArray graph_array;
  for (size_t i = 0; i < values.size(); ++i) {
    // Get the value of the geotype being used
    TypeParam value = values[i];

    // Add the value to the graph array
    graph_array.add<TypeParam>(value);
  }

  // Create the statement to insert the geotype using an object with array
  dse::GraphObject graph_object;
  graph_object.add<dse::GraphArray>("geotype", graph_array);
  dse::GraphStatement graph_statement("[geotype]");
  graph_statement.bind(graph_object);

  // Execute the statement and get the result
  dse::GraphResultSet result_set = this->dse_session_.execute(graph_statement);

  // Assert/Validate the geotype using a graph statement
  CHECK_FAILURE;
  ASSERT_EQ(1ul, result_set.count());
  dse::GraphResult result = result_set.next();
  ASSERT_TRUE(result.is_type<dse::GraphArray>());

  // Gather the values from the graph array result
  std::vector<TypeParam> result_values;
  for (size_t i = 0; i < result.element_count(); ++i) {
    TypeParam value = result.element(i).value<TypeParam>();
    CHECK_FAILURE;
    result_values.push_back(value);
  }
  ASSERT_EQ(values, result_values);
}

/**
 * Perform insert using a graph object
 *
 * This test will perform multiple inserts using a graph statement with the
 * parameterized type values statically assigned against a single node cluster.
 *
 * @jira_ticket CPP-400
 * @test_category dse:graph
 * @test_category dse:geospatial
 * @since 1.0.0
 * @dse_version 5.0.0
 * @expected_result DSE values are inserted and validated via graph
 *                  operations using a graph object
 */
DSE_INTEGRATION_TYPED_TEST_P(DseTypesGraphTest, GraphObject) {
  CHECK_VALUE_TYPE_VERSION(TypeParam);

  const std::vector<TypeParam>& values = DseTypesGraphTest<TypeParam>::values_;

  // Iterate over all the values in the geotype
  for (size_t i = 0; i < values.size(); ++i) {
    // Get the value of the geotype being used
    TypeParam value = values[i];

    // Create the graph statement to insert the geotype using an object
    dse::GraphObject graph_object;
    graph_object.add<TypeParam>("geotype", value);
    dse::GraphStatement graph_statement("[geotype]");
    graph_statement.bind(graph_object);

    // Assert/Validate the geotype using a graph statement
    dse::GraphResultSet result_set = this->dse_session_.execute(graph_statement);
    CHECK_FAILURE;
    ASSERT_EQ(1ul, result_set.count());
    dse::GraphResult result = result_set.next();
    ASSERT_EQ(value, result.value<TypeParam>());
  }
}
// Register all test cases
REGISTER_TYPED_TEST_CASE_P(DseTypesGraphTest, Integration_DSE_GraphArray,
                           Integration_DSE_GraphObject);

// Instantiate the test case for all the geotypes and date range
typedef testing::Types<dse::Point, dse::LineString, dse::Polygon> DseTypes;
INSTANTIATE_TYPED_TEST_CASE_P(DseTypes, DseTypesGraphTest, DseTypes);

/**
 * Values for point tests
 */
const dse::Point GEOMETRY_POINTS[] = {
  dse::Point("0.0, 0.0"),
  dse::Point("2.0, 4.0"),
  dse::Point("-1.2, -100.0"),
};
template <>
const std::vector<dse::Point>
    DseTypesGraphTest<dse::Point>::values_(GEOMETRY_POINTS,
                                           GEOMETRY_POINTS + ARRAY_LEN(GEOMETRY_POINTS));

/**
 * Values for line string tests
 */
const dse::LineString GEOMETRY_LINE_STRING[] = {
  dse::LineString("0.0 0.0, 1.0 1.0"),
  dse::LineString("1.0 3.0, 2.0 6.0, 3.0 9.0"),
  dse::LineString("-1.2 -100.0, 0.99 3.0"),
  dse::LineString("LINESTRING EMPTY"),
};
template <>
const std::vector<dse::LineString> DseTypesGraphTest<dse::LineString>::values_(
    GEOMETRY_LINE_STRING, GEOMETRY_LINE_STRING + ARRAY_LEN(GEOMETRY_LINE_STRING));

/**
 * Values for polygon tests
 */
const dse::Polygon GEOMETRY_POLYGON[] = {
  dse::Polygon("(1.0 3.0, 3.0 1.0, 3.0 6.0, 1.0 3.0)"),
  dse::Polygon("(0.0 10.0, 10.0 0.0, 10.0 10.0, 0.0 10.0), \
                (6.0 7.0, 3.0 9.0, 9.0 9.0, 6.0 7.0)"),
  dse::Polygon("POLYGON EMPTY"),
};
template <>
const std::vector<dse::Polygon>
    DseTypesGraphTest<dse::Polygon>::values_(GEOMETRY_POLYGON,
                                             GEOMETRY_POLYGON + ARRAY_LEN(GEOMETRY_POLYGON));
