/*
  Copyright (c) 2017 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include "dse_integration.hpp"

#define GEOMETRY_TABLE_FORMAT "CREATE TABLE %s (id timeuuid PRIMARY KEY, value %s)"
#define GEOMETRY_INSERT_FORMAT "INSERT INTO %s (id, value) VALUES(%s, %s)"
#define GEOMETRY_SELECT_FORMAT "SELECT value FROM %s WHERE id=%s"
#define GEOMETRY_PRIMARY_KEY_TABLE_FORMAT "CREATE TABLE %s (id %s PRIMARY KEY, value timeuuid)"
#define GEOMETRY_PRIMARY_KEY_TABLE_SUFFIX "_pk"

/**
 * Geometry (geo types) integration tests
 *
 * @dse_version 5.0.0
 */
template<class C>
class GeometryTest : public DseIntegration {
public:
  /**
   * Geo type values
   */
  static const std::vector<C> values_;

  void SetUp() {
    CHECK_VERSION(5.0.0);

    // Call the parent setup function
    dse_workload_.push_back(CCM::DSE_WORKLOAD_GRAPH);
    DseIntegration::SetUp();

    // Assign the primary key table name
    table_name_primary_key_ = table_name_ + GEOMETRY_PRIMARY_KEY_TABLE_SUFFIX;

    // Create the table, insert, and select queries
    session_.execute(format_string(GEOMETRY_TABLE_FORMAT, table_name_.c_str(), values_[0].cql_type().c_str()));
    session_.execute(format_string(GEOMETRY_PRIMARY_KEY_TABLE_FORMAT, table_name_primary_key_.c_str(), values_[0].cql_type().c_str()));
    insert_query_ = format_string(GEOMETRY_INSERT_FORMAT, table_name_.c_str(), "?", "?");
    insert_query_primary_key_ = format_string(GEOMETRY_INSERT_FORMAT, table_name_primary_key_.c_str() , "?", "?");
    select_query_ = format_string(GEOMETRY_SELECT_FORMAT, table_name_.c_str(), "?");
    select_query_primary_key_ = format_string(GEOMETRY_SELECT_FORMAT, table_name_primary_key_.c_str(), "?");

    // Create the prepared statement
    prepared_statement_ = session_.prepare(insert_query_);
    prepared_statement_primary_key_ = session_.prepare(insert_query_primary_key_);
  }

protected:
  /**
   * The table name for queries with the geo type as the primary key
   */
  std::string table_name_primary_key_;
  /**
   * Prepared statement to utilize
   */
  Prepared prepared_statement_;
  /**
   * Prepared statement to utilize (with geo type as the primary key)
   */
  Prepared prepared_statement_primary_key_;
  /**
   * Pre-formatted insert query
   */
  std::string insert_query_;
  /**
   * Pre-formatted insert query (with geo type as the primary key)
   */
  std::string insert_query_primary_key_;
  /**
   * Pre-formatted select query
   */
  std::string select_query_;
  /**
   * Pre-formatted select query (with geo type as the primary key)
   */
  std::string select_query_primary_key_;

  /**
   * Assert and validate the geo type
   *
   * @param id Time UUID to use for select query when validating value
   * @param value Value to assert
   */
  void assert_and_validate_geo_type(TimeUuid id, C value) {
    Statement select_statement(select_query_, 1);
    select_statement.bind<TimeUuid>(0, id);
    Result result = session_.execute(select_statement);
    ASSERT_EQ(1u, result.row_count());
    ASSERT_EQ(value, C(result.first_row(), 0));
  }

  /**
   * Assert and validate the geo type
   *
   * @param value Value to assert
   * @param statement Graph statement to execute
   */
  void assert_and_validate_geo_type(C value,
    test::driver::DseGraphStatement statement) {
    test::driver::DseGraphResultSet result_set = dse_session_.execute(statement);
    CHECK_FAILURE;
    ASSERT_EQ(1ul, result_set.count());
    test::driver::DseGraphResult result = result_set.next();
    ASSERT_EQ(value, result.value<C>());
  }

  /**
   * Assert and validate the geo type values in graph array
   *
   * @param expected_values Values to assert in graph array
   * @param statement Graph statement to execute
   */
  void assert_and_validate_geo_type(std::vector<C> expected_values,
    test::driver::DseGraphStatement statement) {
    // Execute the statement and get the result
    test::driver::DseGraphResultSet result_set = dse_session_.execute(statement);
    CHECK_FAILURE;
    ASSERT_EQ(1ul, result_set.count());
    test::driver::DseGraphResult result = result_set.next();
    ASSERT_TRUE(result.is_type<test::driver::DseGraphArray>());

    // Gather the values from the graph array result
    std::vector<C> values;
    for (size_t i = 0; i < result.element_count(); ++i) {
      C value = result.element(i).value<C>();
      CHECK_FAILURE;
      values.push_back(value);
    }
    ASSERT_EQ(expected_values.size(), values.size());
    ASSERT_TRUE(std::equal(expected_values.begin(),
      expected_values.begin() + expected_values.size(), values.begin()));
  }

  /**
   * Assert and validate the geo type which is the primary key
   *
   * @param value Value to use for select query when validating UUID
   * @param id UUID to assert
   */
  void assert_and_validate_geo_type_primary_key(C value, TimeUuid id) {
    Statement select_statement(select_query_primary_key_, 1);
    select_statement.bind<C>(0, value);
    Result result = session_.execute(select_statement);
    ASSERT_EQ(1u, result.row_count());
    ASSERT_EQ(id, TimeUuid(result.first_row(), 0));
  }
};
TYPED_TEST_CASE_P(GeometryTest);

/**
 * Perform insert using a simple statement operation
 *
 * This test will perform multiple inserts using a simple statement with the
 * parameterized type values statically assigned against a single node cluster.
 *
 * @jira_ticket CPP-351
 * @test_category queries:basic
 * @since 1.0.0
 * @dse_version 5.0.0
 * @expected_result Geo type values are inserted and validated
 */
DSE_INTEGRATION_TYPED_TEST_P(GeometryTest, SimpleStatement) {
  CHECK_VERSION(5.0.0);

  // Iterate over all the values in the geo type
  for (size_t i = 0; i < GeometryTest<TypeParam>::values_.size(); ++i) {
    // Get the value of the geo type being used
    TypeParam value = GeometryTest<TypeParam>::values_[i];

    // Insert the geo type executed by a CQL query string statement
    TimeUuid id = this->uuid_generator_.generate_timeuuid();
    this->session_.execute(this->format_string(GEOMETRY_INSERT_FORMAT,
      this->table_name_.c_str(), id.cql_value().c_str(), value.cql_value().c_str()));

    // Assert/Validate the geo type
    this->assert_and_validate_geo_type(id, value);

    // Insert the geo type as the primary key executed by a CQL query string
    if (!value.is_null()) {
      this->session_.execute(this->format_string(GEOMETRY_INSERT_FORMAT,
        this->table_name_primary_key_.c_str(), value.cql_value().c_str(), id.cql_value().c_str()));

      // Assert/Validate the id where the geo type is the primary key
      this->assert_and_validate_geo_type_primary_key(value, id);
    }

    // Insert the geo type executed by a bounded statement
    Statement statement(this->insert_query_, 2);
    id = this->uuid_generator_.generate_timeuuid();
    statement.bind<TimeUuid>(0, id);
    statement.bind<TypeParam>(1, value);
    this->session_.execute(statement);

    // Assert/Validate the geo type
    this->assert_and_validate_geo_type(id, value);

    // Insert the geo type as the primary key executed by a bounded statement
    if (!value.is_null()) {
      statement = Statement(this->insert_query_primary_key_, 2);
      statement.bind<TypeParam>(0, value);
      statement.bind<TimeUuid>(1, id);
      this->session_.execute(statement);

      // Assert/Validate the id where the geo type is the primary key
      this->assert_and_validate_geo_type_primary_key(value, id);
    }
  }
}

/**
 * Perform insert using a prepared statement operation
 *
 * This test will perform multiple inserts using a prepared statement with the
 * parameterized type values statically assigned against a single node cluster.
 *
 * @jira_ticket CPP-351
 * @test_category prepared_statements
 * @test_category queries:basic
 * @since 1.0.0
 * @dse_version 5.0.0
 * @expected_result Geo type values are inserted and validated
 */
DSE_INTEGRATION_TYPED_TEST_P(GeometryTest, PreparedStatement) {
  CHECK_VERSION(5.0.0);

  // Iterate over all the values in the geo type
  for (size_t i = 0; i < GeometryTest<TypeParam>::values_.size(); ++i) {
    // Get the value of the geo type being used
    TypeParam value = GeometryTest<TypeParam>::values_[i];

    // Bind the time UUID and geo type
    Statement statement = this->prepared_statement_.bind();
    TimeUuid id = this->uuid_generator_.generate_timeuuid();
    statement.bind<TimeUuid>(0, id);
    statement.bind<TypeParam>(1, value);
    this->session_.execute(statement);

    // Assert/Validate the geo type
    this->assert_and_validate_geo_type(id, value);

    // Bind the time UUID and geo type where geo type is the primary key
    if (!value.is_null()) {
      statement = this->prepared_statement_primary_key_.bind();
      statement.bind<TypeParam>(0, value);
      statement.bind<TimeUuid>(1, id);
      this->session_.execute(statement);

      // Assert/Validate the id where the geo type is the primary key
      this->assert_and_validate_geo_type_primary_key(value, id);
    }
  }
}

/**
 * Perform insert using a graph array
 *
 * This test will perform multiple inserts using a graph statement with the
 * parameterized type values statically assigned against a single node cluster.
 *
 * @jira_ticket CPP-400
 * @test_category dse:graph
 * @since 1.0.0
 * @dse_version 5.0.0
 * @expected_result Geo type values are inserted and validated via graph
 *                  operations using a graph array (attached to a graph object)
 */
DSE_INTEGRATION_TYPED_TEST_P(GeometryTest, GraphArray) {
  CHECK_VERSION(5.0.0);

  // Iterate over all the values in the geo type and add them to a graph array
  test::driver::DseGraphArray graph_array;
  for (size_t i = 0; i < GeometryTest<TypeParam>::values_.size(); ++i) {
    // Get the value of the geo type being used
    TypeParam value = GeometryTest<TypeParam>::values_[i];

    // Add the value to the graph array
    graph_array.add<TypeParam>(value);
  }

  // Create the statement to insert the geo type using an object with array
  test::driver::DseGraphObject graph_object;
  graph_object.add<test::driver::DseGraphArray>("geo_type", graph_array);
  test::driver::DseGraphStatement graph_statement("[geo_type]");
  graph_statement.bind(graph_object);

  // Assert/Validate the geo type using a graph statement
  this->assert_and_validate_geo_type(GeometryTest<TypeParam>::values_,
    graph_statement);
}

/**
 * Perform insert using a graph object
 *
 * This test will perform multiple inserts using a graph statement with the
 * parameterized type values statically assigned against a single node cluster.
 *
 * @jira_ticket CPP-400
 * @test_category dse:graph
 * @since 1.0.0
 * @dse_version 5.0.0
 * @expected_result Geo type values are inserted and validated via graph
 *                  operations using a graph object
 */
DSE_INTEGRATION_TYPED_TEST_P(GeometryTest, GraphObject) {
  CHECK_VERSION(5.0.0);

  // Iterate over all the values in the geo type
  for (size_t i = 0; i < GeometryTest<TypeParam>::values_.size(); ++i) {
    // Get the value of the geo type being used
    TypeParam value = GeometryTest<TypeParam>::values_[i];

    // Create the graph statement to insert the geo type using an object
    test::driver::DseGraphObject graph_object;
    graph_object.add<TypeParam>("geo_type", value);
    test::driver::DseGraphStatement graph_statement("[geo_type]");
    graph_statement.bind(graph_object);

    // Assert/Validate the geo type using a graph statement
    this->assert_and_validate_geo_type(value, graph_statement);
  }
}

// Register all test cases
REGISTER_TYPED_TEST_CASE_P(GeometryTest, Integration_DSE_SimpleStatement,
  Integration_DSE_PreparedStatement, Integration_DSE_GraphArray,
  Integration_DSE_GraphObject); //TODO: Create expanding macro for registering typed tests

// Instantiate the test case for all the geo types
typedef testing::Types<test::driver::DsePoint, test::driver::DseLineString, test::driver::DsePolygon> GeoTypes;
INSTANTIATE_TYPED_TEST_CASE_P(Geometry, GeometryTest, GeoTypes);

/**
 * Perform insert and select operations for geo type point
 *
 * This test will perform multiple insert and select operations using the
 * geo type point values specified against a single node cluster.
 *
 * @jira_ticket CPP-351
 * @jira_ticket CPP-400
 * @test_category dse:geospatial
 * @since 1.0.0
 * @expected_result Geo type values are inserted and validated
 */
const test::driver::DsePoint GEOMETRY_POINTS[] = {
  test::driver::DsePoint(0.0, 0.0),
  test::driver::DsePoint(2.0, 4.0),
  test::driver::DsePoint(-1.2, -100.0),
  test::driver::DsePoint() // NULL point
};
template<> const std::vector<test::driver::DsePoint> GeometryTest<test::driver::DsePoint>::values_(
  GEOMETRY_POINTS,
  GEOMETRY_POINTS + sizeof(GEOMETRY_POINTS) / sizeof(GEOMETRY_POINTS[0]));

/**
 * Perform insert and select operations for geo type line string
 *
 * This test will perform multiple insert and select operations using the
 * geo type line string values specified against a single node cluster.
 *
 * @jira_ticket CPP-351
 * @jira_ticket CPP-400
 * @test_category dse:geospatial
 * @since 1.0.0
 * @expected_result Geo type values are inserted and validated
 */
const test::driver::DseLineString GEOMETRY_LINE_STRING[] = {
  test::driver::DseLineString("0.0 0.0, 1.0 1.0"),
  test::driver::DseLineString("1.0 3.0, 2.0 6.0, 3.0 9.0"),
  test::driver::DseLineString("-1.2 -100.0, 0.99 3.0"),
  test::driver::DseLineString("LINESTRING EMPTY"),
  test::driver::DseLineString() // NULL line string
};
template<> const std::vector<test::driver::DseLineString> GeometryTest<test::driver::DseLineString>::values_(
  GEOMETRY_LINE_STRING,
  GEOMETRY_LINE_STRING + sizeof(GEOMETRY_LINE_STRING) / sizeof(GEOMETRY_LINE_STRING[0]));

/**
 * Perform insert and select operations for geo type polygon
 *
 * This test will perform multiple insert and select operations using the
 * geo type polygon values specified against a single node cluster.
 *
 * @jira_ticket CPP-351
 * @jira_ticket CPP-400
 * @test_category dse:geospatial
 * @since 1.0.0
 * @expected_result Geo type values are inserted and validated
 */
const test::driver::DsePolygon GEOMETRY_POLYGON[] = {
  test::driver::DsePolygon("(1.0 3.0, 3.0 1.0, 3.0 6.0, 1.0 3.0)"),
  test::driver::DsePolygon("(0.0 10.0, 10.0 0.0, 10.0 10.0, 0.0 10.0), \
                            (6.0 7.0, 3.0 9.0, 9.0 9.0, 6.0 7.0)"),
  test::driver::DsePolygon("POLYGON EMPTY"),
  test::driver::DsePolygon() // NULL polygon
};
template<> const std::vector<test::driver::DsePolygon> GeometryTest<test::driver::DsePolygon>::values_(
  GEOMETRY_POLYGON,
  GEOMETRY_POLYGON + sizeof(GEOMETRY_POLYGON) / sizeof(GEOMETRY_POLYGON[0]));
