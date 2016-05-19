/*
  Copyright (c) 2014-2016 DataStax
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
class GeometryIntegrationTest : public DseIntegration {
public:
  /**
   * Geo type values
   */
  static const std::vector<C> values_;

  void SetUp() {
    CHECK_VERSION(5.0.0);

    // Call the parent setup class
    Integration::SetUp();

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
    ASSERT_EQ(1, result.row_count());
    ASSERT_EQ(value, C(result.first_row(), 0));
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
    ASSERT_EQ(1, result.row_count());
    ASSERT_EQ(id, TimeUuid(result.first_row(), 0));
  }
};
TYPED_TEST_CASE_P(GeometryIntegrationTest);

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
TYPED_TEST_P(GeometryIntegrationTest, SimpleStatement) {
  CHECK_VERSION(5.0.0);

  // Iterate over all the values in the geo type
  for (size_t i = 0; i < GeometryIntegrationTest<TypeParam>::values_.size(); ++i) {
    // Get the value of the geo type being used
    TypeParam value = GeometryIntegrationTest<TypeParam>::values_[i];

    // Insert the geo type executed by a CQL query string statement
    TimeUuid id = this->uuid_generator_.generate_timeuuid();
    this->session_.execute(this->format_string(GEOMETRY_INSERT_FORMAT,
      this->table_name_.c_str(), id.cql_value().c_str(), value.cql_value().c_str()));

    // Assert/Validate the geo type
    this->assert_and_validate_geo_type(id, value);

    // Insert the geo type as the primary key executed by a CQL query string
    this->session_.execute(this->format_string(GEOMETRY_INSERT_FORMAT,
      this->table_name_primary_key_.c_str(), value.cql_value().c_str(), id.cql_value().c_str()));

    // Assert/Validate the id where the geo type is the primary key
    this->assert_and_validate_geo_type_primary_key(value, id);

    // Insert the geo type executed by a bounded statement
    Statement statement(this->insert_query_, 2);
    id = this->uuid_generator_.generate_timeuuid();
    statement.bind<TimeUuid>(0, id);
    statement.bind<TypeParam>(1, value);
    this->session_.execute(statement);

    // Assert/Validate the geo type
    this->assert_and_validate_geo_type(id, value);

    // Insert the geo type as the primary key executed by a bounded statement
    statement = Statement(this->insert_query_primary_key_, 2);
    statement.bind<TypeParam>(0, value);
    statement.bind<TimeUuid>(1, id);
    this->session_.execute(statement);

    // Assert/Validate the id where the geo type is the primary key
    this->assert_and_validate_geo_type_primary_key(value, id);
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
TYPED_TEST_P(GeometryIntegrationTest, PreparedStatement) {
  CHECK_VERSION(5.0.0);

  // Iterate over all the values in the geo type
  for (size_t i = 0; i < GeometryIntegrationTest<TypeParam>::values_.size(); ++i) {
    // Get the value of the geo type being used
    TypeParam value = GeometryIntegrationTest<TypeParam>::values_[i];

    // Bind the time UUID and geo type
    Statement statement = this->prepared_statement_.bind();
    TimeUuid id = this->uuid_generator_.generate_timeuuid();
    statement.bind<TimeUuid>(0, id);
    statement.bind<TypeParam>(1, value);
    this->session_.execute(statement);

    // Assert/Validate the geo type
    this->assert_and_validate_geo_type(id, value);

    // Bind the time UUID and geo type where geo type is the primary key
    statement = this->prepared_statement_primary_key_.bind();
    statement.bind<TypeParam>(0, value);
    statement.bind<TimeUuid>(1, id);
    this->session_.execute(statement);

    // Assert/Validate the id where the geo type is the primary key
    this->assert_and_validate_geo_type_primary_key(value, id);
  }
}

// Register all test cases
REGISTER_TYPED_TEST_CASE_P(GeometryIntegrationTest, SimpleStatement, PreparedStatement);

// Instantiate the test case for all the geo types
typedef testing::Types<test::driver::DsePoint, test::driver::DseLineString, test::driver::DsePolygon> GeoTypes;
INSTANTIATE_TYPED_TEST_CASE_P(Geometry, GeometryIntegrationTest, GeoTypes);

/**
 * Perform insert and select operations for geo type point
 *
 * This test will perform multiple insert and select operations using the
 * geo type point values specified against a single node cluster.
 *
 * @jira_ticket CPP-351
 * @test_category dse:geometric
 * @since 1.0.0
 * @expected_result Geo type values are inserted and validated
 */
const test::driver::DsePoint GEOMETRY_POINTS[] = {
  test::driver::DsePoint(0.0, 0.0),
  test::driver::DsePoint(2.0, 4.0),
  test::driver::DsePoint(-1.2, -100.0)
};
template<> const std::vector<test::driver::DsePoint> GeometryIntegrationTest<test::driver::DsePoint>::values_(
  GEOMETRY_POINTS,
  GEOMETRY_POINTS + sizeof(GEOMETRY_POINTS) / sizeof(GEOMETRY_POINTS[0]));

/**
 * Perform insert and select operations for geo type line string
 *
 * This test will perform multiple insert and select operations using the
 * geo type line string values specified against a single node cluster.
 *
 * @jira_ticket CPP-351
 * @test_category dse:geometric
 * @since 1.0.0
 * @expected_result Geo type values are inserted and validated
 */
const test::driver::DseLineString GEOMETRY_LINE_STRING[] = {
  test::driver::DseLineString("0.0 0.0, 1.0 1.0"),
  test::driver::DseLineString("1.0 3.0, 2.0 6.0, 3.0 9.0"),
  test::driver::DseLineString("-1.2 -100.0, 0.99 3.0"),
  test::driver::DseLineString()
};
template<> const std::vector<test::driver::DseLineString> GeometryIntegrationTest<test::driver::DseLineString>::values_(
  GEOMETRY_LINE_STRING,
  GEOMETRY_LINE_STRING + sizeof(GEOMETRY_LINE_STRING) / sizeof(GEOMETRY_LINE_STRING[0]));

/**
 * Perform insert and select operations for geo type polygon
 *
 * This test will perform multiple insert and select operations using the
 * geo type polygon values specified against a single node cluster.
 *
 * @jira_ticket CPP-351
 * @test_category dse:geometric
 * @since 1.0.0
 * @expected_result Geo type values are inserted and validated
 */
const test::driver::DsePolygon GEOMETRY_POLYGON[] = {
  test::driver::DsePolygon("(1.0 3.0, 3.0 1.0, 3.0 6.0, 1.0 3.0)"),
  test::driver::DsePolygon("(0.0 10.0, 10.0 0.0, 10.0 10.0, 0.0 10.0), \
                     (6.0 7.0, 3.0 9.0, 9.0 9.0, 6.0 7.0)"),
  test::driver::DsePolygon()
};
template<> const std::vector<test::driver::DsePolygon> GeometryIntegrationTest<test::driver::DsePolygon>::values_(
  GEOMETRY_POLYGON,
  GEOMETRY_POLYGON + sizeof(GEOMETRY_POLYGON) / sizeof(GEOMETRY_POLYGON[0]));
