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

#include "dse_integration.hpp"

#define DSE_TYPE_TABLE_FORMAT "CREATE TABLE IF NOT EXISTS %s (id %s PRIMARY KEY, value %s)"
#define DSE_TYPE_INSERT_FORMAT "INSERT INTO %s (id, value) VALUES(%s, %s)"
#define DSE_TYPE_SELECT_FORMAT "SELECT value FROM %s WHERE id=%s"

/**
 * DSE type (geotypes and date range) integration tests
 *
 * Note: Geotypes require version DSE 5.0.0+ and date range requires
 * version DSE 5.1.0+.
 *
 * @dse_version 5.0.0+
 */
template <class C>
class DseTypesTest : public DseIntegration {
public:
  /**
   * DSE type values
   */
  static const std::vector<C> values_;

  void SetUp() {
    CHECK_VERSION(5.0.0);

    // Enable schema metadata to easily create user type (when needed)
    is_schema_metadata_ = true;

    // Call the parent setup function
    DseIntegration::SetUp();
  }

protected:
  /**
   * Prepared statement to utilize
   */
  Prepared prepared_statement_;
  /**
   * Pre-formatted insert query
   */
  std::string insert_query_;
  /**
   * Pre-formatted select query
   */
  std::string select_query_;

  /**
   * Default setup for most of the tests
   */
  void default_setup() {
    // Create the table, insert, and select queries
    initialize(values_[0].cql_type());
  }

  /**
   * Create the tables, insert, and select queries for the test
   *
   * @param cql_type CQL value type to use for the tables
   */
  void initialize(const std::string& cql_type) {
    session_.execute(format_string(DSE_TYPE_TABLE_FORMAT, table_name_.c_str(), cql_type.c_str(),
                                   cql_type.c_str()));
    insert_query_ = format_string(DSE_TYPE_INSERT_FORMAT, table_name_.c_str(), "?", "?");
    select_query_ = format_string(DSE_TYPE_SELECT_FORMAT, table_name_.c_str(), "?");
    prepared_statement_ = session_.prepare(insert_query_);
  }
};
TYPED_TEST_CASE_P(DseTypesTest);

/**
 * Perform insert using a simple and prepared statement operation
 *
 * This test will perform multiple inserts using a simple/prepared statement with the
 * parameterized type values statically assigned against a single node cluster.
 *
 * @jira_ticket CPP-351
 * @test_category queries:basic
 * @test_category prepared_statements
 * @test_category dse:geospatial
 * @test_category dse:daterange
 * @since 1.0.0
 * @dse_version 5.0.0+
 * @expected_result DSE values are inserted and validated
 */
DSE_INTEGRATION_TYPED_TEST_P(DseTypesTest, Basic) {
  CHECK_VALUE_TYPE_VERSION(TypeParam);

  this->default_setup();
  const std::vector<TypeParam>& values = DseTypesTest<TypeParam>::values_;

  // Iterate over all the DSE type values
  for (typename std::vector<TypeParam>::const_iterator it = values.begin(); it != values.end();
       ++it) {
    // Get the current value
    const TypeParam& value = *it;

    // Create both simple and prepared statements
    Statement statements[] = { Statement(this->insert_query_, 2),
                               this->prepared_statement_.bind() };

    // Iterate over all the statements
    for (size_t i = 0; i < ARRAY_LEN(statements); ++i) {
      Statement& statement = statements[i];

      // Bind both the primary key and the value with the DSE type and insert
      statement.bind<TypeParam>(0, value);
      statement.bind<TypeParam>(1, value);
      this->session_.execute(statement);

      // Validate the insert and result
      Statement select_statement(this->select_query_, 1);
      select_statement.bind<TypeParam>(0, value);
      Result result = this->session_.execute(select_statement);
      ASSERT_EQ(1u, result.row_count());
      ASSERT_EQ(value, result.first_row().next().as<TypeParam>());
    }
  }
}

/**
 * Perform insert using a collection; list
 *
 * This test will perform multiple inserts using simple and prepared statements
 * with the parameterized type values statically assigned against a single node
 * cluster using a list.
 *
 * @jira_ticket CPP-445
 * @test_category prepared_statements
 * @test_category data_types:collections
 * @test_category dse:geospatial
 * @test_category dse:daterange
 * @since 1.2.0
 * @dse_version 5.0.0+
 * @expected_result DSE values are inserted using a list and then validated
 *                  via simple and prepared statement operations
 */
DSE_INTEGRATION_TYPED_TEST_P(DseTypesTest, List) {
  CHECK_VALUE_TYPE_VERSION(TypeParam);

  // Initialize the table and assign the values for the list
  List<TypeParam> list(DseTypesTest<TypeParam>::values_);
  this->initialize("frozen<" + list.cql_type() + ">");

  // Create both simple and prepared statements
  Statement statements[] = { Statement(this->insert_query_, 2), this->prepared_statement_.bind() };

  // Iterate over all the statements
  for (size_t i = 0; i < ARRAY_LEN(statements); ++i) {
    Statement& statement = statements[i];

    // Bind both the primary key and the value with the DSE type list and insert
    statement.bind<List<TypeParam> >(0, list);
    statement.bind<List<TypeParam> >(1, list);
    this->session_.execute(statement);

    // Validate the result
    Statement select_statement(this->select_query_, 1);
    select_statement.bind<List<TypeParam> >(0, list);
    Result result = this->session_.execute(select_statement);
    ASSERT_EQ(1u, result.row_count());
    List<TypeParam> result_list(result.first_row().next().as<List<TypeParam> >());
    ASSERT_EQ(list.value(), result_list.value());
  }
}

/**
 * Perform insert using a collection; set
 *
 * This test will perform multiple inserts using simple and prepared statements
 * with the parameterized type values statically assigned against a single node
 * cluster using a set.
 *
 * @jira_ticket CPP-445
 * @test_category prepared_statements
 * @test_category data_types:collections
 * @test_category dse:geospatial
 * @test_category dse:daterange
 * @since 1.2.0
 * @dse_version 5.0.0+
 * @expected_result DSE values are inserted using a set and then validated
 *                  via simple and prepared statement operations
 */
DSE_INTEGRATION_TYPED_TEST_P(DseTypesTest, Set) {
  CHECK_VALUE_TYPE_VERSION(TypeParam);

  // Initialize the table and assign the values for the set
  Set<TypeParam> set(DseTypesTest<TypeParam>::values_);
  this->initialize("frozen<" + set.cql_type() + ">");

  // Create both simple and prepared statements
  Statement statements[] = { Statement(this->insert_query_, 2), this->prepared_statement_.bind() };

  // Iterate overall all the statements
  for (size_t i = 0; i < ARRAY_LEN(statements); ++i) {
    Statement& statement = statements[i];

    // Bind both the primary key and the value with the DSE type set and insert
    statement.bind<Set<TypeParam> >(0, set);
    statement.bind<Set<TypeParam> >(1, set);
    this->session_.execute(statement);

    // Validate the result
    Statement select_statement(this->select_query_, 1);
    select_statement.bind<Set<TypeParam> >(0, set);
    Result result = this->session_.execute(select_statement);
    ASSERT_EQ(1u, result.row_count());
    Set<TypeParam> result_set = result.first_row().next().as<Set<TypeParam> >();
    ASSERT_EQ(set.value(), result_set.value());
  }
}

/**
 * Perform insert using a collection; map
 *
 * This test will perform multiple inserts using simple and prepared statements
 * with the parameterized type values statically assigned against a single node
 * cluster using a map.
 *
 * @jira_ticket CPP-445
 * @test_category prepared_statements
 * @test_category data_types:collections
 * @test_category dse:geospatial
 * @test_category dse:daterange
 * @since 1.2.0
 * @dse_version 5.0.0+
 * @expected_result DSE values are inserted using a map and then validated
 *                  via simple and prepared statement operations
 */
DSE_INTEGRATION_TYPED_TEST_P(DseTypesTest, Map) {
  CHECK_VALUE_TYPE_VERSION(TypeParam);

  // Initialize the table and assign the values for the map
  std::map<TypeParam, TypeParam> map_values;
  const std::vector<TypeParam>& values = DseTypesTest<TypeParam>::values_;
  for (typename std::vector<TypeParam>::const_iterator it = values.begin(); it != values.end();
       ++it) {
    map_values[*it] = *it;
  }
  Map<TypeParam, TypeParam> map(map_values);
  this->initialize("frozen<" + map.cql_type() + ">");

  // Create both simple and prepared statements
  Statement statements[] = { Statement(this->insert_query_, 2), this->prepared_statement_.bind() };

  // Iterate over all the statements
  for (size_t i = 0; i < ARRAY_LEN(statements); ++i) {
    Statement& statement = statements[i];

    // Bind both the primary key and the value with the DSE type map and insert
    statement.bind<Map<TypeParam, TypeParam> >(0, map);
    statement.bind<Map<TypeParam, TypeParam> >(1, map);
    this->session_.execute(statement);

    // Validate the result
    Statement select_statement(this->select_query_, 1);
    select_statement.bind<Map<TypeParam, TypeParam> >(0, map);
    Result result = this->session_.execute(select_statement);
    ASSERT_EQ(1u, result.row_count());
    Column column = result.first_row().next();
    Map<TypeParam, TypeParam> result_map(column.as<Map<TypeParam, TypeParam> >());
    ASSERT_EQ(map_values, result_map.value());
  }
}

/**
 * Perform insert using a tuple
 *
 * This test will perform multiple inserts using simple and prepared statements
 * with the parameterized type values statically assigned against a single node
 * cluster using a tuple.
 *
 * @jira_ticket CPP-445
 * @test_category prepared_statements
 * @test_category data_types:tuple
 * @test_category dse:geospatial
 * @test_category dse:daterange
 * @since 1.2.0
 * @dse_version 5.0.0+
 * @expected_result DSE values are inserted using a tuple and then
 *                  validated via simple and prepared statement operations
 */
DSE_INTEGRATION_TYPED_TEST_P(DseTypesTest, Tuple) {
  CHECK_VALUE_TYPE_VERSION(TypeParam);

  // Initialize the table and assign the values for the tuple
  const std::vector<TypeParam>& values = DseTypesTest<TypeParam>::values_;
  Tuple tuple(values.size());
  std::string cql_type("tuple<");
  for (size_t i = 0; i < values.size(); ++i) {
    if (i > 0) cql_type.append(", ");
    cql_type.append(values[i].cql_type());
    tuple.set<TypeParam>(values[i], i);
  }
  cql_type.append(">");
  this->initialize(cql_type);

  // Create both simple and prepared statements
  Statement statements[] = { Statement(this->insert_query_, 2), this->prepared_statement_.bind() };

  // Iterate over all the statements
  for (size_t i = 0; i < ARRAY_LEN(statements); ++i) {
    Statement& statement = statements[i];

    // Bind both the primary key and the value with the DSE type tuple and insert
    statement.bind<Tuple>(0, tuple);
    statement.bind<Tuple>(1, tuple);
    this->session_.execute(statement);

    // Validate the result
    Statement select_statement(this->select_query_, 1);
    select_statement.bind<Tuple>(0, tuple);
    Result result = this->session_.execute(select_statement);
    ASSERT_EQ(1u, result.row_count());
    Tuple result_tuple(result.first_row().next().as<Tuple>());
    ASSERT_EQ(values, result_tuple.values<TypeParam>());
  }
}

/**
 * Perform insert using a user data type
 *
 * This test will perform multiple inserts using simple and prepared statements
 * with the parameterized type values statically assigned against a single node
 * cluster using a tuple.
 *
 * @jira_ticket CPP-445
 * @test_category prepared_statements
 * @test_category data_types:udt
 * @test_category dse:geospatial
 * @test_category dse:daterange
 * @since 1.2.0
 * @dse_version 5.0.0+
 * @expected_result DSE values are inserted using a user data type and then
 *                  validated via simple and prepared statement operations
 */
DSE_INTEGRATION_TYPED_TEST_P(DseTypesTest, UDT) {
  CHECK_VALUE_TYPE_VERSION(TypeParam);

  // Build the UDT type name e.g. udt_pointtype, udt_line_string, etc.
  const std::vector<TypeParam>& values = DseTypesTest<TypeParam>::values_;
  std::string cql_type("udt_" + Utils::to_lower(Utils::replace_all(values[0].cql_type(), "'", "")));

  // Create the UDT type
  std::string create_type("CREATE TYPE ");
  create_type.append(cql_type);
  create_type.append(" (");
  for (size_t i = 0; i < values.size(); ++i) {
    if (i > 0) create_type.append(", ");
    std::stringstream field;
    field << "field" << i;
    create_type.append(field.str());
    create_type.append(" ");
    create_type.append(values[i].cql_type());
  }
  create_type.append(")");
  this->session_.execute(create_type);

  // Initialize the table; NOTE: UDT must be frozen for older versions of DSE
  this->initialize("frozen<" + cql_type + ">");

  // Build our UDT values and UDT type
  std::map<std::string, TypeParam> udt_values;
  for (size_t i = 0; i < values.size(); ++i) {
    std::stringstream field;
    field << "field" << i;
    udt_values[field.str()] = values[i];
  }
  UserType user_type(
      this->session_.schema().keyspace(this->keyspace_name_).user_type(cql_type).data_type());

  // Assign/Set the values in the user type
  for (typename std::map<std::string, TypeParam>::const_iterator it = udt_values.begin();
       it != udt_values.end(); ++it) {
    user_type.set<TypeParam>(it->second, it->first);
  }

  // Use both simple and prepared statements
  Statement statements[] = { Statement(this->insert_query_, 2), this->prepared_statement_.bind() };

  for (size_t i = 0; i < ARRAY_LEN(statements); ++i) {
    Statement& statement = statements[i];

    // Bind both the primary key and the value with the DSE type UDT and insert
    statement.bind<UserType>(0, user_type);
    statement.bind<UserType>(1, user_type);
    this->session_.execute(statement);

    // Validate the result
    Statement select_statement(this->select_query_, 1);
    select_statement.bind<UserType>(0, user_type);
    Result result = this->session_.execute(select_statement);
    ASSERT_EQ(1u, result.row_count());
    UserType result_udt_values(result.first_row().next().as<UserType>());
    ASSERT_EQ(udt_values, result_udt_values.values<TypeParam>());
  }
}

// Register all test cases
REGISTER_TYPED_TEST_CASE_P(DseTypesTest, Integration_DSE_Basic, Integration_DSE_List,
                           Integration_DSE_Set, Integration_DSE_Map, Integration_DSE_Tuple,
                           Integration_DSE_UDT);

// Instantiate the test case for all the geotypes and date range
typedef testing::Types<dse::Point, dse::LineString, dse::Polygon, dse::DateRange> DseTypes;
INSTANTIATE_TYPED_TEST_CASE_P(DseTypes, DseTypesTest, DseTypes);

/**
 * Values for point tests
 */
const dse::Point GEOMETRY_POINTS[] = { dse::Point("0.0, 0.0"), dse::Point("2.0, 4.0"),
                                       dse::Point("-1.2, -100.0") };
template <>
const std::vector<dse::Point> DseTypesTest<dse::Point>::values_(GEOMETRY_POINTS,
                                                                GEOMETRY_POINTS +
                                                                    ARRAY_LEN(GEOMETRY_POINTS));

/**
 * Values for line string tests
 */
const dse::LineString GEOMETRY_LINE_STRING[] = { dse::LineString("0.0 0.0, 1.0 1.0"),
                                                 dse::LineString("1.0 3.0, 2.0 6.0, 3.0 9.0"),
                                                 dse::LineString("-1.2 -100.0, 0.99 3.0"),
                                                 dse::LineString("LINESTRING EMPTY") };
template <>
const std::vector<dse::LineString>
    DseTypesTest<dse::LineString>::values_(GEOMETRY_LINE_STRING,
                                           GEOMETRY_LINE_STRING + ARRAY_LEN(GEOMETRY_LINE_STRING));

/**
 * Values for polygon tests
 */
const dse::Polygon GEOMETRY_POLYGON[] = {
  dse::Polygon("(1.0 3.0, 3.0 1.0, 3.0 6.0, 1.0 3.0)"),
  dse::Polygon("(0.0 10.0, 10.0 0.0, 10.0 10.0, 0.0 10.0), (6.0 7.0, 3.0 9.0, 9.0 9.0, 6.0 7.0)"),
  dse::Polygon("POLYGON EMPTY")
};
template <>
const std::vector<dse::Polygon>
    DseTypesTest<dse::Polygon>::values_(GEOMETRY_POLYGON,
                                        GEOMETRY_POLYGON + ARRAY_LEN(GEOMETRY_POLYGON));

/**
 * Values for date range tests
 */
const dse::DateRange DATE_RANGES[] = {
  // Single dates
  dse::DateRange(values::dse::DateRange(DSE_DATE_RANGE_PRECISION_YEAR, "1970")),
  dse::DateRange(values::dse::DateRange(DSE_DATE_RANGE_PRECISION_YEAR, "2017")),
  dse::DateRange(values::dse::DateRange(DSE_DATE_RANGE_PRECISION_MONTH, "04/2017")),
  dse::DateRange(values::dse::DateRange(DSE_DATE_RANGE_PRECISION_DAY, "04/14/2017")),
  dse::DateRange(values::dse::DateRange(DSE_DATE_RANGE_PRECISION_HOUR, "01:00 01/01/1970")),
  dse::DateRange(values::dse::DateRange(DSE_DATE_RANGE_PRECISION_HOUR, "23:00 04/14/2017")),
  dse::DateRange(values::dse::DateRange(DSE_DATE_RANGE_PRECISION_MINUTE, "23:59 04/14/2017")),
  dse::DateRange(values::dse::DateRange(DSE_DATE_RANGE_PRECISION_SECOND, "00:00:01 01/01/1970")),
  dse::DateRange(values::dse::DateRange(DSE_DATE_RANGE_PRECISION_SECOND, "23:59:59 04/14/2017")),
  dse::DateRange(values::dse::DateRange(values::dse::DateRangeBound(0))),
  dse::DateRange(values::dse::DateRange(values::dse::DateRangeBound(1000))),
  dse::DateRange(values::dse::DateRange(values::dse::DateRangeBound(1))),

  // Single date unbounded
  dse::DateRange(values::dse::DateRange(values::dse::DateRangeBound::unbounded())),

  // Upper and lower bounds
  dse::DateRange(values::dse::DateRange(DSE_DATE_RANGE_PRECISION_YEAR, "1970",
                                        DSE_DATE_RANGE_PRECISION_YEAR, "2017")),
  dse::DateRange(values::dse::DateRange(DSE_DATE_RANGE_PRECISION_MONTH, "02/1970",
                                        DSE_DATE_RANGE_PRECISION_MONTH, "08/2017")),
  dse::DateRange(values::dse::DateRange(DSE_DATE_RANGE_PRECISION_DAY, "4/14/1970",
                                        DSE_DATE_RANGE_PRECISION_DAY, "8/14/2017")),
  dse::DateRange(values::dse::DateRange(DSE_DATE_RANGE_PRECISION_HOUR, "01:00 4/14/1970",
                                        DSE_DATE_RANGE_PRECISION_HOUR, "12:00 8/14/2017")),
  dse::DateRange(values::dse::DateRange(DSE_DATE_RANGE_PRECISION_MINUTE, "01:01 2/28/1970",
                                        DSE_DATE_RANGE_PRECISION_MINUTE, "12:12 4/14/2017")),
  dse::DateRange(values::dse::DateRange(DSE_DATE_RANGE_PRECISION_SECOND, "01:01:01 4/14/1970",
                                        DSE_DATE_RANGE_PRECISION_SECOND, "12:12:12 4/14/2017")),
  dse::DateRange(
      values::dse::DateRange(values::dse::DateRangeBound(1), values::dse::DateRangeBound(1000))),

  // Upper and lower bounds mixed precisions
  dse::DateRange(values::dse::DateRange(DSE_DATE_RANGE_PRECISION_SECOND, "01:01:01 4/14/1970",
                                        DSE_DATE_RANGE_PRECISION_MONTH, "04/2017")),
  dse::DateRange(values::dse::DateRange(DSE_DATE_RANGE_PRECISION_YEAR, "2017",
                                        DSE_DATE_RANGE_PRECISION_MONTH, "04/2017")),

  // Lower unbounded
  dse::DateRange(values::dse::DateRange(
      values::dse::DateRangeBound::unbounded(),
      values::dse::DateRangeBound::upper(DSE_DATE_RANGE_PRECISION_YEAR, "2017"))),
  dse::DateRange(values::dse::DateRange(
      values::dse::DateRangeBound::unbounded(),
      values::dse::DateRangeBound::upper(DSE_DATE_RANGE_PRECISION_MONTH, "08/2017"))),
  dse::DateRange(values::dse::DateRange(
      values::dse::DateRangeBound::unbounded(),
      values::dse::DateRangeBound::upper(DSE_DATE_RANGE_PRECISION_DAY, "8/14/2017"))),
  dse::DateRange(values::dse::DateRange(
      values::dse::DateRangeBound::unbounded(),
      values::dse::DateRangeBound::upper(DSE_DATE_RANGE_PRECISION_HOUR, "12:00 8/14/2017"))),
  dse::DateRange(values::dse::DateRange(
      values::dse::DateRangeBound::unbounded(),
      values::dse::DateRangeBound::upper(DSE_DATE_RANGE_PRECISION_MINUTE, "12:12 4/14/2017"))),
  dse::DateRange(values::dse::DateRange(
      values::dse::DateRangeBound::unbounded(),
      values::dse::DateRangeBound::upper(DSE_DATE_RANGE_PRECISION_SECOND, "12:12:12 4/14/2017"))),
  dse::DateRange(values::dse::DateRange(values::dse::DateRangeBound::unbounded(),
                                        values::dse::DateRangeBound(1000))),

  // Upper unbounded
  dse::DateRange(values::dse::DateRange(
      values::dse::DateRangeBound::lower(DSE_DATE_RANGE_PRECISION_YEAR, "1970"),
      values::dse::DateRangeBound::unbounded())),
  dse::DateRange(values::dse::DateRange(
      values::dse::DateRangeBound::lower(DSE_DATE_RANGE_PRECISION_MONTH, "02/1970"),
      values::dse::DateRangeBound::unbounded())),
  dse::DateRange(values::dse::DateRange(
      values::dse::DateRangeBound::lower(DSE_DATE_RANGE_PRECISION_DAY, "4/14/1970"),
      values::dse::DateRangeBound::unbounded())),
  dse::DateRange(values::dse::DateRange(
      values::dse::DateRangeBound::lower(DSE_DATE_RANGE_PRECISION_HOUR, "01:00 4/14/1970"),
      values::dse::DateRangeBound::unbounded())),
  dse::DateRange(values::dse::DateRange(
      values::dse::DateRangeBound::lower(DSE_DATE_RANGE_PRECISION_MINUTE, "01:01 2/28/1970"),
      values::dse::DateRangeBound::unbounded())),
  dse::DateRange(values::dse::DateRange(
      values::dse::DateRangeBound::lower(DSE_DATE_RANGE_PRECISION_SECOND, "01:01:01 4/14/1970"),
      values::dse::DateRangeBound::unbounded())),
  dse::DateRange(values::dse::DateRange(values::dse::DateRangeBound(1),
                                        values::dse::DateRangeBound::unbounded()))
};

template <>
const std::vector<dse::DateRange>
    DseTypesTest<dse::DateRange>::values_(DATE_RANGES, DATE_RANGES + ARRAY_LEN(DATE_RANGES));
