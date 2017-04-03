/*
  Copyright (c) 2017 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include "dse_integration.hpp"

#define GEOMETRY_TABLE_FORMAT "CREATE TABLE %s (id %s PRIMARY KEY, value %s)"
#define GEOMETRY_INSERT_FORMAT "INSERT INTO %s (id, value) VALUES(%s, %s)"
#define GEOMETRY_SELECT_FORMAT "SELECT value FROM %s WHERE id=%s"

/**
 * Geometry (geotypes) integration tests
 *
 * @dse_version 5.0.0
 */
template<class C>
class GeometryTest : public DseIntegration {
public:
  /**
   * Geotype values
   */
  static const std::vector<C> values_;

  void SetUp() {
    CHECK_VERSION(5.0.0);

    // Enable schema metadata to easily create user type (when needed)
    is_schema_metadata_ = true;

    // Call the parent setup function
    dse_workload_.push_back(CCM::DSE_WORKLOAD_GRAPH);
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
    session_.execute(format_string(GEOMETRY_TABLE_FORMAT,
      table_name_.c_str(), cql_type.c_str(), cql_type.c_str()));
    insert_query_ = format_string(GEOMETRY_INSERT_FORMAT, table_name_.c_str(), "?", "?");
    select_query_ = format_string(GEOMETRY_SELECT_FORMAT, table_name_.c_str(), "?");
    prepared_statement_ = session_.prepare(insert_query_);
  }
};
TYPED_TEST_CASE_P(GeometryTest);

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
 * @since 1.0.0
 * @dse_version 5.0.0
 * @expected_result Geotype values are inserted and validated
 */
DSE_INTEGRATION_TYPED_TEST_P(GeometryTest, Basic) {
  CHECK_VERSION(5.0.0);
  default_setup();
  const std::vector<TypeParam>& values = GeometryTest<TypeParam>::values_;

  // Iterate over all the values in the geotype
  for (typename std::vector<TypeParam>::const_iterator it = values.begin();
    it != values.end(); ++it) {
    // Get the current value
    const TypeParam& value = *it;

    // Create both simple and prepared statements
    Statement statements[] = {
      Statement(this->insert_query_, 2),
      this->prepared_statement_.bind()
    };

    // Iterate over all the statements
    for (size_t i = 0; i < ARRAY_LEN(statements); ++i) {
      Statement& statement = statements[i];

      // Bind both the primary key and the value with the geotype and insert
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
 * @expected_result Geotype values are inserted and validated via graph
 *                  operations using a graph array (attached to a graph object)
 */
DSE_INTEGRATION_TYPED_TEST_P(GeometryTest, GraphArray) {
  CHECK_VERSION(5.0.0);
  default_setup();
  const std::vector<TypeParam>& values = GeometryTest<TypeParam>::values_;

  // Iterate over all the values in the geotype and add them to a graph array
  test::driver::DseGraphArray graph_array;
  for (size_t i = 0; i < values.size(); ++i) {
    // Get the value of the geotype being used
    TypeParam value = values[i];

    // Add the value to the graph array
    graph_array.add<TypeParam>(value);
  }

  // Create the statement to insert the geotype using an object with array
  test::driver::DseGraphObject graph_object;
  graph_object.add<test::driver::DseGraphArray>("geotype", graph_array);
  test::driver::DseGraphStatement graph_statement("[geotype]");
  graph_statement.bind(graph_object);

  // Execute the statement and get the result
  test::driver::DseGraphResultSet result_set = this->dse_session_.execute(graph_statement);

  // Assert/Validate the geotype using a graph statement
  CHECK_FAILURE;
  ASSERT_EQ(1ul, result_set.count());
  test::driver::DseGraphResult result = result_set.next();
  ASSERT_TRUE(result.is_type<test::driver::DseGraphArray>());

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
 * @expected_result Geotype values are inserted and validated via graph
 *                  operations using a graph object
 */
DSE_INTEGRATION_TYPED_TEST_P(GeometryTest, GraphObject) {
  CHECK_VERSION(5.0.0);
  default_setup();
  const std::vector<TypeParam>& values = GeometryTest<TypeParam>::values_;

  // Iterate over all the values in the geotype
  for (size_t i = 0; i < values.size(); ++i) {
    // Get the value of the geotype being used
    TypeParam value = values[i];

    // Create the graph statement to insert the geotype using an object
    test::driver::DseGraphObject graph_object;
    graph_object.add<TypeParam>("geotype", value);
    test::driver::DseGraphStatement graph_statement("[geotype]");
    graph_statement.bind(graph_object);

    // Assert/Validate the geotype using a graph statement
    test::driver::DseGraphResultSet result_set = this->dse_session_.execute(graph_statement);
    CHECK_FAILURE;
    ASSERT_EQ(1ul, result_set.count());
    test::driver::DseGraphResult result = result_set.next();
    ASSERT_EQ(value, result.value<TypeParam>());
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
 * @since 1.2.0
 * @dse_version 5.0.0
 * @expected_result Geotype values are inserted using a list and then validated
 *                  via simple and prepared statement operations
 */
DSE_INTEGRATION_TYPED_TEST_P(GeometryTest, List) {
  CHECK_VERSION(5.0.0);

  // Initialize the table and assign the values for the list
  List<TypeParam> list(GeometryTest<TypeParam>::values_);
  initialize("frozen<" + list.cql_type() + ">");

  // Create both simple and prepared statements
  Statement statements[] = {
    Statement(this->insert_query_, 2),
    this->prepared_statement_.bind()
  };

  // Iterate over all the statements
  for (size_t i = 0; i < ARRAY_LEN(statements); ++i) {
    Statement& statement = statements[i];

    // Bind both the primary key and the value with the geotype list and insert
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
 * @since 1.2.0
 * @dse_version 5.0.0
 * @expected_result Geotype values are inserted using a set and then validated
 *                  via simple and prepared statement operations
 */
DSE_INTEGRATION_TYPED_TEST_P(GeometryTest, Set) {
  CHECK_VERSION(5.0.0);

  // Initialize the table and assign the values for the set
  Set<TypeParam> set(GeometryTest<TypeParam>::values_);
  GeometryTest<TypeParam>::initialize("frozen<" + set.cql_type() + ">");

  // Create both simple and prepared statements
  Statement statements[] = {
    Statement(this->insert_query_, 2),
    this->prepared_statement_.bind()
  };

  // Iterate overall all the statements
  for (size_t i = 0; i < ARRAY_LEN(statements); ++i) {
    Statement& statement = statements[i];

    // Bind both the primary key and the value with the geotype set and insert
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
 * @since 1.2.0
 * @dse_version 5.0.0
 * @expected_result Geotype values are inserted using a map and then validated
 *                  via simple and prepared statement operations
 */
DSE_INTEGRATION_TYPED_TEST_P(GeometryTest, Map) {
  CHECK_VERSION(5.0.0);

  // Initialize the table and assign the values for the map
  std::map<TypeParam, TypeParam> map_values;
  const std::vector<TypeParam>& values = GeometryTest<TypeParam>::values_;
  for (typename std::vector<TypeParam>::const_iterator it = values.begin();
    it != values.end(); ++it) {
    map_values[*it] = *it;
  }
  Map<TypeParam, TypeParam> map(map_values);
  initialize("frozen<" + map.cql_type() + ">");

  // Create both simple and prepared statements
  Statement statements[] = {
    Statement(this->insert_query_, 2),
    this->prepared_statement_.bind()
  };

  // Iterate over all the statements
  for (size_t i = 0; i < ARRAY_LEN(statements); ++i) {
    Statement& statement = statements[i];

    // Bind both the primary key and the value with the geotype map and insert
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
 * @since 1.2.0
 * @dse_version 5.0.0
 * @expected_result Geotype values are inserted using a tuple and then
 *                  validated via simple and prepared statement operations
 */
DSE_INTEGRATION_TYPED_TEST_P(GeometryTest, Tuple) {
  CHECK_VERSION(5.0.0);

  // Initialize the table and assign the values for the tuple
  const std::vector<TypeParam>& values = GeometryTest<TypeParam>::values_;
  Tuple tuple(values.size());
  std::string cql_type("tuple<");
  for (size_t i = 0; i < values.size(); ++i) {
    if (i > 0) cql_type.append(", ");
    cql_type.append(values[i].cql_type());
    tuple.set<TypeParam>(values[i], i);
  }
  cql_type.append(">");
  initialize(cql_type);

  // Create both simple and prepared statements
  Statement statements[] = {
    Statement(this->insert_query_, 2),
    this->prepared_statement_.bind()
  };

  // Iterate over all the statements
  for (size_t i = 0; i < ARRAY_LEN(statements); ++i) {
    Statement& statement = statements[i];

    // Bind both the primary key and the value with the geotype tuple and insert
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
 * @since 1.2.0
 * @dse_version 5.0.0
 * @expected_result Geotype values are inserted using a user data type and then
 *                  validated via simple and prepared statement operations
 */
DSE_INTEGRATION_TYPED_TEST_P(GeometryTest, UDT) {
  CHECK_VERSION(5.0.0);

  // Build the UDT type name e.g. udt_pointtype, udt_line_string, etc.
  const std::vector<TypeParam>& values = GeometryTest<TypeParam>::values_;
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
  initialize("frozen<" +  cql_type + ">");

  // Build our UDT values and UDT type
  std::map<std::string, TypeParam> udt_values;
  for (size_t i = 0; i < values.size(); ++i) {
    std::stringstream field;
    field << "field" << i;
    udt_values[field.str()] = values[i];
  }
  UserType user_type(this->session_.schema()
                     .keyspace(this->keyspace_name_)
                     .user_type(cql_type).data_type());

  // Assign/Set the values in the user type
  for (typename std::map<std::string, TypeParam>::const_iterator it = udt_values.begin();
    it != udt_values.end(); ++it) {
    user_type.set<TypeParam>(it->second, it->first);
  }

  // Use both simple and prepared statements
  Statement statements[] = {
    Statement(this->insert_query_, 2),
    this->prepared_statement_.bind()
  };

  for (size_t i = 0; i < ARRAY_LEN(statements); ++i) {
    Statement& statement = statements[i];

    // Bind both the primary key and the value with the geotype UDT and insert
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
REGISTER_TYPED_TEST_CASE_P(GeometryTest, Integration_DSE_Basic,
  Integration_DSE_GraphArray, Integration_DSE_GraphObject,
  Integration_DSE_List, Integration_DSE_Set,
  Integration_DSE_Map, Integration_DSE_Tuple, Integration_DSE_UDT); //TODO: Create expanding macro for registering typed tests

// Instantiate the test case for all the geotypes
typedef testing::Types<test::driver::DsePoint, test::driver::DseLineString, test::driver::DsePolygon> GeoTypes;
INSTANTIATE_TYPED_TEST_CASE_P(Geometry, GeometryTest, GeoTypes);

/**
 * Values for point tests
 */
const test::driver::DsePoint GEOMETRY_POINTS[] = {
  test::driver::DsePoint(0.0, 0.0),
  test::driver::DsePoint(2.0, 4.0),
  test::driver::DsePoint(-1.2, -100.0),
};
template<> const std::vector<test::driver::DsePoint> GeometryTest<test::driver::DsePoint>::values_(
  GEOMETRY_POINTS,
  GEOMETRY_POINTS + ARRAY_LEN(GEOMETRY_POINTS));

/**
 * Values for line string tests
 */
const test::driver::DseLineString GEOMETRY_LINE_STRING[] = {
  test::driver::DseLineString("0.0 0.0, 1.0 1.0"),
  test::driver::DseLineString("1.0 3.0, 2.0 6.0, 3.0 9.0"),
  test::driver::DseLineString("-1.2 -100.0, 0.99 3.0"),
  test::driver::DseLineString("LINESTRING EMPTY"),
};
template<> const std::vector<test::driver::DseLineString> GeometryTest<test::driver::DseLineString>::values_(
  GEOMETRY_LINE_STRING,
  GEOMETRY_LINE_STRING + ARRAY_LEN(GEOMETRY_LINE_STRING));

/**
 * Values for polygon tests
 */
const test::driver::DsePolygon GEOMETRY_POLYGON[] = {
  test::driver::DsePolygon("(1.0 3.0, 3.0 1.0, 3.0 6.0, 1.0 3.0)"),
  test::driver::DsePolygon("(0.0 10.0, 10.0 0.0, 10.0 10.0, 0.0 10.0), \
                            (6.0 7.0, 3.0 9.0, 9.0 9.0, 6.0 7.0)"),
  test::driver::DsePolygon("POLYGON EMPTY"),
};
template<> const std::vector<test::driver::DsePolygon> GeometryTest<test::driver::DsePolygon>::values_(
  GEOMETRY_POLYGON,
  GEOMETRY_POLYGON + ARRAY_LEN(GEOMETRY_POLYGON));

