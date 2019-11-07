/*
  Copyright (c) 2014-2017 DataStax

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

#include "integration.hpp"

/**
 * Cassandra type integration tests
 */
template <class C>
class CassandraTypesTests : public Integration {
public:
  /**
   * Cassandra type values
   */
  static const std::vector<C> values_;

  CassandraTypesTests()
      : is_key_allowed_(true) {}

  void SetUp() {
    // Enable schema metadata to easily create user type (when needed)
    is_schema_metadata_ = true;

    // Determine additional circumstances not allowable for a data type
    if (values_[0].cql_type().compare("duration") == 0) {
      is_key_allowed_ = false;
    }

    // Call the parent setup function
    Integration::SetUp();
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
   * Flag to determine if data type as primary/map key is allowed
   */
  bool is_key_allowed_;

  /**
   * Default setup for most of the tests
   */
  void default_setup(bool is_named = false) {
    // Create the table, insert, and select queries
    initialize(values_[0].cql_type(), is_named);
  }

  /**
   * Create the tables, insert, and select queries for the test
   *
   * @param cql_type CQL value type to use for the tables
   */
  void initialize(const std::string& cql_type, bool is_named = false) {
    if (is_key_allowed_) {
      session_.execute(format_string(CASSANDRA_KEY_VALUE_TABLE_FORMAT, table_name_.c_str(),
                                     cql_type.c_str(), cql_type.c_str()));
    } else {
      session_.execute(format_string(CASSANDRA_KEY_VALUE_TABLE_FORMAT, table_name_.c_str(), "int",
                                     cql_type.c_str()));
    }
    insert_query_ =
        format_string(CASSANDRA_KEY_VALUE_INSERT_FORMAT, table_name_.c_str(),
                      (is_named ? ":named_key" : "?"), (is_named ? ":named_value" : "?"));
    select_query_ = format_string(CASSANDRA_SELECT_VALUE_FORMAT, table_name_.c_str(),
                                  (is_named ? ":named_key" : "?"));
    prepared_statement_ = session_.prepare(insert_query_);
  }
};
TYPED_TEST_CASE_P(CassandraTypesTests);

/**
 * Specialized duration integration test extension
 */
class CassandraTypesDurationTests : public CassandraTypesTests<Duration> {};

/**
 * Perform insert using a simple and prepared statement operation
 *
 * This test will perform multiple inserts using a simple/prepared statement
 * with the parameterized type values statically assigned against a single node
 * cluster.
 *
 * @test_category queries:basic
 * @test_category prepared_statements
 * @test_category data_types:primitive
 * @since core:1.0.0
 * @expected_result Cassandra values are inserted and validated
 */
CASSANDRA_INTEGRATION_TYPED_TEST_P(CassandraTypesTests, Basic) {
  CHECK_VALUE_TYPE_VERSION(TypeParam);

  this->default_setup();
  const std::vector<TypeParam>& values = CassandraTypesTests<TypeParam>::values_;

  // Iterate over all the Cassandra type values
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

      // Bind both the primary key and the value with the Cassandra type and insert
      if (this->is_key_allowed_) {
        statement.bind<TypeParam>(0, value);
      } else {
        statement.bind<Integer>(0, Integer(i));
      }
      statement.bind<TypeParam>(1, value);
      this->session_.execute(statement);

      // Validate the insert and result
      Statement select_statement(this->select_query_, 1);
      if (this->is_key_allowed_) {
        select_statement.bind<TypeParam>(0, value);
      } else {
        select_statement.bind<Integer>(0, Integer(i));
      }
      Result result = this->session_.execute(select_statement);
      ASSERT_EQ(1u, result.row_count());
      ASSERT_EQ(1u, result.column_count());
      ASSERT_EQ(value, result.first_row().next().as<TypeParam>());
    }
  }
}

/**
 * Perform insert by name using a simple and prepared statement operation
 *
 * This test will perform multiple inserts by name using a simple/prepared
 * statement with the parameterized type values statically assigned against a
 * single node cluster.
 *
 * @test_category queries:basic
 * @test_category prepared_statements
 * @test_category data_types:primitive
 * @since core:1.0.0
 * @expected_result Cassandra values are inserted and validated
 */
CASSANDRA_INTEGRATION_TYPED_TEST_P(CassandraTypesTests, ByName) {
  CHECK_VALUE_TYPE_VERSION(TypeParam);

  this->default_setup();
  const std::vector<TypeParam>& values = CassandraTypesTests<TypeParam>::values_;

  // Iterate over all the Cassandra type values
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

      // Bind both the primary key and the value with the Cassandra type and insert
      if (this->is_key_allowed_) {
        statement.bind<TypeParam>("key", value);
      } else {
        statement.bind<Integer>("key", Integer(i));
      }
      statement.bind<TypeParam>("value", value);
      this->session_.execute(statement);

      // Validate the insert and result
      Statement select_statement(this->select_query_, 1);
      if (this->is_key_allowed_) {
        select_statement.bind<TypeParam>("key", value);
      } else {
        select_statement.bind<Integer>("key", Integer(i));
      }
      Result result = this->session_.execute(select_statement);
      ASSERT_EQ(1u, result.row_count());
      ASSERT_EQ(value, result.first_row().next().as<TypeParam>());
    }
  }
}

/**
 * Perform insert by named parameter using a simple and prepared statement operation
 *
 * This test will perform multiple inserts with named parameter using a simple/prepared
 * statement with the parameterized type values statically assigned against a single node cluster.
 *
 * @test_category queries:basic
 * @test_category prepared_statements
 * @test_category data_types:primitive
 * @test_category queries:named_parameters
 * @since core:2.10.0-beta
 * @jira_ticket CPP-263
 * @expected_result Cassandra values are inserted and validated
 */
CASSANDRA_INTEGRATION_TYPED_TEST_P(CassandraTypesTests, NamedParameters) {
  CHECK_VERSION(2.1.0);
  CHECK_VALUE_TYPE_VERSION(TypeParam);

  this->default_setup(true);
  const std::vector<TypeParam>& values = CassandraTypesTests<TypeParam>::values_;

  // Iterate over all the Cassandra type values
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

      // Bind both the primary key and the value with the Cassandra type and insert
      if (this->is_key_allowed_) {
        statement.bind<TypeParam>("named_key", value);
      } else {
        statement.bind<Integer>("named_key", Integer(i));
      }
      statement.bind<TypeParam>("named_value", value);
      this->session_.execute(statement);

      // Validate the insert and result
      Statement select_statement(this->select_query_, 1);
      if (this->is_key_allowed_) {
        select_statement.bind<TypeParam>("named_key", value);
      } else {
        select_statement.bind<Integer>("named_key", Integer(i));
      }
      Result result = this->session_.execute(select_statement);
      ASSERT_EQ(1u, result.row_count());
      ASSERT_EQ(value, result.first_row().next().as<TypeParam>());
    }
  }
}

/**
 * Perform NULL value inserts using a simple and prepared statement operation
 *
 * This test will perform multiple NULL inserts using a simple/prepared
 * statement with the parameterized type against a single node cluster.
 *
 * @test_category queries:basic
 * @test_category prepared_statements
 * @test_category data_types:primitive
 * @since core:1.0.0
 * @expected_result Cassandra NULL values are inserted and validated
 */
CASSANDRA_INTEGRATION_TYPED_TEST_P(CassandraTypesTests, NullValues) {
  CHECK_VALUE_TYPE_VERSION(TypeParam);

  this->is_key_allowed_ = false; // Ensure the TypeParam is not allowed as a key
  this->default_setup();

  // Create both simple and prepared statements
  Statement statements[] = { Statement(this->insert_query_, 2), this->prepared_statement_.bind() };

  // Iterate over all the statements
  for (size_t i = 0; i < ARRAY_LEN(statements); ++i) {
    Statement& statement = statements[i];
    TypeParam null_value;

    // Bind the NULL value with the Cassandra type and insert
    statement.bind<Integer>(0, Integer(i));
    statement.bind<TypeParam>(1, null_value);
    this->session_.execute(statement);

    // Validate the insert and result
    Statement select_statement(this->select_query_, 1);
    select_statement.bind<Integer>(0, Integer(i));
    Result result = this->session_.execute(select_statement);
    ASSERT_EQ(1u, result.row_count());
    TypeParam select_value = result.first_row().next().as<TypeParam>();
    ASSERT_EQ(null_value, select_value);
    ASSERT_TRUE(select_value.is_null());
  }
}

/**
 * Perform insert using a NULL list collection
 *
 * This test will perform multiple NULL inserts using a simple/prepared
 * statement with the parameterized type inside a list collection against a
 * single node cluster.
 *
 * @test_category queries:basic
 * @test_category prepared_statements
 * @test_category data_types:collections
 * @since core:1.0.0
 * @expected_result Cassandra NULL values are inserted and validated
 */
CASSANDRA_INTEGRATION_TYPED_TEST_P(CassandraTypesTests, NullList) {
  CHECK_VALUE_TYPE_VERSION(TypeParam);

  this->is_key_allowed_ = false; // Ensure the TypeParam is not allowed as a key
  this->default_setup();

  // Create both simple and prepared statements
  Statement statements[] = { Statement(this->insert_query_, 2), this->prepared_statement_.bind() };

  // Iterate over all the statements
  for (size_t i = 0; i < ARRAY_LEN(statements); ++i) {
    Statement& statement = statements[i];
    test::driver::List<TypeParam> value;

    // Bind the NULL collection and insert
    statement.bind<Integer>(0, Integer(i));
    statement.bind<test::driver::List<TypeParam> >(1, value);
    this->session_.execute(statement);

    // Validate the insert and result
    Statement select_statement(this->select_query_, 1);
    select_statement.bind<Integer>(0, Integer(i));
    Result result = this->session_.execute(select_statement);
    ASSERT_EQ(1u, result.row_count());
    test::driver::List<TypeParam> select_value =
        result.first_row().next().as<test::driver::List<TypeParam> >();
    ASSERT_EQ(value, select_value);
    ASSERT_TRUE(select_value.is_null());
  }
}

/**
 *Perform insert using a NULL map collection
 *
 * This test will perform multiple NULL inserts using a simple/prepared
 * statement with the parameterized type inside a map collection against a
 * single node cluster.
 *
 * @test_category queries:basic
 * @test_category prepared_statements
 * @test_category data_types:collections
 * @since core:1.0.0
 * @expected_result Cassandra NULL values are inserted and validated
 */
CASSANDRA_INTEGRATION_TYPED_TEST_P(CassandraTypesTests, NullMap) {
  CHECK_VALUE_TYPE_VERSION(TypeParam);

  this->is_key_allowed_ = false; // Ensure the TypeParam is not allowed as a key
  this->default_setup();

  // Create both simple and prepared statements
  Statement statements[] = { Statement(this->insert_query_, 2), this->prepared_statement_.bind() };

  // Iterate over all the statements
  for (size_t i = 0; i < ARRAY_LEN(statements); ++i) {
    Statement& statement = statements[i];
    test::driver::Map<TypeParam, TypeParam> value;

    // Bind the NULL collection and insert
    statement.bind<Integer>(0, Integer(i));
    statement.bind<test::driver::Map<TypeParam, TypeParam> >(1, value);
    this->session_.execute(statement);

    // Validate the insert and result
    Statement select_statement(this->select_query_, 1);
    select_statement.bind<Integer>(0, Integer(i));
    Result result = this->session_.execute(select_statement);
    ASSERT_EQ(1u, result.row_count());
    test::driver::Map<TypeParam, TypeParam> select_value =
        result.first_row().next().as<test::driver::Map<TypeParam, TypeParam> >();
    ASSERT_EQ(value, select_value);
    ASSERT_TRUE(select_value.is_null());
  }
}

/**
 * Perform insert using a NULL set collection
 *
 * This test will perform multiple NULL inserts using a simple/prepared
 * statement with the parameterized type inside a set collection against a
 * single node cluster.
 *
 * @test_category queries:basic
 * @test_category prepared_statements
 * @test_category data_types:collections
 * @since core:1.0.0
 * @expected_result Cassandra NULL values are inserted and validated
 */
CASSANDRA_INTEGRATION_TYPED_TEST_P(CassandraTypesTests, NullSet) {
  CHECK_VALUE_TYPE_VERSION(TypeParam);

  this->is_key_allowed_ = false; // Ensure the TypeParam is not allowed as a key
  this->default_setup();

  // Create both simple and prepared statements
  Statement statements[] = { Statement(this->insert_query_, 2), this->prepared_statement_.bind() };

  // Iterate over all the statements
  for (size_t i = 0; i < ARRAY_LEN(statements); ++i) {
    Statement& statement = statements[i];
    test::driver::Set<TypeParam> value;

    // Bind the NULL collection and insert
    statement.bind<Integer>(0, Integer(i));
    statement.bind<test::driver::Set<TypeParam> >(1, value);
    this->session_.execute(statement);

    // Validate the insert and result
    Statement select_statement(this->select_query_, 1);
    select_statement.bind<Integer>(0, Integer(i));
    Result result = this->session_.execute(select_statement);
    ASSERT_EQ(1u, result.row_count());
    test::driver::Set<TypeParam> select_value =
        result.first_row().next().as<test::driver::Set<TypeParam> >();
    ASSERT_EQ(value, select_value);
    ASSERT_TRUE(select_value.is_null());
  }
}

/**
 * Perform insert using a list collection
 *
 * This test will perform multiple inserts using simple and prepared statements
 * with the parameterized type values statically assigned against a single node
 * cluster using a list.
 *
 * @test_category queries:basic
 * @test_category prepared_statements
 * @test_category data_types:collections
 * @since core 1.0.0
 * @expected_result Cassandra values are inserted using a list and then
 *                  validated via simple and prepared statement operations
 */
CASSANDRA_INTEGRATION_TYPED_TEST_P(CassandraTypesTests, List) {
  CHECK_VALUE_TYPE_VERSION(TypeParam);

  // Initialize the table and assign the values for the list
  List<TypeParam> list(CassandraTypesTests<TypeParam>::values_);
  this->initialize("frozen<" + list.cql_type() + ">");

  // Create both simple and prepared statements
  Statement statements[] = { Statement(this->insert_query_, 2), this->prepared_statement_.bind() };

  // Iterate over all the statements
  for (size_t i = 0; i < ARRAY_LEN(statements); ++i) {
    Statement& statement = statements[i];

    // Bind both the primary key and the value with the Cassandra type list and insert
    if (this->is_key_allowed_) {
      statement.bind<List<TypeParam> >(0, list);
    } else {
      statement.bind<Integer>(0, Integer(i));
    }
    statement.bind<List<TypeParam> >(1, list);
    this->session_.execute(statement);

    // Validate the result
    Statement select_statement(this->select_query_, 1);
    if (this->is_key_allowed_) {
      select_statement.bind<List<TypeParam> >(0, list);
    } else {
      select_statement.bind<Integer>(0, Integer(i));
    }
    Result result = this->session_.execute(select_statement);
    ASSERT_EQ(1u, result.row_count());
    List<TypeParam> result_list(result.first_row().next().as<List<TypeParam> >());
    ASSERT_EQ(list.value(), result_list.value());
  }
}

/**
 * Perform insert using a set collection
 *
 * This test will perform multiple inserts using simple and prepared statements
 * with the parameterized type values statically assigned against a single node
 * cluster using a set.
 *
 * @test_category queries:basic
 * @test_category prepared_statements
 * @test_category data_types:collections
 * @since core 1.0.0
 * @expected_result Cassandra values are inserted using a set and then validated
 *                  via simple and prepared statement operations
 */
CASSANDRA_INTEGRATION_TYPED_TEST_P(CassandraTypesTests, Set) {
  CHECK_VALUE_TYPE_VERSION(TypeParam);
  if (CassandraTypesTests<TypeParam>::values_[0].cql_type().compare("duration") == 0) {
    SKIP_TEST("Unsupported CQL Type Duration: Set does not support duration");
  }

  // Initialize the table and assign the values for the set
  Set<TypeParam> set(CassandraTypesTests<TypeParam>::values_);
  this->initialize("frozen<" + set.cql_type() + ">");

  // Create both simple and prepared statements
  Statement statements[] = { Statement(this->insert_query_, 2), this->prepared_statement_.bind() };

  // Iterate overall all the statements
  for (size_t i = 0; i < ARRAY_LEN(statements); ++i) {
    Statement& statement = statements[i];

    // Bind both the primary key and the value with the Cassandra type set and insert
    if (this->is_key_allowed_) {
      statement.bind<Set<TypeParam> >(0, set);
    } else {
      statement.bind<Integer>(0, Integer(i));
    }
    statement.bind<Set<TypeParam> >(1, set);
    this->session_.execute(statement);

    // Validate the result
    Statement select_statement(this->select_query_, 1);
    if (this->is_key_allowed_) {
      select_statement.bind<Set<TypeParam> >(0, set);
    } else {
      select_statement.bind<Integer>(0, Integer(i));
    }
    Result result = this->session_.execute(select_statement);
    ASSERT_EQ(1u, result.row_count());
    Set<TypeParam> result_set = result.first_row().next().as<Set<TypeParam> >();
    ASSERT_EQ(set.value(), result_set.value());
  }
}

/**
 * Perform insert using a map collection
 *
 * This test will perform multiple inserts using simple and prepared statements
 * with the parameterized type values statically assigned against a single node
 * cluster using a map.
 *
 * @test_category queries:basic
 * @test_category prepared_statements
 * @test_category data_types:collections
 * @since core 1.0.0
 * @expected_result Cassandra values are inserted using a map and then validated
 *                  via simple and prepared statement operations
 */
CASSANDRA_INTEGRATION_TYPED_TEST_P(CassandraTypesTests, Map) {
  CHECK_VALUE_TYPE_VERSION(TypeParam);

  // TODO(fero): Move this into its own parameterized method or keep this branching?
  if (this->is_key_allowed_) {
    // Initialize the table and assign the values for the map
    std::map<TypeParam, TypeParam> map_values;
    const std::vector<TypeParam>& values = CassandraTypesTests<TypeParam>::values_;
    for (typename std::vector<TypeParam>::const_iterator it = values.begin(); it != values.end();
         ++it) {
      map_values[*it] = *it;
    }
    Map<TypeParam, TypeParam> map(map_values);
    this->initialize("frozen<" + map.cql_type() + ">");

    // Create both simple and prepared statements
    Statement statements[] = { Statement(this->insert_query_, 2),
                               this->prepared_statement_.bind() };

    // Iterate over all the statements
    for (size_t i = 0; i < ARRAY_LEN(statements); ++i) {
      Statement& statement = statements[i];

      // Bind both the primary key and the value with the Cassandra type map and insert
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
  } else {
    // Initialize the table and assign the values for the map
    std::map<Integer, TypeParam> map_values;
    const std::vector<TypeParam>& values = CassandraTypesTests<TypeParam>::values_;
    cass_int32_t count = 1;
    for (typename std::vector<TypeParam>::const_iterator it = values.begin(); it != values.end();
         ++it) {
      map_values[Integer(count++)] = *it;
    }
    Map<Integer, TypeParam> map(map_values);
    this->initialize("frozen<" + map.cql_type() + ">");

    // Create both simple and prepared statements
    Statement statements[] = { Statement(this->insert_query_, 2),
                               this->prepared_statement_.bind() };

    // Iterate over all the statements
    for (size_t i = 0; i < ARRAY_LEN(statements); ++i) {
      Statement& statement = statements[i];

      // Bind both the primary key and the value with the Cassandra type map and insert
      statement.bind<Integer>(0, Integer(i));
      statement.bind<Map<Integer, TypeParam> >(1, map);
      this->session_.execute(statement);

      // Validate the result
      Statement select_statement(this->select_query_, 1);
      select_statement.bind<Integer>(0, Integer(i));
      Result result = this->session_.execute(select_statement);
      ASSERT_EQ(1u, result.row_count());
      Column column = result.first_row().next();
      Map<Integer, TypeParam> result_map(column.as<Map<Integer, TypeParam> >());
      ASSERT_EQ(map_values, result_map.value());
    }
  }
}

/**
 * Perform insert using a tuple
 *
 * This test will perform multiple inserts using simple and prepared statements
 * with the parameterized type values statically assigned against a single node
 * cluster using a tuple.
 *
 * @test_category queries:basic
 * @test_category prepared_statements
 * @test_category data_types:tuple
 * @since core 1.0.0
 * @cassandra_version 2.1.0
 * @expected_result Cassandra values are inserted using a tuple and then
 *                  validated via simple and prepared statement operations
 */
CASSANDRA_INTEGRATION_TYPED_TEST_P(CassandraTypesTests, Tuple) {
  CHECK_VERSION(2.1.0);
  CHECK_VALUE_TYPE_VERSION(TypeParam);

  // Initialize the table and assign the values for the tuple
  const std::vector<TypeParam>& values = CassandraTypesTests<TypeParam>::values_;
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

    // Bind both the primary key and the value with the Cassandra type tuple and insert
    if (this->is_key_allowed_) {
      statement.bind<Tuple>(0, tuple);
    } else {
      statement.bind<Integer>(0, Integer(i));
    }
    statement.bind<Tuple>(1, tuple);
    this->session_.execute(statement);

    // Validate the result
    Statement select_statement(this->select_query_, 1);
    if (this->is_key_allowed_) {
      select_statement.bind<Tuple>(0, tuple);
    } else {
      select_statement.bind<Integer>(0, Integer(i));
    }
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
 * @test_category queries:basic
 * @test_category prepared_statements
 * @test_category data_types:udt
 * @since core 1.0.0
 * @cassandra_version 2.2.0
 * @expected_result Cassandra values are inserted using a user data type and
 *                  then validated via simple and prepared statement operations
 */
CASSANDRA_INTEGRATION_TYPED_TEST_P(CassandraTypesTests, UDT) {
  CHECK_VERSION(2.2.0);
  CHECK_VALUE_TYPE_VERSION(TypeParam);

  // Build the UDT type name e.g. udt_pointtype, udt_line_string, etc.
  const std::vector<TypeParam>& values = CassandraTypesTests<TypeParam>::values_;
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

  // Initialize the table; NOTE: UDT must be frozen for older versions of Cassandra
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

    // Bind both the primary key and the value with the Cassandra type UDT and insert
    if (this->is_key_allowed_) {
      statement.bind<UserType>(0, user_type);
    } else {
      statement.bind<Integer>(0, Integer(i));
    }
    statement.bind<UserType>(1, user_type);
    this->session_.execute(statement);

    // Validate the result
    Statement select_statement(this->select_query_, 1);
    if (this->is_key_allowed_) {
      select_statement.bind<UserType>(0, user_type);
    } else {
      select_statement.bind<Integer>(0, Integer(i));
    }
    Result result = this->session_.execute(select_statement);
    ASSERT_EQ(1u, result.row_count());
    UserType result_udt_values(result.first_row().next().as<UserType>());
    ASSERT_EQ(udt_values, result_udt_values.values<TypeParam>());
  }
}

// Register all parameterized test cases for primitives (excludes duration)
REGISTER_TYPED_TEST_CASE_P(CassandraTypesTests, Integration_Cassandra_Basic,
                           Integration_Cassandra_ByName, Integration_Cassandra_NamedParameters,
                           Integration_Cassandra_NullValues, Integration_Cassandra_NullList,
                           Integration_Cassandra_NullMap, Integration_Cassandra_NullSet,
                           Integration_Cassandra_List, Integration_Cassandra_Set,
                           Integration_Cassandra_Map, Integration_Cassandra_Tuple,
                           Integration_Cassandra_UDT);

/**
 * Attempt to utilize an invalid duration value on a statement
 *
 * This test will perform a query using mixed positive and negative values for a
 * duration type. The statement will be executed and a
 * `CASS_ERROR_SERVER_INVALID_QUERY` should be returned by the future.
 *
 * @jira_ticket CPP-429
 * @test_category data_types:duration
 * @since Core 2.6.0
 * @expected_result Statement request will execute and a server error will
 *                  occur.
 */
CASSANDRA_INTEGRATION_TEST_F(CassandraTypesDurationTests, MixedValues) {
  CHECK_FAILURE;
  CHECK_VALUE_TYPE_VERSION(Duration);

  this->default_setup();

  // Create a simple statement and bind mixed values for duration
  Statement statement(this->insert_query_, 2);
  Duration duration(CassDuration(0, -1, 1));
  statement.bind<Integer>(0, Integer(1));
  statement.bind<Duration>(1, duration);

  // Execute the statement and validate the server error
  Result result = this->session_.execute(statement, false);
  ASSERT_EQ(CASS_ERROR_SERVER_INVALID_QUERY, result.error_code());
  ASSERT_STREQ("The duration months, days and nanoseconds must be all of the same sign (0, -1, 1)",
               result.error_message().c_str());
}

// Instantiate the test case for all the Cassandra data types
typedef testing::Types<Ascii, BigInteger, Blob, Boolean, Date, Decimal, Double, Duration, Float,
                       Inet, Integer, SmallInteger, Text, Time, Timestamp, TimeUuid, TinyInteger,
                       Uuid, Varchar, Varint>
    CassandraTypes;
INSTANTIATE_TYPED_TEST_CASE_P(CassandraTypes, CassandraTypesTests, CassandraTypes);

/**
 * Values for ASCII tests
 */
const Ascii ASCII_VALUES[] = { Ascii("DataStax"), Ascii("C/C++"), Ascii("Driver"),
                               Ascii("Cassandra") };
template <>
const std::vector<Ascii>
    CassandraTypesTests<Ascii>::values_(ASCII_VALUES, ASCII_VALUES + ARRAY_LEN(ASCII_VALUES));

/**
 * Values for bigint tests
 */
const BigInteger BIGINT_VALUES[] = { BigInteger::max(), BigInteger::min(),
                                     BigInteger(static_cast<cass_int64_t>(0)), BigInteger(37) };
template <>
const std::vector<BigInteger>
    CassandraTypesTests<BigInteger>::values_(BIGINT_VALUES,
                                             BIGINT_VALUES + ARRAY_LEN(BIGINT_VALUES));

/**
 * Values for blob tests
 */
const Blob BLOB_VALUES[] = { Blob("DataStax C/C++ Driver"), Blob("Cassandra"),
                             Blob("DataStax Enterprise") };
template <>
const std::vector<Blob> CassandraTypesTests<Blob>::values_(BLOB_VALUES,
                                                           BLOB_VALUES + ARRAY_LEN(BLOB_VALUES));

/**
 * Values for boolean tests
 */
const Boolean BOOLEAN_VALUES[] = { Boolean(true), Boolean(false) };
template <>
const std::vector<Boolean> CassandraTypesTests<Boolean>::values_(BOOLEAN_VALUES,
                                                                 BOOLEAN_VALUES +
                                                                     ARRAY_LEN(BOOLEAN_VALUES));

/**
 * Values for date tests
 */
const Date DATE_VALUES[] = { Date::max(), // maximum for strftime
                             Date::min(), // minimum for strftime
                             Date(static_cast<cass_uint32_t>(0)), Date(12345u) };
template <>
const std::vector<Date> CassandraTypesTests<Date>::values_(DATE_VALUES,
                                                           DATE_VALUES + ARRAY_LEN(DATE_VALUES));

/**
 * Values for decimal tests
 */
const Decimal DECIMAL_VALUES[] = { Decimal("3."
                                           "1415926535897932384626433832795028841971693993751058209"
                                           "749445923078164062862089986280348253421170679"),
                                   Decimal("2."
                                           "7182818284590452353602874713526624977572470936999595749"
                                           "669676277240766303535475945713821785251664274"),
                                   Decimal("1."
                                           "6180339887498948482045868343656381177203091798057628621"
                                           "354486227052604628189024497072072041893911374") };
template <>
const std::vector<Decimal> CassandraTypesTests<Decimal>::values_(DECIMAL_VALUES,
                                                                 DECIMAL_VALUES +
                                                                     ARRAY_LEN(DECIMAL_VALUES));

/**
 * Values for double tests
 */
const Double DOUBLE_VALUES[] = { Double::max(), Double::min(), Double(3.1415926535),
                                 Double(2.7182818284), Double(1.6180339887) };
template <>
const std::vector<Double>
    CassandraTypesTests<Double>::values_(DOUBLE_VALUES, DOUBLE_VALUES + ARRAY_LEN(DOUBLE_VALUES));

/**
 * Values for duration tests
 */
const Duration DURATION_VALUES[] = {
  Duration(CassDuration(1, 2, 3)),
  Duration(CassDuration(1, 0, std::numeric_limits<cass_int64_t>::max())),
  Duration(CassDuration(-1, 0, std::numeric_limits<cass_int64_t>::min())),
  Duration(CassDuration(std::numeric_limits<cass_int32_t>::max(), 1, 0)),
  Duration(CassDuration(std::numeric_limits<cass_int32_t>::min(), -1, 0)),
  Duration(CassDuration(0, std::numeric_limits<cass_int32_t>::max(), 1)),
  Duration(CassDuration(0, std::numeric_limits<cass_int32_t>::min(), -1))
};
template <>
const std::vector<Duration> CassandraTypesTests<Duration>::values_(DURATION_VALUES,
                                                                   DURATION_VALUES +
                                                                       ARRAY_LEN(DURATION_VALUES));

/**
 * Values for float tests
 */
const Float FLOAT_VALUES[] = { Float::max(), Float::min(), Float(3.14159f), Float(2.71828f),
                               Float(1.61803f) };
template <>
const std::vector<Float>
    CassandraTypesTests<Float>::values_(FLOAT_VALUES, FLOAT_VALUES + ARRAY_LEN(FLOAT_VALUES));

/**
 * Values for inet tests
 */
const Inet INET_VALUES[] = { Inet::max(), Inet::min(), Inet("127.0.0.1"), Inet("0:0:0:0:0:0:0:1"),
                             Inet("2001:db8:85a3:0:0:8a2e:370:7334") };
template <>
const std::vector<Inet> CassandraTypesTests<Inet>::values_(INET_VALUES,
                                                           INET_VALUES + ARRAY_LEN(INET_VALUES));

/**
 * Values for int tests
 */
const Integer INT_VALUES[] = { Integer::max(), Integer::min(), Integer(0), Integer(148) };
template <>
const std::vector<Integer>
    CassandraTypesTests<Integer>::values_(INT_VALUES, INT_VALUES + ARRAY_LEN(INT_VALUES));

/**
 * Values for smallint tests
 */
const SmallInteger SMALLINT_VALUES[] = { SmallInteger::max(), SmallInteger::min(),
                                         SmallInteger(static_cast<int16_t>(0)), SmallInteger(148) };
template <>
const std::vector<SmallInteger>
    CassandraTypesTests<SmallInteger>::values_(SMALLINT_VALUES,
                                               SMALLINT_VALUES + ARRAY_LEN(SMALLINT_VALUES));

/**
 * Values for text tests
 */
const Text TEXT_VALUES[] = { Text("The quick brown fox jumps over the lazy dog"),
                             Text("Hello World"), Text("DataStax C/C++ Driver") };
template <>
const std::vector<Text> CassandraTypesTests<Text>::values_(TEXT_VALUES,
                                                           TEXT_VALUES + ARRAY_LEN(TEXT_VALUES));

/**
 * Values for time tests
 */
const Time TIME_VALUES[] = { Time::max(), Time::min(), Time(static_cast<cass_int64_t>(0)),
                             Time(9876543210) };
template <>
const std::vector<Time> CassandraTypesTests<Time>::values_(TIME_VALUES,
                                                           TIME_VALUES + ARRAY_LEN(TIME_VALUES));

/**
 * Values for timestamp tests
 */
const Timestamp TIMESTAMP_VALUES[] = { Timestamp::max(), Timestamp::min(), Timestamp(123),
                                       Timestamp(456), Timestamp(789) };
template <>
const std::vector<Timestamp>
    CassandraTypesTests<Timestamp>::values_(TIMESTAMP_VALUES,
                                            TIMESTAMP_VALUES + ARRAY_LEN(TIMESTAMP_VALUES));

/**
 * Values for timeuuid tests
 */
const TimeUuid TIMEUUID_VALUES[] = { TimeUuid::min(),
                                     TimeUuid::max(),
                                     TimeUuid(values::TimeUuid::min(0)),
                                     TimeUuid(values::TimeUuid::max(0)),
                                     TimeUuid(values::TimeUuid::min(CASS_UINT64_MAX)),
                                     TimeUuid(values::TimeUuid::max(CASS_UINT64_MAX)),
                                     TimeUuid(values::TimeUuid::min(uv_hrtime())),
                                     TimeUuid(values::TimeUuid::max(uv_hrtime())) };
template <>
const std::vector<TimeUuid> CassandraTypesTests<TimeUuid>::values_(TIMEUUID_VALUES,
                                                                   TIMEUUID_VALUES +
                                                                       ARRAY_LEN(TIMEUUID_VALUES));

/**
 * Values for tinyint tests
 */
const TinyInteger TINYINT_VALUES[] = { TinyInteger::max(), TinyInteger::min(),
                                       TinyInteger(static_cast<int8_t>(0)), TinyInteger(37) };
template <>
const std::vector<TinyInteger>
    CassandraTypesTests<TinyInteger>::values_(TINYINT_VALUES,
                                              TINYINT_VALUES + ARRAY_LEN(TINYINT_VALUES));

/**
 * Values for uuid tests
 */
const Uuid UUID_VALUES[] = { Uuid::max(), Uuid::min(), Uuid("03398c99-c635-4fad-b30a-3b2c49f785c2"),
                             Uuid("03398c99-c635-4fad-b30a-3b2c49f785c3"),
                             Uuid("03398c99-c635-4fad-b30a-3b2c49f785c4") };
template <>
const std::vector<Uuid> CassandraTypesTests<Uuid>::values_(UUID_VALUES,
                                                           UUID_VALUES + ARRAY_LEN(UUID_VALUES));

/**
 * Values for varchar tests
 */
const Varchar VARCHAR_VALUES[] = { Varchar("The quick brown fox jumps over the lazy dog"),
                                   Varchar("Hello World"), Varchar("DataStax C/C++ Driver") };
template <>
const std::vector<Varchar> CassandraTypesTests<Varchar>::values_(VARCHAR_VALUES,
                                                                 VARCHAR_VALUES +
                                                                     ARRAY_LEN(VARCHAR_VALUES));

/**
 * Values for varint tests
 */
const Varint VARINT_VALUES[] = { Varint("123456789012345678901234567890"),
                                 Varint("98765432109876543210987654321098765432109876543210"),
                                 Varint("0"), Varint("-296") };
template <>
const std::vector<Varint>
    CassandraTypesTests<Varint>::values_(VARINT_VALUES, VARINT_VALUES + ARRAY_LEN(VARINT_VALUES));
