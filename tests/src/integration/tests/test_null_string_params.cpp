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

#include "cassandra.h"
#include "integration.hpp"
#include "result_response.hpp"

/****
 * TODO: As integration tests are migrated from Boost to GTest,
 * some of these test cases should probably move to test files
 * related to their types/functionality.
 */

// Name of materialized view used in this test file.
#define VIEW_NAME "my_view"

using namespace datastax::internal::core;

/**
 * Null string api args test, without initially creating a connection.
 */
class DisconnectedNullStringApiArgsTest : public Integration {
public:
  DisconnectedNullStringApiArgsTest() { is_session_requested_ = false; }

  void SetUp() {
    Integration::SetUp();
    cluster_ = default_cluster();
  }
};

/**
 * Null string api args test, connected to the cluster at the beginning
 * of each test.
 */
class NullStringApiArgsTest : public Integration {
public:
  NullStringApiArgsTest() { is_schema_metadata_ = true; }
};

/**
 * Null string api args test, connected to the cluster at the beginning
 * of each test, and with a representative schema set up.
 */
class SchemaNullStringApiArgsTest : public NullStringApiArgsTest {
public:
  void SetUp() {
    CHECK_VERSION(2.2.0);
    NullStringApiArgsTest::SetUp();
    populateSchema();
    schema_meta_ = session_.schema();
    keyspace_meta_ = schema_meta_.keyspace(keyspace_name_);
    table_meta_ = keyspace_meta_.table(table_name_);
  }

  void populateSchema() {
    session_.execute(format_string("CREATE TABLE %s (key text, value bigint, "
                                   "PRIMARY KEY (key))",
                                   table_name_.c_str()));

    session_.execute("CREATE FUNCTION avg_state(state tuple<int, bigint>, val int) "
                     "CALLED ON NULL INPUT RETURNS tuple<int, bigint> "
                     "LANGUAGE java AS "
                     "  'if (val != null) { "
                     "    state.setInt(0, state.getInt(0) + 1); "
                     "    state.setLong(1, state.getLong(1) + val.intValue()); "
                     "  } ;"
                     "  return state;'"
                     ";");
    session_.execute("CREATE FUNCTION avg_final (state tuple<int, bigint>) "
                     "CALLED ON NULL INPUT RETURNS double "
                     "LANGUAGE java AS "
                     "  'double r = 0; "
                     "  if (state.getInt(0) == 0) return null; "
                     "  r = state.getLong(1); "
                     "  r /= state.getInt(0); "
                     "  return Double.valueOf(r);' "
                     ";");

    session_.execute("CREATE AGGREGATE average(int) "
                     "SFUNC avg_state STYPE tuple<int, bigint> FINALFUNC avg_final "
                     "INITCOND(0, 0);");

    if (server_version_ >= "3.0.0") {
      session_.execute(format_string("CREATE MATERIALIZED VIEW %s "
                                     "AS SELECT value, key "
                                     "   FROM %s"
                                     "   WHERE value IS NOT NULL and key IS NOT NULL "
                                     "PRIMARY KEY(value, key)",
                                     VIEW_NAME, table_name_.c_str()));
    }
    session_.execute("CREATE TYPE address (street text, city text)");

    session_.execute(
        format_string("CREATE INDEX schema_meta_index ON %s (value)", table_name_.c_str()));
  }

protected:
  Schema schema_meta_;
  Keyspace keyspace_meta_;
  Table table_meta_;
};

/**
 * Set the contact points to null
 *
 * @jira_ticket CPP-368
 * @test_category configuration:error_codes
 * @expected_result Connection is unsuccessful with NULL contact points.
 */
CASSANDRA_INTEGRATION_TEST_F(DisconnectedNullStringApiArgsTest, SetContactPoints) {
  EXPECT_EQ(CASS_OK, cass_cluster_set_contact_points(cluster_.get(), NULL));
  ASSERT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, cluster_.connect("", false).connect_error_code());
}

/**
 * Set host-list in white-list policy to null
 *
 * @jira_ticket CPP-368
 * @test_category configuration
 * @expected_result Successfully connect; the null white-list is essentially ignored.
 */
CASSANDRA_INTEGRATION_TEST_F(DisconnectedNullStringApiArgsTest, SetWhitelistFilteringNullHosts) {
  cass_cluster_set_whitelist_filtering(cluster_.get(), NULL);
  ASSERT_EQ(CASS_OK, cluster_.connect("", false).connect_error_code());
}

/**
 * Set host-list in black-list policy to null
 *
 * @jira_ticket CPP-368
 * @test_category configuration
 * @expected_result Successfully connect; the null black-list is essentially ignored.
 */
CASSANDRA_INTEGRATION_TEST_F(DisconnectedNullStringApiArgsTest, SetBlacklistFilteringNullHosts) {
  cass_cluster_set_blacklist_filtering(cluster_.get(), NULL);
  ASSERT_EQ(CASS_OK, cluster_.connect("", false).connect_error_code());
}

/**
 * Set dc-list in white-list dc-filtering policy to null
 *
 * @jira_ticket CPP-368
 * @test_category configuration
 * @expected_result Successfully connect; the null dc white-list is essentially ignored.
 */
CASSANDRA_INTEGRATION_TEST_F(DisconnectedNullStringApiArgsTest, SetWhitelistDcFilteringNullDcs) {
  cass_cluster_set_whitelist_dc_filtering(cluster_.get(), NULL);
  ASSERT_EQ(CASS_OK, cluster_.connect("", false).connect_error_code());
}

/**
 * Set dc-list in black-list dc-filtering policy to null
 *
 * @jira_ticket CPP-368
 * @test_category configuration
 * @expected_result Successfully connect; the null dc black-list is essentially ignored.
 */
CASSANDRA_INTEGRATION_TEST_F(DisconnectedNullStringApiArgsTest, SetBlacklistDcFilteringNullDcs) {
  cass_cluster_set_blacklist_dc_filtering(cluster_.get(), NULL);
  ASSERT_EQ(CASS_OK, cluster_.connect("", false).connect_error_code());
}

/**
 * Set keyspace in session-connect to null
 *
 * @jira_ticket CPP-368
 * @test_category configuration
 * @expected_result Successfully connect with no keyspace binding.
 */
CASSANDRA_INTEGRATION_TEST_F(DisconnectedNullStringApiArgsTest, ConnectKeyspaceNullKeyspace) {
  Session test_session = cass_session_new();
  Future future = cass_session_connect_keyspace(test_session.get(), cluster_.get(), NULL);
  ASSERT_EQ(CASS_OK, future.error_code());
}

/**
 * Set keyspace in cass_schema_meta_keyspace_by_name call to null
 *
 * @jira_ticket CPP-368
 * @test_category schema
 * @expected_result Null, meaning that no keyspace was found with the given name (null).
 */
CASSANDRA_INTEGRATION_TEST_F(NullStringApiArgsTest, SchemaMetaKeyspaceByNameNullKeyspace) {
  Schema schema_meta = session_.schema();
  const CassKeyspaceMeta* keyspace_meta =
      cass_schema_meta_keyspace_by_name(schema_meta.get(), NULL);
  EXPECT_EQ(NULL, keyspace_meta);
}

/**
 * Set query in prepare request to null
 *
 * @jira_ticket CPP-368
 * @test_category prepared_statements:error_codes
 * @expected_result Syntax error because there is no query to prepare.
 */
CASSANDRA_INTEGRATION_TEST_F(NullStringApiArgsTest, PrepareNullQuery) {
  Future future = cass_session_prepare(session_.get(), NULL);
  ASSERT_EQ(CASS_ERROR_SERVER_SYNTAX_ERROR, future.error_code());
}

/**
 * Set string arguments to NULL for cass_keyspace_meta_* functions.
 *
 * @jira_ticket CPP-368
 * @test_category metadata
 * @expected_result Null for each lookup, since no object has a null name.
 */
CASSANDRA_INTEGRATION_TEST_F(SchemaNullStringApiArgsTest, KeyspaceMetaFunctions) {
  CHECK_VERSION(2.2.0);
  const CassTableMeta* table_meta = cass_keyspace_meta_table_by_name(keyspace_meta_.get(), NULL);
  EXPECT_EQ(NULL, table_meta);

  if (schema_meta_.version().major_version >= 3) {
    const CassMaterializedViewMeta* view_meta =
        cass_keyspace_meta_materialized_view_by_name(keyspace_meta_.get(), NULL);
    EXPECT_EQ(NULL, view_meta);
  }

  const CassDataType* type_meta = cass_keyspace_meta_user_type_by_name(keyspace_meta_.get(), NULL);
  EXPECT_EQ(NULL, type_meta);

  const CassValue* field_meta = cass_keyspace_meta_field_by_name(keyspace_meta_.get(), NULL);
  EXPECT_EQ(NULL, field_meta);

  const CassFunctionMeta* function_meta =
      cass_keyspace_meta_function_by_name(keyspace_meta_.get(), NULL, "abc");
  EXPECT_EQ(NULL, function_meta);

  function_meta = cass_keyspace_meta_function_by_name(keyspace_meta_.get(), "avg_final", NULL);
  EXPECT_EQ(NULL, function_meta);

  const CassAggregateMeta* aggregate_meta =
      cass_keyspace_meta_aggregate_by_name(keyspace_meta_.get(), NULL, "abc");
  EXPECT_EQ(NULL, aggregate_meta);

  aggregate_meta = cass_keyspace_meta_aggregate_by_name(keyspace_meta_.get(), "average", NULL);
  EXPECT_EQ(NULL, aggregate_meta);
}

/**
 * Set string arguments to NULL for cass_table_meta_*, cass_column_meta_*,
 * cass_index_meta_* functions.
 *
 * @jira_ticket CPP-368
 * @test_category metadata
 * @expected_result Null for each lookup, since no object has a null name.
 */
CASSANDRA_INTEGRATION_TEST_F(SchemaNullStringApiArgsTest, TableMetaFunctions) {
  CHECK_VERSION(2.2.0);
  const CassColumnMeta* column_meta = cass_table_meta_column_by_name(table_meta_.get(), NULL);
  EXPECT_EQ(NULL, column_meta);

  const CassIndexMeta* index_meta = cass_table_meta_index_by_name(table_meta_.get(), NULL);
  EXPECT_EQ(NULL, index_meta);

  if (schema_meta_.version().major_version >= 3) {
    const CassMaterializedViewMeta* view_meta =
        cass_table_meta_materialized_view_by_name(table_meta_.get(), NULL);
    EXPECT_EQ(NULL, view_meta);
  }

  const CassValue* field_meta = cass_table_meta_field_by_name(table_meta_.get(), NULL);
  EXPECT_EQ(NULL, field_meta);

  column_meta = cass_table_meta_column_by_name(table_meta_.get(), "value");
  EXPECT_NE(static_cast<CassColumnMeta*>(NULL), column_meta);
  field_meta = cass_column_meta_field_by_name(column_meta, NULL);
  EXPECT_EQ(NULL, field_meta);

  index_meta = cass_table_meta_index_by_name(table_meta_.get(), "schema_meta_index");
  EXPECT_NE(static_cast<CassIndexMeta*>(NULL), index_meta);
  field_meta = cass_index_meta_field_by_name(index_meta, NULL);
  EXPECT_EQ(NULL, field_meta);
}

/**
 * Set string arguments to NULL for cass_materialized_view_meta_* functions.
 *
 * @jira_ticket CPP-368
 * @test_category metadata
 * @expected_result Null for each lookup, since no object has a null name.
 */
CASSANDRA_INTEGRATION_TEST_F(SchemaNullStringApiArgsTest, MaterializedViewMetaFunctions) {
  CHECK_VERSION(3.0.0);

  const CassMaterializedViewMeta* view_meta =
      cass_table_meta_materialized_view_by_name(table_meta_.get(), VIEW_NAME);
  ASSERT_NE(static_cast<CassMaterializedViewMeta*>(NULL), view_meta);

  const CassColumnMeta* column_meta = cass_materialized_view_meta_column_by_name(view_meta, NULL);
  EXPECT_EQ(NULL, column_meta);

  const CassValue* field_meta = cass_materialized_view_meta_field_by_name(view_meta, NULL);
  EXPECT_EQ(NULL, field_meta);
}

/**
 * Set string arguments to NULL for cass_function_meta_* and
 * cass_aggregate_meta_* functions.
 *
 * @jira_ticket CPP-368
 * @test_category metadata
 * @expected_result Null for each lookup, since no object has a null name.
 */
CASSANDRA_INTEGRATION_TEST_F(SchemaNullStringApiArgsTest, FunctionAndAggregateMetaFunctions) {
  CHECK_VERSION(2.2.0);
  // C* 3.x annotate collection columns as frozen.
  const CassFunctionMeta* function_meta =
      (schema_meta_.version().major_version >= 3)
          ? cass_keyspace_meta_function_by_name(keyspace_meta_.get(), "avg_final",
                                                "frozen<tuple<int,bigint>>")
          : cass_keyspace_meta_function_by_name(keyspace_meta_.get(), "avg_final",
                                                "tuple<int,bigint>");
  ASSERT_NE(static_cast<CassFunctionMeta*>(NULL), function_meta);

  const CassDataType* data_type = cass_function_meta_argument_type_by_name(function_meta, NULL);
  EXPECT_EQ(NULL, data_type);

  const CassValue* field_meta = cass_function_meta_field_by_name(function_meta, NULL);
  EXPECT_EQ(NULL, field_meta);

  const CassAggregateMeta* aggregate_meta =
      cass_keyspace_meta_aggregate_by_name(keyspace_meta_.get(), "average", "int");
  ASSERT_NE(static_cast<CassAggregateMeta*>(NULL), aggregate_meta);
  field_meta = cass_aggregate_meta_field_by_name(aggregate_meta, NULL);
  EXPECT_EQ(NULL, field_meta);
}

/**
 * Set string arguments to NULL for cass_statement_* functions
 *
 * @jira_ticket CPP-368
 * @test_category queries
 * @expected_result Error out appropriately for invalid queries, succeed otherwise.
 */
CASSANDRA_INTEGRATION_TEST_F(SchemaNullStringApiArgsTest, StatementFunctions) {
  CHECK_VERSION(2.2.0);
  Statement statement(NULL);

  statement = cass_statement_new(NULL, 0);
  EXPECT_EQ(CASS_ERROR_SERVER_SYNTAX_ERROR, session_.execute(statement, false).error_code());

  statement = cass_statement_new(format_string("SELECT * FROM %s", table_name_.c_str()).c_str(), 0);
  EXPECT_EQ(CASS_OK, cass_statement_set_keyspace(statement.get(), NULL));
  EXPECT_EQ(CASS_OK, session_.execute(statement, false).error_code());

  // Test that when the session does not have the keyspace set, running
  // the statement fails.
  Session session_without_keyspace = cluster_.connect();
  EXPECT_EQ(CASS_ERROR_SERVER_INVALID_QUERY,
            session_without_keyspace.execute(statement, false).error_code());

  std::string query_string = format_string("INSERT INTO %s (key, value) "
                                           "VALUES (42, :v)",
                                           table_name_.c_str());
  const char* query = query_string.c_str();

  statement = cass_statement_new(query, 1);
  EXPECT_EQ(CASS_OK, cass_statement_bind_null_by_name(statement.get(), NULL));
  EXPECT_EQ(CASS_ERROR_SERVER_INVALID_QUERY, session_.execute(statement, false).error_code());

#define BIND_BY_NAME_TEST(FUNC, NAME, VALUE)              \
  statement = cass_statement_new(query, 1);               \
  EXPECT_EQ(CASS_OK, FUNC(statement.get(), NAME, VALUE)); \
  EXPECT_EQ(CASS_ERROR_SERVER_INVALID_QUERY, session_.execute(statement, false).error_code());

#define BIND_BY_NAME_TEST_WITH_VALUE_LEN(FUNC, NAME, VALUE, VALUE_LEN)                         \
  statement = cass_statement_new(query, 1);                                                    \
  EXPECT_EQ(CASS_OK, FUNC(statement.get(), NAME, VALUE, VALUE_LEN));                           \
  EXPECT_EQ(CASS_ERROR_SERVER_INVALID_QUERY, session_.execute(statement, false).error_code()); \
  statement = cass_statement_new(query, 1);                                                    \
  EXPECT_EQ(CASS_OK, FUNC(statement.get(), "v", NULL, 0));                                     \
  EXPECT_EQ(CASS_ERROR_SERVER_INVALID_QUERY, session_.execute(statement, false).error_code());

  BIND_BY_NAME_TEST(cass_statement_bind_int8_by_name, NULL, 42);
  BIND_BY_NAME_TEST(cass_statement_bind_int16_by_name, NULL, 42);
  BIND_BY_NAME_TEST(cass_statement_bind_int32_by_name, NULL, 42);
  BIND_BY_NAME_TEST(cass_statement_bind_uint32_by_name, NULL, 42);
  BIND_BY_NAME_TEST(cass_statement_bind_int64_by_name, NULL, 42);
  BIND_BY_NAME_TEST(cass_statement_bind_float_by_name, NULL, 42.2f);
  BIND_BY_NAME_TEST(cass_statement_bind_double_by_name, NULL, 42.0);
  BIND_BY_NAME_TEST(cass_statement_bind_bool_by_name, NULL, cass_true);

  statement = cass_statement_new(query, 1);
  EXPECT_EQ(CASS_OK, cass_statement_bind_string(statement.get(), 0, NULL));
  EXPECT_EQ(CASS_ERROR_SERVER_INVALID_QUERY, session_.execute(statement, false).error_code());

  BIND_BY_NAME_TEST(cass_statement_bind_string_by_name, NULL, "val");
  BIND_BY_NAME_TEST(cass_statement_bind_string_by_name, "v", NULL);

  BIND_BY_NAME_TEST_WITH_VALUE_LEN(cass_statement_bind_bytes_by_name, NULL,
                                   reinterpret_cast<const cass_byte_t*>("a"), 1);

  statement = cass_statement_new(query, 1);
  EXPECT_EQ(CASS_OK, cass_statement_bind_custom(statement.get(), 0, "myclass", NULL, 0));
  EXPECT_EQ(CASS_ERROR_SERVER_INVALID_QUERY, session_.execute(statement, false).error_code());

  statement = cass_statement_new(query, 1);
  EXPECT_EQ(CASS_OK, cass_statement_bind_custom(statement.get(), 0, NULL,
                                                reinterpret_cast<const cass_byte_t*>("a"), 1));
  EXPECT_EQ(CASS_ERROR_SERVER_INVALID_QUERY, session_.execute(statement, false).error_code());

  statement = cass_statement_new(query, 1);
  EXPECT_EQ(CASS_OK,
            cass_statement_bind_custom_by_name(statement.get(), NULL, "myclass",
                                               reinterpret_cast<const cass_byte_t*>("a"), 1));
  EXPECT_EQ(CASS_ERROR_SERVER_INVALID_QUERY, session_.execute(statement, false).error_code());

  statement = cass_statement_new(query, 1);
  EXPECT_EQ(CASS_OK, cass_statement_bind_custom_by_name(statement.get(), "v", "myclass", NULL, 0));
  EXPECT_EQ(CASS_ERROR_SERVER_INVALID_QUERY, session_.execute(statement, false).error_code());

  statement = cass_statement_new(query, 1);
  EXPECT_EQ(CASS_OK, cass_statement_bind_custom_by_name(
                         statement.get(), "v", NULL, reinterpret_cast<const cass_byte_t*>("a"), 1));
  EXPECT_EQ(CASS_ERROR_SERVER_INVALID_QUERY, session_.execute(statement, false).error_code());

  UuidGen uuid_generator(11);
  BIND_BY_NAME_TEST(cass_statement_bind_uuid_by_name, NULL,
                    uuid_generator.generate_random_uuid().value());

  CassInet inet;
  EXPECT_EQ(CASS_OK, cass_inet_from_string("127.1.2.3", &inet));
  BIND_BY_NAME_TEST(cass_statement_bind_inet_by_name, NULL, inet);

  const cass_uint8_t pi[] = { 57,  115, 235, 135, 229, 215, 8,   125, 13,  43,  1,   25, 32,  135,
                              129, 180, 112, 176, 158, 120, 246, 235, 29,  145, 238, 50, 108, 239,
                              219, 100, 250, 84,  6,   186, 148, 76,  230, 46,  181, 89, 239, 247 };
  statement = cass_statement_new(query, 1);
  EXPECT_EQ(CASS_OK,
            cass_statement_bind_decimal_by_name(statement.get(), NULL, pi, sizeof(pi), 100));
  EXPECT_EQ(CASS_ERROR_SERVER_INVALID_QUERY, session_.execute(statement, false).error_code());

  statement = cass_statement_new(query, 1);
  EXPECT_EQ(CASS_OK, cass_statement_bind_duration_by_name(statement.get(), NULL, 1, 2, 3));
  EXPECT_EQ(CASS_ERROR_SERVER_INVALID_QUERY, session_.execute(statement, false).error_code());

  CassCollection* collection = cass_collection_new(CASS_COLLECTION_TYPE_SET, 2);
  EXPECT_EQ(CASS_OK, cass_collection_append_string(collection, "a"));
  EXPECT_EQ(CASS_OK, cass_collection_append_string(collection, "b"));
  BIND_BY_NAME_TEST(cass_statement_bind_collection_by_name, NULL, collection);
  cass_collection_free(collection);

  CassTuple* tuple = cass_tuple_new(2);
  EXPECT_EQ(CASS_OK, cass_tuple_set_string(tuple, 0, "a"));
  EXPECT_EQ(CASS_OK, cass_tuple_set_string(tuple, 1, "b"));
  BIND_BY_NAME_TEST(cass_statement_bind_tuple_by_name, NULL, tuple);
  cass_tuple_free(tuple);

  const CassDataType* udt_address =
      cass_keyspace_meta_user_type_by_name(keyspace_meta_.get(), "address");
  ASSERT_NE(static_cast<CassDataType*>(NULL), udt_address);

  CassUserType* address = cass_user_type_new_from_data_type(udt_address);
  EXPECT_EQ(CASS_OK, cass_user_type_set_string_by_name(address, "street", "123 My Street"));
  EXPECT_EQ(CASS_OK, cass_user_type_set_string_by_name(address, "city", "Somewhere"));
  BIND_BY_NAME_TEST(cass_statement_bind_user_type_by_name, NULL, address);
  cass_user_type_free(address);

#undef BIND_BY_NAME_TEST

#undef BIND_BY_NAME_TEST_WITH_VALUE_LEN
}

/**
 * Set string arguments to NULL for cass_prepared_* functions
 *
 * @jira_ticket CPP-368
 * @test_category prepared_statements
 * @expected_result Null because no parameter in the statement has a null name.
 */
CASSANDRA_INTEGRATION_TEST_F(SchemaNullStringApiArgsTest, PreparedFunctions) {
  CHECK_VERSION(2.2.0);
  Prepared prepared = session_.prepare(format_string("INSERT INTO %s (key, value) "
                                                     "VALUES ('42', :v)",
                                                     table_name_.c_str()));
  const CassDataType* data_type = cass_prepared_parameter_data_type_by_name(prepared.get(), NULL);
  EXPECT_EQ(NULL, data_type);
}

/**
 * Set string arguments to NULL for cass_data_type_* functions.
 *
 * @jira_ticket CPP-368
 * @test_category data_types:udt
 * @expected_result For functions that set string attributes on data-types,
 *   they succeed because NULL acts as a no-op. For functions that retrieve
 *   attributes by name, they should return null due to failed lookup (of
 *   null).
 */
CASSANDRA_INTEGRATION_TEST_F(SchemaNullStringApiArgsTest, DataTypeFunctions) {
  CHECK_VERSION(2.2.0);
  CassDataType* udt = cass_data_type_new(CASS_VALUE_TYPE_UDT);
  EXPECT_EQ(CASS_OK, cass_data_type_set_type_name(udt, NULL));
  EXPECT_EQ(NULL, cass_data_type_sub_data_type_by_name(udt, NULL));

  CassDataType* custom_type = cass_data_type_new(CASS_VALUE_TYPE_CUSTOM);
  EXPECT_EQ(CASS_OK, cass_data_type_set_class_name(custom_type, NULL));

  EXPECT_EQ(CASS_OK, cass_data_type_add_sub_type_by_name(udt, NULL, custom_type));
  EXPECT_EQ(CASS_OK, cass_data_type_add_sub_value_type_by_name(udt, NULL, CASS_VALUE_TYPE_BOOLEAN));

  cass_data_type_free(udt);
  cass_data_type_free(custom_type);
}

/**
 * Set string arguments to NULL for cass_collection_* and cass_tuple_* functions.
 *
 * @jira_ticket CPP-368
 * @test_category data_types:collections
 * @expected_result Success; null strings are added/encoded in collections fine.
 */
CASSANDRA_INTEGRATION_TEST_F(SchemaNullStringApiArgsTest, CollectionFunctions) {
  CHECK_VERSION(2.2.0);
  CassCollection* collection = cass_collection_new(CASS_COLLECTION_TYPE_SET, 2);
  EXPECT_EQ(CASS_OK, cass_collection_append_string(collection, NULL));
  EXPECT_EQ(CASS_OK, cass_collection_append_custom(collection, NULL,
                                                   reinterpret_cast<const cass_byte_t*>("a"), 1));
  cass_collection_free(collection);

  CassTuple* tuple = cass_tuple_new(2);
  EXPECT_EQ(CASS_OK, cass_tuple_set_string(tuple, 0, NULL));
  EXPECT_EQ(CASS_OK,
            cass_tuple_set_custom(tuple, 0, NULL, reinterpret_cast<const cass_byte_t*>("a"), 1));
  cass_tuple_free(tuple);
}

/**
 * Set string arguments to NULL for cass_user_type_* functions.
 *
 * @jira_ticket CPP-368
 * @test_category data_types:udt
 * @expected_result Error out because a udt can't have a field whose name is null.
 *   However, succeed in storing a null value in a udt field.
 */
CASSANDRA_INTEGRATION_TEST_F(SchemaNullStringApiArgsTest, UserTypeFunctions) {
  CHECK_VERSION(2.2.0);
  const CassDataType* udt_address =
      cass_keyspace_meta_user_type_by_name(keyspace_meta_.get(), "address");
  ASSERT_NE(static_cast<CassDataType*>(NULL), udt_address);

  CassUserType* address = cass_user_type_new_from_data_type(udt_address);
  EXPECT_EQ(CASS_ERROR_LIB_NAME_DOES_NOT_EXIST, cass_user_type_set_null_by_name(address, NULL));
  EXPECT_EQ(CASS_ERROR_LIB_NAME_DOES_NOT_EXIST, cass_user_type_set_int8_by_name(address, NULL, 42));
  EXPECT_EQ(CASS_ERROR_LIB_NAME_DOES_NOT_EXIST,
            cass_user_type_set_int16_by_name(address, NULL, 42));
  EXPECT_EQ(CASS_ERROR_LIB_NAME_DOES_NOT_EXIST,
            cass_user_type_set_int32_by_name(address, NULL, 42));
  EXPECT_EQ(CASS_ERROR_LIB_NAME_DOES_NOT_EXIST,
            cass_user_type_set_uint32_by_name(address, NULL, 42));
  EXPECT_EQ(CASS_ERROR_LIB_NAME_DOES_NOT_EXIST,
            cass_user_type_set_int64_by_name(address, NULL, 42));
  EXPECT_EQ(CASS_ERROR_LIB_NAME_DOES_NOT_EXIST,
            cass_user_type_set_float_by_name(address, NULL, 42));
  EXPECT_EQ(CASS_ERROR_LIB_NAME_DOES_NOT_EXIST,
            cass_user_type_set_double_by_name(address, NULL, 42));
  EXPECT_EQ(CASS_ERROR_LIB_NAME_DOES_NOT_EXIST,
            cass_user_type_set_bool_by_name(address, NULL, cass_false));
  EXPECT_EQ(CASS_OK, cass_user_type_set_string(address, 0, NULL));
  EXPECT_EQ(CASS_ERROR_LIB_NAME_DOES_NOT_EXIST,
            cass_user_type_set_string_by_name(address, NULL, "foo"));
  EXPECT_EQ(CASS_ERROR_LIB_NAME_DOES_NOT_EXIST,
            cass_user_type_set_bytes_by_name(address, NULL,
                                             reinterpret_cast<const cass_byte_t*>("a"), 1));
  EXPECT_EQ(CASS_ERROR_LIB_INVALID_VALUE_TYPE,
            cass_user_type_set_custom(address, 0, "org.foo", NULL, 0));
  EXPECT_EQ(
      CASS_ERROR_LIB_INVALID_VALUE_TYPE,
      cass_user_type_set_custom(address, 0, NULL, reinterpret_cast<const cass_byte_t*>("a"), 1));
  EXPECT_EQ(CASS_ERROR_LIB_NAME_DOES_NOT_EXIST,
            cass_user_type_set_custom_by_name(address, NULL, "org.foo", NULL, 0));
  EXPECT_EQ(CASS_ERROR_LIB_NAME_DOES_NOT_EXIST,
            cass_user_type_set_custom_by_name(address, NULL, NULL,
                                              reinterpret_cast<const cass_byte_t*>("a"), 1));
  EXPECT_EQ(CASS_ERROR_LIB_NAME_DOES_NOT_EXIST,
            cass_user_type_set_custom_by_name(address, "v", "org.foo", NULL, 0));
  EXPECT_EQ(CASS_ERROR_LIB_NAME_DOES_NOT_EXIST,
            cass_user_type_set_custom_by_name(address, "v", NULL,
                                              reinterpret_cast<const cass_byte_t*>("a"), 1));

  UuidGen uuid_generator(11);
  EXPECT_EQ(CASS_ERROR_LIB_NAME_DOES_NOT_EXIST,
            cass_user_type_set_uuid_by_name(address, NULL,
                                            uuid_generator.generate_random_uuid().value()));

  CassInet inet;
  EXPECT_EQ(CASS_OK, cass_inet_from_string("127.1.2.3", &inet));
  EXPECT_EQ(CASS_ERROR_LIB_NAME_DOES_NOT_EXIST,
            cass_user_type_set_inet_by_name(address, NULL, inet));

  const cass_uint8_t pi[] = { 57,  115, 235, 135, 229, 215, 8,   125, 13,  43,  1,   25, 32,  135,
                              129, 180, 112, 176, 158, 120, 246, 235, 29,  145, 238, 50, 108, 239,
                              219, 100, 250, 84,  6,   186, 148, 76,  230, 46,  181, 89, 239, 247 };
  EXPECT_EQ(CASS_ERROR_LIB_NAME_DOES_NOT_EXIST,
            cass_user_type_set_decimal_by_name(address, NULL, pi, sizeof(pi), 100));

  EXPECT_EQ(CASS_ERROR_LIB_NAME_DOES_NOT_EXIST,
            cass_user_type_set_duration_by_name(address, NULL, 1, 2, 3));

  CassCollection* collection = cass_collection_new(CASS_COLLECTION_TYPE_SET, 2);
  EXPECT_EQ(CASS_OK, cass_collection_append_string(collection, "a"));
  EXPECT_EQ(CASS_OK, cass_collection_append_string(collection, "b"));
  EXPECT_EQ(CASS_ERROR_LIB_NAME_DOES_NOT_EXIST,
            cass_user_type_set_collection_by_name(address, NULL, collection));
  cass_collection_free(collection);

  CassTuple* tuple = cass_tuple_new(2);
  EXPECT_EQ(CASS_OK, cass_tuple_set_string(tuple, 0, "a"));
  EXPECT_EQ(CASS_OK, cass_tuple_set_string(tuple, 1, "b"));
  EXPECT_EQ(CASS_ERROR_LIB_NAME_DOES_NOT_EXIST,
            cass_user_type_set_tuple_by_name(address, NULL, tuple));
  cass_tuple_free(tuple);

  CassUserType* address2 = cass_user_type_new_from_data_type(udt_address);
  EXPECT_EQ(CASS_ERROR_LIB_NAME_DOES_NOT_EXIST,
            cass_user_type_set_user_type_by_name(address, NULL, address2));
  cass_user_type_free(address2);

  cass_user_type_free(address);
}

/**
 * Set string arguments to NULL for some miscellaneous cass_* functions
 *
 * @jira_ticket CPP-368
 * @test_category responses:uuid:custom_payload:inet
 * @expected_result Retrieving a column with null name results in a null value;
 *   creating a uuid or inet from a NULL string results in an error;
 *   setting the class name in a custom-payload to null succeeds client-side,
 *   though it will certainly fail when processing on a node.
 */
CASSANDRA_INTEGRATION_TEST_F(SchemaNullStringApiArgsTest, MiscellaneousFunctions) {
  CHECK_VERSION(2.2.0);
  ResultResponse response;
  datastax::internal::core::Row r(&response);
  CassRow* row = CassRow::to(&r);
  EXPECT_EQ(NULL, cass_row_get_column_by_name(row, NULL));

  CassUuid uuid;
  EXPECT_EQ(CASS_ERROR_LIB_BAD_PARAMS, cass_uuid_from_string(NULL, &uuid));

  CassCustomPayload* payload = cass_custom_payload_new();
  cass_custom_payload_set(payload, NULL, reinterpret_cast<const cass_byte_t*>("a"), 1);
  cass_custom_payload_remove(payload, NULL);
  cass_custom_payload_free(payload);

  CassInet inet;
  EXPECT_EQ(CASS_ERROR_LIB_BAD_PARAMS, cass_inet_from_string(NULL, &inet));
}
