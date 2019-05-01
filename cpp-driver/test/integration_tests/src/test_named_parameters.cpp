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
#include "test_utils.hpp"

#include <boost/format.hpp>
#include <boost/test/unit_test.hpp>

#define TOTAL_NUMBER_OF_BATCHES 100

struct NamedParametersTests : public test_utils::SingleSessionTest {
private:
  /**
   * Helper method to create the table name for the create CQL statement
   *
   * @param value_type CassValueType to use for value
   * @param is_prepared True if prepared statement should be used; false
   *                    otherwise
   * @param suffix Suffix to apply to table name
   */
  std::string table_name_builder(CassValueType value_type, bool is_prepared,
                                 std::string suffix = "") {
    std::string table_name =
        "named_parameters_" + std::string(test_utils::get_value_type(value_type)) + "_";
    table_name += is_prepared ? "prepared" : "simple";
    table_name += suffix.empty() ? "" : "_" + suffix;
    return table_name;
  }

public:
  NamedParametersTests()
      : test_utils::SingleSessionTest(1, 0) {
    test_utils::execute_query(session,
                              str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT) %
                                  test_utils::SIMPLE_KEYSPACE % "1"));
    test_utils::execute_query(session, str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));
  }

  ~NamedParametersTests() {
    // Drop the keyspace (ignore any and all errors)
    test_utils::execute_query_with_error(
        session,
        str(boost::format(test_utils::DROP_KEYSPACE_FORMAT) % test_utils::SIMPLE_KEYSPACE));
  }

  /**
   * Insert and validate a datatype
   *
   * @param value_type CassValueType to use for value
   * @param value Value to use
   * @param is_prepared True if prepared statement should be used; false
   *                    otherwise
   */
  template <class T>
  void insert_primitive_value(CassValueType value_type, T value, bool is_prepared) {
    // Create the table for the test
    std::string table_name = table_name_builder(value_type, is_prepared);
    std::string create_table = "CREATE TABLE " + table_name + "(key timeuuid PRIMARY KEY, value " +
                               test_utils::get_value_type(value_type) + ")";
    test_utils::execute_query(session, create_table.c_str());

    // Bind and insert the named value parameter into Cassandra
    CassUuid key = test_utils::generate_time_uuid(uuid_gen);
    std::string insert_query =
        "INSERT INTO " + table_name + "(key, value) VALUES(:named_key, :named_value)";
    test_utils::CassStatementPtr statement(cass_statement_new(insert_query.c_str(), 2));
    if (is_prepared) {
      test_utils::CassPreparedPtr prepared = test_utils::prepare(session, insert_query.c_str());
      statement = test_utils::CassStatementPtr(cass_prepared_bind(prepared.get()));
    }
    BOOST_REQUIRE_EQUAL(cass_statement_bind_uuid_by_name(statement.get(), "named_key", key),
                        CASS_OK);
    BOOST_REQUIRE_EQUAL(test_utils::Value<T>::bind_by_name(statement.get(), "named_value", value),
                        CASS_OK);
    test_utils::wait_and_check_error(
        test_utils::CassFuturePtr(cass_session_execute(session, statement.get())).get());

    // Ensure the named parameter value can be read using a named parameter
    std::string select_query = "SELECT value FROM " + table_name + " WHERE key = :named_key";
    statement = test_utils::CassStatementPtr(cass_statement_new(select_query.c_str(), 1));
    if (is_prepared) {
      test_utils::CassPreparedPtr prepared = test_utils::prepare(session, select_query.c_str());
      statement = test_utils::CassStatementPtr(cass_prepared_bind(prepared.get()));
    }
    BOOST_REQUIRE_EQUAL(cass_statement_bind_uuid_by_name(statement.get(), "named_key", key),
                        CASS_OK);
    test_utils::CassFuturePtr future =
        test_utils::CassFuturePtr(cass_session_execute(session, statement.get()));
    test_utils::wait_and_check_error(future.get());
    test_utils::CassResultPtr result(cass_future_get_result(future.get()));
    BOOST_REQUIRE_EQUAL(cass_result_row_count(result.get()), 1);
    BOOST_REQUIRE_EQUAL(cass_result_column_count(result.get()), 1);
    const CassRow* row = cass_result_first_row(result.get());
    const CassValue* row_value = cass_row_get_column(row, 0);
    T result_value;
    BOOST_REQUIRE(cass_value_type(row_value) == value_type);
    BOOST_REQUIRE_EQUAL(test_utils::Value<T>::get(row_value, &result_value), CASS_OK);
    BOOST_REQUIRE(test_utils::Value<T>::equal(result_value, value));
  }

  /**
   * Insert and validate a batch
   *
   * @param value_type CassValueType to use for value
   * @param value Value to use
   * @param total Total number of batches to insert and validate
   */
  template <class T>
  void insert_primitive_batch_value(CassValueType value_type, T value, unsigned int total) {
    // Create the table for the test
    std::string table_name = table_name_builder(value_type, true, "batch");
    std::string create_table = "CREATE TABLE " + table_name + "(key timeuuid PRIMARY KEY, value " +
                               test_utils::get_value_type(value_type) + ")";
    test_utils::execute_query(session, create_table.c_str());

    // Bind and insert the named value parameter into Cassandra
    test_utils::CassBatchPtr batch(cass_batch_new(CASS_BATCH_TYPE_LOGGED));
    std::string insert_query =
        "INSERT INTO " + table_name + "(key, value) VALUES(:named_key , :named_value)";
    test_utils::CassPreparedPtr prepared = test_utils::prepare(session, insert_query.c_str());
    std::vector<CassUuid> uuids;
    for (unsigned int i = 0; i < total; ++i) {
      CassUuid key = test_utils::generate_time_uuid(uuid_gen);
      uuids.push_back(key);
      test_utils::CassStatementPtr statement(cass_prepared_bind(prepared.get()));
      BOOST_REQUIRE_EQUAL(cass_statement_bind_uuid_by_name(statement.get(), "named_key", key),
                          CASS_OK);
      BOOST_REQUIRE_EQUAL(test_utils::Value<T>::bind_by_name(statement.get(), "named_value", value),
                          CASS_OK);
      cass_batch_add_statement(batch.get(), statement.get());
    }
    test_utils::wait_and_check_error(
        test_utils::CassFuturePtr(cass_session_execute_batch(session, batch.get())).get());

    // Ensure the named parameter can be read using a named parameter
    std::string select_query = "SELECT value FROM " + table_name + " WHERE key=:named_key";
    prepared = test_utils::prepare(session, select_query.c_str());
    for (std::vector<CassUuid>::iterator iterator = uuids.begin(); iterator != uuids.end();
         ++iterator) {
      test_utils::CassStatementPtr statement(cass_prepared_bind(prepared.get()));
      BOOST_REQUIRE_EQUAL(cass_statement_bind_uuid_by_name(statement.get(), "named_key", *iterator),
                          CASS_OK);
      test_utils::CassFuturePtr future =
          test_utils::CassFuturePtr(cass_session_execute(session, statement.get()));
      test_utils::wait_and_check_error(future.get());
      test_utils::CassResultPtr result(cass_future_get_result(future.get()));
      BOOST_REQUIRE_EQUAL(cass_result_row_count(result.get()), 1);
      BOOST_REQUIRE_EQUAL(cass_result_column_count(result.get()), 1);
      const CassRow* row = cass_result_first_row(result.get());
      const CassValue* row_value = cass_row_get_column(row, 0);
      T result_value;
      BOOST_REQUIRE(cass_value_type(row_value) == value_type);
      BOOST_REQUIRE_EQUAL(test_utils::Value<T>::get(row_value, &result_value), CASS_OK);
      BOOST_REQUIRE(test_utils::Value<T>::equal(result_value, value));
    }
  }
};

BOOST_AUTO_TEST_SUITE(named_parameters)

/**
 * Ordered and Unordered Named Parameters
 *
 * This test ensures named parameters can be read/written using Cassandra v2.1+
 * whether the are ordered or unordered.
 *
 * @since 2.1.0-beta
 * @jira_ticket CPP-263
 * @test_category queries:named_parameters
 * @cassandra_version 2.1.x
 */
BOOST_AUTO_TEST_CASE(ordered_unordered_read_write) {
  CCM::CassVersion version = test_utils::get_version();
  if ((version.major_version >= 2 && version.minor_version >= 1) || version.major_version >= 3) {
    NamedParametersTests tester;
    std::string create_table =
        "CREATE TABLE ordered_unordered_read_write(key int PRIMARY KEY, value_text text, "
        "value_uuid uuid, value_blob blob, value_list_floats list<float>)";
    std::string insert_query =
        "INSERT INTO ordered_unordered_read_write(key, value_text, value_uuid, value_blob, "
        "value_list_floats) VALUES (:key, :one_text, :two_uuid, :three_blob, :four_list_floats)";
    std::string select_query = "SELECT value_text, value_uuid, value_blob, value_list_floats FROM "
                               "ordered_unordered_read_write WHERE key=?";

    // Create the table and statement for the test
    test_utils::execute_query(tester.session, create_table.c_str());

    // Insert and read elements in the order of their named query parameters
    {
      // Create and insert values for the insert statement
      test_utils::CassStatementPtr statement(cass_statement_new(insert_query.c_str(), 5));
      CassString text("Named parameters - In Order");
      CassUuid uuid = test_utils::generate_random_uuid(tester.uuid_gen);
      CassBytes blob = test_utils::bytes_from_string(text.data);
      BOOST_REQUIRE_EQUAL(test_utils::Value<cass_int32_t>::bind_by_name(statement.get(), "key", 1),
                          CASS_OK);
      BOOST_REQUIRE_EQUAL(
          test_utils::Value<CassString>::bind_by_name(statement.get(), "one_text", text), CASS_OK);
      BOOST_REQUIRE_EQUAL(
          test_utils::Value<CassUuid>::bind_by_name(statement.get(), "two_uuid", uuid), CASS_OK);
      BOOST_REQUIRE_EQUAL(
          test_utils::Value<CassBytes>::bind_by_name(statement.get(), "three_blob", blob), CASS_OK);
      test_utils::CassCollectionPtr list(cass_collection_new(CASS_COLLECTION_TYPE_LIST, 1));
      test_utils::Value<cass_float_t>::append(list.get(), 0.01f);
      BOOST_REQUIRE_EQUAL(
          cass_statement_bind_collection_by_name(statement.get(), "four_list_floats", list.get()),
          CASS_OK);
      test_utils::wait_and_check_error(
          test_utils::CassFuturePtr(cass_session_execute(tester.session, statement.get())).get());

      // Ensure the named query parameters can be read
      statement = test_utils::CassStatementPtr(cass_statement_new(select_query.c_str(), 1));
      BOOST_REQUIRE_EQUAL(cass_statement_bind_int32(statement.get(), 0, 1), CASS_OK);
      test_utils::CassFuturePtr future =
          test_utils::CassFuturePtr(cass_session_execute(tester.session, statement.get()));
      test_utils::wait_and_check_error(future.get());
      test_utils::CassResultPtr result(cass_future_get_result(future.get()));
      BOOST_REQUIRE_EQUAL(cass_result_row_count(result.get()), 1);
      BOOST_REQUIRE_EQUAL(cass_result_column_count(result.get()), 4);
      const CassRow* row = cass_result_first_row(result.get());
      CassString value_text;
      BOOST_REQUIRE_EQUAL(
          test_utils::Value<CassString>::get(cass_row_get_column(row, 0), &value_text), CASS_OK);
      BOOST_REQUIRE(test_utils::Value<CassString>::equal(value_text, text));
      CassUuid value_uuid;
      BOOST_REQUIRE_EQUAL(
          test_utils::Value<CassUuid>::get(cass_row_get_column(row, 1), &value_uuid), CASS_OK);
      BOOST_REQUIRE(test_utils::Value<CassUuid>::equal(value_uuid, uuid));
      CassBytes value_blob;
      BOOST_REQUIRE_EQUAL(
          test_utils::Value<CassBytes>::get(cass_row_get_column(row, 2), &value_blob), CASS_OK);
      BOOST_REQUIRE(test_utils::Value<CassBytes>::equal(value_blob, blob));
      test_utils::CassIteratorPtr iterator(
          cass_iterator_from_collection(cass_row_get_column(row, 3)));
      cass_iterator_next(iterator.get());
      cass_float_t value_float;
      BOOST_REQUIRE_EQUAL(test_utils::Value<cass_float_t>::get(
                              cass_iterator_get_value(iterator.get()), &value_float),
                          CASS_OK);
      BOOST_REQUIRE(test_utils::Value<cass_float_t>::equal(value_float, 0.01f));
    }

    // Insert and read elements out of the order of their named query parameters
    {
      // Create and insert values for the insert statement
      test_utils::CassStatementPtr statement(cass_statement_new(insert_query.c_str(), 5));
      CassString text("Named parameters - Out of Order");
      CassUuid uuid = test_utils::generate_random_uuid(tester.uuid_gen);
      CassBytes blob = test_utils::bytes_from_string(text.data);
      BOOST_REQUIRE_EQUAL(
          test_utils::Value<CassBytes>::bind_by_name(statement.get(), "three_blob", blob), CASS_OK);
      BOOST_REQUIRE_EQUAL(
          test_utils::Value<CassString>::bind_by_name(statement.get(), "one_text", text), CASS_OK);
      test_utils::CassCollectionPtr list(cass_collection_new(CASS_COLLECTION_TYPE_LIST, 1));
      test_utils::Value<cass_float_t>::append(list.get(), 0.02f);
      BOOST_REQUIRE_EQUAL(
          cass_statement_bind_collection_by_name(statement.get(), "four_list_floats", list.get()),
          CASS_OK);
      BOOST_REQUIRE_EQUAL(test_utils::Value<cass_int32_t>::bind_by_name(statement.get(), "key", 2),
                          CASS_OK);
      BOOST_REQUIRE_EQUAL(
          test_utils::Value<CassUuid>::bind_by_name(statement.get(), "two_uuid", uuid), CASS_OK);
      test_utils::wait_and_check_error(
          test_utils::CassFuturePtr(cass_session_execute(tester.session, statement.get())).get());

      // Ensure the named query parameters can be read
      statement = test_utils::CassStatementPtr(cass_statement_new(select_query.c_str(), 1));
      BOOST_REQUIRE_EQUAL(cass_statement_bind_int32(statement.get(), 0, 2), CASS_OK);
      test_utils::CassFuturePtr future =
          test_utils::CassFuturePtr(cass_session_execute(tester.session, statement.get()));
      test_utils::wait_and_check_error(future.get());
      test_utils::CassResultPtr result(cass_future_get_result(future.get()));
      BOOST_REQUIRE_EQUAL(cass_result_row_count(result.get()), 1);
      BOOST_REQUIRE_EQUAL(cass_result_column_count(result.get()), 4);
      const CassRow* row = cass_result_first_row(result.get());
      CassBytes value_blob;
      BOOST_REQUIRE_EQUAL(
          test_utils::Value<CassBytes>::get(cass_row_get_column(row, 2), &value_blob), CASS_OK);
      BOOST_REQUIRE(test_utils::Value<CassBytes>::equal(value_blob, blob));
      CassString value_text;
      BOOST_REQUIRE_EQUAL(
          test_utils::Value<CassString>::get(cass_row_get_column(row, 0), &value_text), CASS_OK);
      BOOST_REQUIRE(test_utils::Value<CassString>::equal(value_text, text));
      test_utils::CassIteratorPtr iterator(
          cass_iterator_from_collection(cass_row_get_column(row, 3)));
      cass_iterator_next(iterator.get());
      cass_float_t value_float;
      BOOST_REQUIRE_EQUAL(test_utils::Value<cass_float_t>::get(
                              cass_iterator_get_value(iterator.get()), &value_float),
                          CASS_OK);
      BOOST_REQUIRE(test_utils::Value<cass_float_t>::equal(value_float, 0.02f));
      CassUuid value_uuid;
      BOOST_REQUIRE_EQUAL(
          test_utils::Value<CassUuid>::get(cass_row_get_column(row, 1), &value_uuid), CASS_OK);
      BOOST_REQUIRE(test_utils::Value<CassUuid>::equal(value_uuid, uuid));
    }

    // Insert and read elements using prepared statement named query parameters
    {
      // Create and insert values for the insert statement
      test_utils::CassPreparedPtr prepared = test_utils::prepare(tester.session, insert_query);
      test_utils::CassStatementPtr statement(cass_prepared_bind(prepared.get()));
      CassString text("Named parameters - Prepared Statement");
      CassUuid uuid = test_utils::generate_random_uuid(tester.uuid_gen);
      CassBytes blob = test_utils::bytes_from_string(text.data);
      BOOST_REQUIRE_EQUAL(
          test_utils::Value<CassBytes>::bind_by_name(statement.get(), "three_blob", blob), CASS_OK);
      BOOST_REQUIRE_EQUAL(
          test_utils::Value<CassString>::bind_by_name(statement.get(), "one_text", text), CASS_OK);
      test_utils::CassCollectionPtr list(cass_collection_new(CASS_COLLECTION_TYPE_LIST, 1));
      test_utils::Value<cass_float_t>::append(list.get(), 0.03f);
      BOOST_REQUIRE_EQUAL(
          cass_statement_bind_collection_by_name(statement.get(), "four_list_floats", list.get()),
          CASS_OK);
      BOOST_REQUIRE_EQUAL(test_utils::Value<cass_int32_t>::bind_by_name(statement.get(), "key", 3),
                          CASS_OK);
      BOOST_REQUIRE_EQUAL(
          test_utils::Value<CassUuid>::bind_by_name(statement.get(), "two_uuid", uuid), CASS_OK);
      test_utils::wait_and_check_error(
          test_utils::CassFuturePtr(cass_session_execute(tester.session, statement.get())).get());

      // Ensure the named query parameters can be read
      statement = test_utils::CassStatementPtr(cass_statement_new(select_query.c_str(), 1));
      BOOST_REQUIRE_EQUAL(cass_statement_bind_int32(statement.get(), 0, 3), CASS_OK);
      test_utils::CassFuturePtr future =
          test_utils::CassFuturePtr(cass_session_execute(tester.session, statement.get()));
      test_utils::wait_and_check_error(future.get());
      test_utils::CassResultPtr result(cass_future_get_result(future.get()));
      BOOST_REQUIRE_EQUAL(cass_result_row_count(result.get()), 1);
      BOOST_REQUIRE_EQUAL(cass_result_column_count(result.get()), 4);
      const CassRow* row = cass_result_first_row(result.get());
      CassBytes value_blob;
      BOOST_REQUIRE_EQUAL(
          test_utils::Value<CassBytes>::get(cass_row_get_column(row, 2), &value_blob), CASS_OK);
      BOOST_REQUIRE(test_utils::Value<CassBytes>::equal(value_blob, blob));
      CassString value_text;
      BOOST_REQUIRE_EQUAL(
          test_utils::Value<CassString>::get(cass_row_get_column(row, 0), &value_text), CASS_OK);
      BOOST_REQUIRE(test_utils::Value<CassString>::equal(value_text, text));
      test_utils::CassIteratorPtr iterator(
          cass_iterator_from_collection(cass_row_get_column(row, 3)));
      cass_iterator_next(iterator.get());
      cass_float_t value_float;
      BOOST_REQUIRE_EQUAL(test_utils::Value<cass_float_t>::get(
                              cass_iterator_get_value(iterator.get()), &value_float),
                          CASS_OK);
      BOOST_REQUIRE(test_utils::Value<cass_float_t>::equal(value_float, 0.03f));
      CassUuid value_uuid;
      BOOST_REQUIRE_EQUAL(
          test_utils::Value<CassUuid>::get(cass_row_get_column(row, 1), &value_uuid), CASS_OK);
      BOOST_REQUIRE(test_utils::Value<CassUuid>::equal(value_uuid, uuid));
    }
  } else {
    std::cout << "Unsupported Test for Cassandra v" << version.to_string()
              << ": Skipping named_parameters/ordered_unordered_read_write" << std::endl;
    BOOST_REQUIRE(true);
  }
}

/**
 * Bound/Prepared Statements Using All Primitive Datatype for named parameters
 *
 * This test ensures named parameters can be read/written using Cassandra v2.1+
 * for all primitive datatypes either bound or prepared statements
 *
 * @since 2.1.0-beta
 * @jira_ticket CPP-263
 * @test_category queries:named_parameters
 * @cassandra_version 2.1.x
 */
BOOST_AUTO_TEST_CASE(all_primitives) {
  CCM::CassVersion version = test_utils::get_version();
  if ((version.major_version >= 2 && version.minor_version >= 1) || version.major_version >= 3) {
    NamedParametersTests tester;
    for (unsigned int i = 0; i < 2; ++i) {
      bool is_prepared = i == 0 ? false : true;

      {
        CassString value("Test Value");
        tester.insert_primitive_value<CassString>(CASS_VALUE_TYPE_ASCII, value, is_prepared);
        tester.insert_primitive_value<CassString>(CASS_VALUE_TYPE_VARCHAR, value,
                                                  is_prepared); // NOTE: text is alias for varchar
      }

      {
        cass_int64_t value = 1234567890;
        tester.insert_primitive_value<cass_int64_t>(CASS_VALUE_TYPE_BIGINT, value, is_prepared);
        tester.insert_primitive_value<cass_int64_t>(CASS_VALUE_TYPE_TIMESTAMP, value, is_prepared);
      }

      {
        CassBytes value = test_utils::bytes_from_string(
            "012345678900123456789001234567890012345678900123456789001234567890");
        tester.insert_primitive_value<CassBytes>(CASS_VALUE_TYPE_BLOB, value, is_prepared);
        tester.insert_primitive_value<CassBytes>(CASS_VALUE_TYPE_VARINT, value, is_prepared);
      }

      tester.insert_primitive_value<cass_bool_t>(CASS_VALUE_TYPE_BOOLEAN, cass_true, is_prepared);

      {
        const cass_uint8_t pi[] = { 57,  115, 235, 135, 229, 215, 8,   125, 13,  43,  1,
                                    25,  32,  135, 129, 180, 112, 176, 158, 120, 246, 235,
                                    29,  145, 238, 50,  108, 239, 219, 100, 250, 84,  6,
                                    186, 148, 76,  230, 46,  181, 89,  239, 247 };
        const cass_int32_t pi_scale = 100;
        CassDecimal value = CassDecimal(pi, sizeof(pi), pi_scale);
        tester.insert_primitive_value<CassDecimal>(CASS_VALUE_TYPE_DECIMAL, value, is_prepared);
      }

      if ((version.major_version >= 3 && version.minor_version >= 10) ||
          version.major_version >= 4) {
        tester.insert_primitive_value<CassDuration>(CASS_VALUE_TYPE_DURATION, CassDuration(1, 2, 3),
                                                    is_prepared);
      }

      tester.insert_primitive_value<cass_double_t>(CASS_VALUE_TYPE_DOUBLE, 3.141592653589793,
                                                   is_prepared);
      tester.insert_primitive_value<cass_float_t>(CASS_VALUE_TYPE_FLOAT, 3.1415926f, is_prepared);
      tester.insert_primitive_value<cass_int32_t>(CASS_VALUE_TYPE_INT, 123, is_prepared);
      if ((version.major_version >= 2 && version.minor_version >= 2) ||
          version.major_version >= 3) {
        tester.insert_primitive_value<cass_int16_t>(CASS_VALUE_TYPE_SMALL_INT, 123, is_prepared);
        tester.insert_primitive_value<cass_int8_t>(CASS_VALUE_TYPE_TINY_INT, 123, is_prepared);
      }

      {
        CassUuid value = test_utils::generate_random_uuid(tester.uuid_gen);
        tester.insert_primitive_value<CassUuid>(CASS_VALUE_TYPE_UUID, value, is_prepared);
      }

      {
        CassInet value = test_utils::inet_v4_from_int(16777343); // 127.0.0.1
        tester.insert_primitive_value<CassInet>(CASS_VALUE_TYPE_INET, value, is_prepared);
      }

      {
        CassUuid value = test_utils::generate_time_uuid(tester.uuid_gen);
        tester.insert_primitive_value<CassUuid>(CASS_VALUE_TYPE_TIMEUUID, value, is_prepared);
      }
    }
  } else {
    std::cout << "Unsupported Test for Cassandra v" << version.to_string()
              << ": Skipping named_parameters/all_primitives" << std::endl;
    BOOST_REQUIRE(true);
  }
}

/**
 * Batch Statements Using All Primitive Datatype for named parameters
 *
 * This test ensures named parameters can be read/written using Cassandra v2.1+
 * for all primitive datatypes using batched statements
 *
 * @since 2.1.0-beta
 * @jira_ticket CPP-263
 * @test_category queries:named_parameters
 * @cassandra_version 2.1.x
 */
BOOST_AUTO_TEST_CASE(all_primitives_batched) {
  CCM::CassVersion version = test_utils::get_version();
  if ((version.major_version >= 2 && version.minor_version >= 1) || version.major_version >= 3) {
    NamedParametersTests tester;

    {
      CassString value("Test Value");
      tester.insert_primitive_batch_value<CassString>(CASS_VALUE_TYPE_ASCII, value,
                                                      TOTAL_NUMBER_OF_BATCHES);
      tester.insert_primitive_batch_value<CassString>(
          CASS_VALUE_TYPE_VARCHAR, value,
          TOTAL_NUMBER_OF_BATCHES); // NOTE: text is alias for varchar
    }

    {
      cass_int64_t value = 1234567890;
      tester.insert_primitive_batch_value<cass_int64_t>(CASS_VALUE_TYPE_BIGINT, value,
                                                        TOTAL_NUMBER_OF_BATCHES);
      tester.insert_primitive_batch_value<cass_int64_t>(CASS_VALUE_TYPE_TIMESTAMP, value,
                                                        TOTAL_NUMBER_OF_BATCHES);
    }

    {
      CassBytes value = test_utils::bytes_from_string(
          "012345678900123456789001234567890012345678900123456789001234567890");
      tester.insert_primitive_batch_value<CassBytes>(CASS_VALUE_TYPE_BLOB, value,
                                                     TOTAL_NUMBER_OF_BATCHES);
      tester.insert_primitive_batch_value<CassBytes>(CASS_VALUE_TYPE_VARINT, value,
                                                     TOTAL_NUMBER_OF_BATCHES);
    }

    tester.insert_primitive_batch_value<cass_bool_t>(CASS_VALUE_TYPE_BOOLEAN, cass_true,
                                                     TOTAL_NUMBER_OF_BATCHES);

    {
      const cass_uint8_t pi[] = { 57,  115, 235, 135, 229, 215, 8,   125, 13,  43,  1,
                                  25,  32,  135, 129, 180, 112, 176, 158, 120, 246, 235,
                                  29,  145, 238, 50,  108, 239, 219, 100, 250, 84,  6,
                                  186, 148, 76,  230, 46,  181, 89,  239, 247 };
      const cass_int32_t pi_scale = 100;
      CassDecimal value = CassDecimal(pi, sizeof(pi), pi_scale);
      tester.insert_primitive_batch_value<CassDecimal>(CASS_VALUE_TYPE_DECIMAL, value,
                                                       TOTAL_NUMBER_OF_BATCHES);
    }

    if ((version.major_version >= 3 && version.minor_version >= 10) || version.major_version >= 4) {
      tester.insert_primitive_batch_value<CassDuration>(
          CASS_VALUE_TYPE_DURATION, CassDuration(1, 2, 3), TOTAL_NUMBER_OF_BATCHES);
    }

    tester.insert_primitive_batch_value<cass_double_t>(CASS_VALUE_TYPE_DOUBLE, 3.141592653589793,
                                                       TOTAL_NUMBER_OF_BATCHES);
    tester.insert_primitive_batch_value<cass_float_t>(CASS_VALUE_TYPE_FLOAT, 3.1415926f,
                                                      TOTAL_NUMBER_OF_BATCHES);
    tester.insert_primitive_batch_value<cass_int32_t>(CASS_VALUE_TYPE_INT, 123,
                                                      TOTAL_NUMBER_OF_BATCHES);
    if ((version.major_version >= 2 && version.minor_version >= 2) || version.major_version >= 3) {
      tester.insert_primitive_batch_value<cass_int16_t>(CASS_VALUE_TYPE_SMALL_INT, 123,
                                                        TOTAL_NUMBER_OF_BATCHES);
      tester.insert_primitive_batch_value<cass_int8_t>(CASS_VALUE_TYPE_TINY_INT, 123,
                                                       TOTAL_NUMBER_OF_BATCHES);
    }

    {
      CassUuid value = test_utils::generate_random_uuid(tester.uuid_gen);
      tester.insert_primitive_batch_value<CassUuid>(CASS_VALUE_TYPE_UUID, value,
                                                    TOTAL_NUMBER_OF_BATCHES);
    }

    {
      CassInet value = test_utils::inet_v4_from_int(16777343); // 127.0.0.1
      tester.insert_primitive_batch_value<CassInet>(CASS_VALUE_TYPE_INET, value,
                                                    TOTAL_NUMBER_OF_BATCHES);
    }

    {
      CassUuid value = test_utils::generate_time_uuid(tester.uuid_gen);
      tester.insert_primitive_batch_value<CassUuid>(CASS_VALUE_TYPE_TIMEUUID, value,
                                                    TOTAL_NUMBER_OF_BATCHES);
    }
  } else {
    std::cout << "Unsupported Test for Cassandra v" << version.to_string()
              << ": Skipping named_parameters/all_primitives" << std::endl;
    BOOST_REQUIRE(true);
  }
}

/**
 * Bound/Prepared Statements Using invalid named parameters
 *
 * This test ensures invalid named parameters return errors when prepared or
 * executed.
 *
 * @since 2.1.0-beta
 * @jira_ticket CPP-263
 * @test_category queries:named_parameters
 * @cassandra_version 2.1.x
 */
BOOST_AUTO_TEST_CASE(invalid_name) {
  CCM::CassVersion version = test_utils::get_version();
  if ((version.major_version >= 2 && version.minor_version >= 1) || version.major_version >= 3) {
    NamedParametersTests tester;
    std::string create_table =
        "CREATE TABLE named_parameter_invalid(key int PRIMARY KEY, value text)";
    std::string insert_query =
        "INSERT INTO named_parameter_invalid(key, value) VALUES (:key_name, :value_name)";

    // Create the table and statement for the test
    test_utils::execute_query(tester.session, create_table.c_str());

    // Invalid name
    {
      // Simple
      test_utils::CassStatementPtr statement(cass_statement_new(insert_query.c_str(), 2));
      BOOST_REQUIRE_EQUAL(
          test_utils::Value<cass_int32_t>::bind_by_name(statement.get(), "invalid_key_name", 0),
          CASS_OK);
      BOOST_REQUIRE_EQUAL(test_utils::Value<CassString>::bind_by_name(
                              statement.get(), "invalid_value_name", CassString("invalid")),
                          CASS_OK);
      BOOST_REQUIRE_EQUAL(
          test_utils::wait_and_return_error(
              test_utils::CassFuturePtr(cass_session_execute(tester.session, statement.get()))
                  .get()),
          CASS_ERROR_SERVER_INVALID_QUERY);

      // Prepared
      test_utils::CassPreparedPtr prepared =
          test_utils::prepare(tester.session, insert_query.c_str());
      statement = test_utils::CassStatementPtr(cass_prepared_bind(prepared.get()));
      BOOST_REQUIRE_EQUAL(
          test_utils::Value<cass_int32_t>::bind_by_name(statement.get(), "invalid_key_name", 0),
          CASS_ERROR_LIB_NAME_DOES_NOT_EXIST);
      BOOST_REQUIRE_EQUAL(test_utils::Value<CassString>::bind_by_name(
                              statement.get(), "invalid_value_name", CassString("invalid")),
                          CASS_ERROR_LIB_NAME_DOES_NOT_EXIST);
    }
  } else {
    std::cout << "Unsupported Test for Cassandra v" << version.to_string()
              << ": Skipping named_parameters/invalid" << std::endl;
    BOOST_REQUIRE(true);
  }
}

BOOST_AUTO_TEST_SUITE_END()
