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

#include "test_utils.hpp"
#include "cassandra.h"

#include <boost/format.hpp>
#include <boost/test/unit_test.hpp>

#define NESTED_COLLECTION_SIZE_LIMITER 50 // Limit nested collections size (as to not exceed high water mark)

struct TupleTests : public test_utils::SingleSessionTest {
private:
  /**
   * Helper method to create the table name for the create CQL statement
   *
   * @param tuple_type CassValueType to use for value
   * @param size Size of the tuple
   * @param collection_type Value type to use when overriding primary subtype
   *                        NOTE: This value must map to a CassCollectionType to
   *                        be considered for tuple.
   */
  std::string table_name_builder(CassValueType tuple_type, unsigned int size, CassValueType collection_type) {
    // Build the table prefix
    std::string cql_value_type = test_utils::get_value_type(tuple_type);
    std::string table_prefix = cql_value_type;
    if (collection_type == CASS_VALUE_TYPE_LIST || collection_type == CASS_VALUE_TYPE_MAP || collection_type == CASS_VALUE_TYPE_SET || collection_type == CASS_VALUE_TYPE_TUPLE) {
      std::string collection_cql_value_type = test_utils::get_value_type(collection_type);
      table_prefix = cql_value_type + "_" + collection_cql_value_type;
    }

    // Finalize the table name and return the result
    std::stringstream table_name;
    table_name << "tuple_" << table_prefix << "_" << size;
    return table_name.str();
  }

  /**
   * Helper method to build the CQL for a tuple datatype
   *
   * @param tuple_type CassValueType to use for value
   * @param size Size of the tuple
   * @param collection_type Value type to use when overriding primary subtype
   *                        NOTE: This value must map to a CassCollectionType to
   *                        be considered for tuple.
   * @param is_frozen True if frozen; false otherwise (default: true)
   */
  std::string tuple_cql_builder(CassValueType tuple_type, unsigned int size, CassValueType collection_type, bool is_frozen = true) {
    std::string cql_value_type = test_utils::get_value_type(tuple_type);
    std::string tuple_value_types = cql_value_type;

    // Determine if this is a nested collection
    if (collection_type == CASS_VALUE_TYPE_LIST || collection_type == CASS_VALUE_TYPE_MAP || collection_type == CASS_VALUE_TYPE_SET || collection_type == CASS_VALUE_TYPE_TUPLE) {
      // Build the nested collection
      std::string collection_cql_value_type = test_utils::get_value_type(collection_type);
      tuple_value_types = collection_cql_value_type + "<";
      if (collection_type == CASS_VALUE_TYPE_MAP) {
        tuple_value_types += cql_value_type + ", ";
      } else if (collection_type == CASS_VALUE_TYPE_TUPLE) {
        for (unsigned int i = 1; i < size; ++i) {
          tuple_value_types += cql_value_type + ", ";
        }
      }
      tuple_value_types += cql_value_type + ">";
      cql_value_type = tuple_value_types;
    }

    // Finalize the CQL and return the tuple
    for (unsigned int i = 1; i < size; ++i) {
      tuple_value_types += ", " + cql_value_type;
    }
    std::string prefix("tuple<");
    std::string suffix(">");
    if (is_frozen) {
      prefix = "frozen<" + prefix;
      suffix += ">";
    }
    return prefix + tuple_value_types + suffix;
  }

public:
  /**
   * Varying sizes for number of items in tuple
   */
  static const unsigned int sizes_[];
  /**
   * Value types associated with nested collections
   *
   * NOTE: Includes CASS_VALUE_TYPE_UNKNOWN for looping
   */
  static const CassValueType nested_collection_types_[];

  TupleTests() : test_utils::SingleSessionTest(1, 0) {
    test_utils::execute_query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT) % test_utils::SIMPLE_KEYSPACE % "1"));
    test_utils::execute_query(session, str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));
  }

  ~TupleTests() {
    // Drop the keyspace (ignore any and all errors)
    test_utils::execute_query_with_error(session,
      str(boost::format(test_utils::DROP_KEYSPACE_FORMAT)
      % test_utils::SIMPLE_KEYSPACE));
  }

  /**
   * Insert and validate a tuple of varying size
   *
   * @param tuple_type CassValueType to use for value
   * @param tuple_values Value to use for all tuple values
   * @param size Size of the tuple (default: 1)
   * @param collection_type Value type to use when overriding primary subtype
   *                        (default: CASS_VALUE_TYPE_UNKNOWN; indicates not
   *                        overridden). NOTE: This value must map to a
   *                        CassCollectionType to be considered for tuple.
   */
  template <class T>
  void insert_varying_sized_value(CassValueType tuple_type, T tuple_values, unsigned int size = 1, CassValueType collection_type = CASS_VALUE_TYPE_UNKNOWN) {
    // Create the table for the test
    std::string table_name = table_name_builder(tuple_type, size, collection_type);
    std::string tuple_cql = tuple_cql_builder(tuple_type, size, collection_type);
    std::string create_table = "CREATE TABLE " + table_name + "(key timeuuid PRIMARY KEY, value " + tuple_cql + ")";
    test_utils::execute_query(session, create_table.c_str());

    // Insert the values into the tuple collection
    test_utils::CassTuplePtr tuple(cass_tuple_new(size));
    for (unsigned int i = 0; i < size; ++i) {
      // Handle collection subtypes
      if (collection_type == CASS_VALUE_TYPE_LIST || collection_type == CASS_VALUE_TYPE_MAP || collection_type == CASS_VALUE_TYPE_SET || collection_type == CASS_VALUE_TYPE_TUPLE) {
        test_utils::CassCollectionPtr collection(cass_collection_new(static_cast<CassCollectionType>(collection_type), size));
        for (unsigned int j = 0; j < size; ++j) {
          BOOST_REQUIRE_EQUAL(test_utils::Value<T>::append(collection.get(), tuple_values), CASS_OK);
          if (collection_type == CASS_VALUE_TYPE_MAP) {
            BOOST_REQUIRE_EQUAL(test_utils::Value<T>::append(collection.get(), tuple_values), CASS_OK);
          }
        }
        BOOST_REQUIRE_EQUAL(cass_tuple_set_collection(tuple.get(), i, collection.get()), CASS_OK);
      } else {
        // Handle regular primitive types
        BOOST_REQUIRE_EQUAL(test_utils::Value<T>::tuple_set(tuple.get(), i, tuple_values), CASS_OK);
      }
    }

    // Bind and insert the tuple collection into Cassandra
    CassUuid key = test_utils::generate_time_uuid(uuid_gen);
    std::string insert_query = "INSERT INTO " + table_name + "(key, value) VALUES(? , ?)";
    test_utils::CassStatementPtr statement(cass_statement_new(insert_query.c_str(), 2));
    BOOST_REQUIRE_EQUAL(cass_statement_bind_uuid(statement.get(), 0, key), CASS_OK);
    BOOST_REQUIRE_EQUAL(cass_statement_bind_tuple(statement.get(), 1, tuple.get()), CASS_OK);
    test_utils::wait_and_check_error(test_utils::CassFuturePtr(cass_session_execute(session, statement.get())).get());

    // Ensure the tuple collection can be read
    std::string select_query = "SELECT value FROM " + table_name + " WHERE key=?";
    statement = test_utils::CassStatementPtr(cass_statement_new(select_query.c_str(), 1));
    BOOST_REQUIRE_EQUAL(cass_statement_bind_uuid(statement.get(), 0, key), CASS_OK);
    test_utils::CassFuturePtr future = test_utils::CassFuturePtr(cass_session_execute(session, statement.get()));
    test_utils::wait_and_check_error(future.get());
    test_utils::CassResultPtr result(cass_future_get_result(future.get()));
    BOOST_REQUIRE_EQUAL(cass_result_row_count(result.get()), 1);
    BOOST_REQUIRE_EQUAL(cass_result_column_count(result.get()), 1);
    const CassRow* row = cass_result_first_row(result.get());
    const CassValue* value = cass_row_get_column(row, 0);
    BOOST_REQUIRE_EQUAL(cass_value_type(value), CASS_VALUE_TYPE_TUPLE);
    BOOST_REQUIRE_EQUAL(cass_value_primary_sub_type(value), CASS_VALUE_TYPE_UNKNOWN);
    BOOST_REQUIRE_EQUAL(cass_value_secondary_sub_type(value), CASS_VALUE_TYPE_UNKNOWN);
    test_utils::CassIteratorPtr tuple_iterator(cass_iterator_from_tuple(value));
    BOOST_REQUIRE_EQUAL(cass_value_item_count(value), size);
    int count = 0;
    while (cass_iterator_next(tuple_iterator.get())) {
      ++count;
      const CassValue* abstract_collection_value = cass_iterator_get_value(tuple_iterator.get());

      // Handle collection subtypes (and nested tuples)
      if (cass_value_is_collection(abstract_collection_value)) {
        BOOST_REQUIRE_EQUAL(cass_value_type(abstract_collection_value), collection_type);
        BOOST_REQUIRE_EQUAL(cass_value_primary_sub_type(abstract_collection_value), tuple_type);
        BOOST_REQUIRE_EQUAL(cass_value_secondary_sub_type(abstract_collection_value), collection_type == CASS_VALUE_TYPE_MAP ? tuple_type : CASS_VALUE_TYPE_UNKNOWN);
        BOOST_REQUIRE_EQUAL(cass_value_item_count(abstract_collection_value), size);

        test_utils::CassIteratorPtr subtype_iterator(cass_iterator_from_collection(abstract_collection_value));
        int subtype_count = 0;
        while (cass_iterator_next(subtype_iterator.get())) {
          ++subtype_count;
          T collection_value;
          BOOST_REQUIRE(cass_value_type(cass_iterator_get_value(subtype_iterator.get())) == tuple_type);
          BOOST_REQUIRE_EQUAL(test_utils::Value<T>::get(cass_iterator_get_value(subtype_iterator.get()), &collection_value), CASS_OK);
          BOOST_REQUIRE(test_utils::Value<T>::equal(collection_value, tuple_values));
          if (collection_type == CASS_VALUE_TYPE_MAP) {
            BOOST_REQUIRE(cass_iterator_next(subtype_iterator.get()) == cass_true);
            BOOST_REQUIRE_EQUAL(test_utils::Value<T>::get(cass_iterator_get_value(subtype_iterator.get()), &collection_value), CASS_OK);
            BOOST_REQUIRE(test_utils::Value<T>::equal(collection_value, tuple_values));
          }
        }
        BOOST_REQUIRE_EQUAL(subtype_count, size);
      } else {
        T tuple_value;
        BOOST_REQUIRE_EQUAL(test_utils::Value<T>::get(abstract_collection_value, &tuple_value), CASS_OK);
        BOOST_REQUIRE(test_utils::Value<T>::equal(tuple_value, tuple_values));
      }
    }
    BOOST_REQUIRE_EQUAL(count, size);
  }

  /**
  * Insert and validate a tuple of varying size
  *
  * @param tuple_type CassValueType to use for value
  * @param size Size of the tuple (default: 1)
  * @param collection_type Value type to use when overriding primary subtype
  *                        (default: CASS_VALUE_TYPE_UNKNOWN; indicates not
  *                        overridden). NOTE: This value must map to a
  *                        CassCollectionType to be considered for tuple.
  */
  template <class T>
  void insert_varying_sized_null_value(CassValueType tuple_type, unsigned int size = 1, CassValueType collection_type = CASS_VALUE_TYPE_UNKNOWN) {
    // Create the table for the test
    std::string table_name = table_name_builder(tuple_type, size, collection_type) + "_null";
    std::string tuple_cql = tuple_cql_builder(tuple_type, size, collection_type);
    std::string create_table = "CREATE TABLE " + table_name + "(key timeuuid PRIMARY KEY, value " + tuple_cql + ")";
    test_utils::execute_query(session, create_table.c_str());

    // Create empty (null) tuple collection
    test_utils::CassTuplePtr tuple(cass_tuple_new(size));
    for (unsigned int i = 0; i < size; ++i) {
      // Handle collection subtypes
      if (collection_type == CASS_VALUE_TYPE_LIST || collection_type == CASS_VALUE_TYPE_MAP || collection_type == CASS_VALUE_TYPE_SET || collection_type == CASS_VALUE_TYPE_TUPLE) {
        test_utils::CassCollectionPtr collection(cass_collection_new(static_cast<CassCollectionType>(collection_type), size));
        BOOST_REQUIRE_EQUAL(cass_tuple_set_collection(tuple.get(), i, collection.get()), CASS_OK);
      }
    }

    // Bind and insert the tuple collection into Cassandra
    CassUuid key = test_utils::generate_time_uuid(uuid_gen);
    std::string insert_query = "INSERT INTO " + table_name + "(key, value) VALUES(? , ?)";
    test_utils::CassStatementPtr statement(cass_statement_new(insert_query.c_str(), 2));
    BOOST_REQUIRE_EQUAL(cass_statement_bind_uuid(statement.get(), 0, key), CASS_OK);
    BOOST_REQUIRE_EQUAL(cass_statement_bind_tuple(statement.get(), 1, tuple.get()), CASS_OK);
    test_utils::wait_and_check_error(test_utils::CassFuturePtr(cass_session_execute(session, statement.get())).get());

    // Ensure the tuple collection can be read
    std::string select_query = "SELECT value FROM " + table_name + " WHERE key=?";
    statement = test_utils::CassStatementPtr(cass_statement_new(select_query.c_str(), 1));
    BOOST_REQUIRE_EQUAL(cass_statement_bind_uuid(statement.get(), 0, key), CASS_OK);
    test_utils::CassFuturePtr future = test_utils::CassFuturePtr(cass_session_execute(session, statement.get()));
    test_utils::wait_and_check_error(future.get());
    test_utils::CassResultPtr result(cass_future_get_result(future.get()));
    BOOST_REQUIRE_EQUAL(cass_result_row_count(result.get()), 1);
    BOOST_REQUIRE_EQUAL(cass_result_column_count(result.get()), 1);
    const CassRow* row = cass_result_first_row(result.get());
    const CassValue* value = cass_row_get_column(row, 0);
    BOOST_REQUIRE_EQUAL(cass_value_type(value), CASS_VALUE_TYPE_TUPLE);
    BOOST_REQUIRE_EQUAL(cass_value_primary_sub_type(value), CASS_VALUE_TYPE_UNKNOWN);
    BOOST_REQUIRE_EQUAL(cass_value_secondary_sub_type(value), CASS_VALUE_TYPE_UNKNOWN);
    test_utils::CassIteratorPtr tuple_iterator(cass_iterator_from_tuple(value));
    BOOST_REQUIRE_EQUAL(cass_value_item_count(value), size);
    int count = 0;
    while (cass_iterator_next(tuple_iterator.get())) {
      ++count;
      const CassValue* abstract_collection_value = cass_iterator_get_value(tuple_iterator.get());

      // Handle collection subtypes (and nested tuples)
      if (cass_value_is_collection(abstract_collection_value)) {
        BOOST_REQUIRE_EQUAL(cass_value_type(abstract_collection_value), collection_type);
        BOOST_REQUIRE_EQUAL(cass_value_primary_sub_type(abstract_collection_value), tuple_type);
        BOOST_REQUIRE_EQUAL(cass_value_secondary_sub_type(abstract_collection_value), collection_type == CASS_VALUE_TYPE_MAP ? tuple_type : CASS_VALUE_TYPE_UNKNOWN);
        BOOST_REQUIRE_EQUAL(cass_value_item_count(abstract_collection_value), size);

        test_utils::CassIteratorPtr subtype_iterator(cass_iterator_from_collection(abstract_collection_value));
        int subtype_count = 0;
        while (cass_iterator_next(subtype_iterator.get())) {
          ++subtype_count;
          BOOST_REQUIRE(cass_value_is_null(cass_iterator_get_value(subtype_iterator.get())));
          T collection_value;
          BOOST_REQUIRE_EQUAL(test_utils::Value<T>::get(cass_iterator_get_value(subtype_iterator.get()), &collection_value), CASS_ERROR_LIB_NULL_VALUE);
          if (collection_type == CASS_VALUE_TYPE_MAP) {
            BOOST_REQUIRE(cass_iterator_next(subtype_iterator.get()) == cass_true);
            BOOST_REQUIRE(cass_value_is_null(cass_iterator_get_value(subtype_iterator.get())));
            T collection_value;
            BOOST_REQUIRE_EQUAL(test_utils::Value<T>::get(cass_iterator_get_value(subtype_iterator.get()), &collection_value), CASS_ERROR_LIB_NULL_VALUE);
          }
        }
        BOOST_REQUIRE_EQUAL(subtype_count, size);
      } else {
        BOOST_REQUIRE(cass_value_is_null(abstract_collection_value));
        T collection_value;
        BOOST_REQUIRE_EQUAL(test_utils::Value<T>::get(abstract_collection_value, &collection_value), CASS_ERROR_LIB_NULL_VALUE);
      }
    }
    BOOST_REQUIRE_EQUAL(count, size);
  }
};

const unsigned int TupleTests::sizes_[] = { 1, 2, 3, 5, 37, 73, 74, 877 };
//TODO: Determine why nested collections are not working properly (values are inserted as seen via cqlsh)
//const CassValueType TupleTests::nested_collection_types_[] = { CASS_VALUE_TYPE_UNKNOWN, CASS_VALUE_TYPE_LIST, CASS_VALUE_TYPE_MAP, CASS_VALUE_TYPE_SET, CASS_VALUE_TYPE_TUPLE };
const CassValueType TupleTests::nested_collection_types_[] = { CASS_VALUE_TYPE_UNKNOWN };

BOOST_AUTO_TEST_SUITE(tuples)

/**
 * Read/Write Tuple
 *
 * This test ensures tuple values can be read/written using Cassandra v2.1+
 *
 * @since 2.1.0-beta
 * @jira_ticket CPP-262
 * @test_category data_types:tuples
 * @cassandra_version 2.1.x
 */
BOOST_AUTO_TEST_CASE(read_write) {
  CCM::CassVersion version = test_utils::get_version();
  if ((version.major_version >= 2 && version.minor_version >= 1) || version.major_version >= 3) {
    TupleTests tester;
    std::string create_table = "CREATE TABLE tuple_read_write(key int PRIMARY KEY, value frozen<tuple<int, text, float>>)";
    std::string insert_query = "INSERT INTO tuple_read_write(key, value) VALUES (?, ?)";
    std::string select_query = "SELECT value FROM tuple_read_write WHERE key=?";

    // Create the table for the test
    test_utils::execute_query(tester.session, create_table.c_str());

    // Full tuples
    {
      // Create one hundred simple read/write tests
      for (unsigned int size = 1; size <= 1; ++size) {
        // Insert the values into the tuple collection
        test_utils::CassTuplePtr tuple(cass_tuple_new(3));
        test_utils::Value<cass_int32_t>::tuple_set(tuple.get(), 0, size * 10);
        std::string random_string = test_utils::generate_random_string();
        CassString tuple_string = CassString(random_string.c_str());
        test_utils::Value<CassString>::tuple_set(tuple.get(), 1, tuple_string);
        test_utils::Value<cass_float_t>::tuple_set(tuple.get(), 2, static_cast<cass_float_t>(size * 100.0f));

        // Bind and insert the tuple collection into Cassandra
        test_utils::CassStatementPtr statement(cass_statement_new(insert_query.c_str(), 2));
        // Alternate bound parameters (simple) and prepared statement use
        if (size % 2 == 0) {
          test_utils::CassPreparedPtr prepared = test_utils::prepare(tester.session, insert_query.c_str());
          statement = test_utils::CassStatementPtr(cass_prepared_bind(prepared.get()));
        }
        BOOST_REQUIRE_EQUAL(cass_statement_bind_int32(statement.get(), 0, size), CASS_OK);
        BOOST_REQUIRE_EQUAL(cass_statement_bind_tuple(statement.get(), 1, tuple.get()), CASS_OK);
        test_utils::wait_and_check_error(test_utils::CassFuturePtr(cass_session_execute(tester.session, statement.get())).get());

        // Ensure the tuple collection can be read
        statement = test_utils::CassStatementPtr(cass_statement_new(select_query.c_str(), 1));
        // Alternate bound parameters (simple) and prepared statement use
        if (size % 2 == 0) {
          test_utils::CassPreparedPtr prepared = test_utils::prepare(tester.session, select_query.c_str());
          statement = test_utils::CassStatementPtr(cass_prepared_bind(prepared.get()));
        }
        BOOST_REQUIRE_EQUAL(cass_statement_bind_int32(statement.get(), 0, size), CASS_OK);
        test_utils::CassFuturePtr future = test_utils::CassFuturePtr(cass_session_execute(tester.session, statement.get()));
        test_utils::wait_and_check_error(future.get());
        test_utils::CassResultPtr result(cass_future_get_result(future.get()));
        BOOST_REQUIRE_EQUAL(cass_result_row_count(result.get()), 1);
        BOOST_REQUIRE_EQUAL(cass_result_column_count(result.get()), 1);
        const CassRow* row = cass_result_first_row(result.get());
        const CassValue* value = cass_row_get_column(row, 0);
        BOOST_REQUIRE_EQUAL(cass_value_type(value), CASS_VALUE_TYPE_TUPLE);
        BOOST_REQUIRE_EQUAL(cass_value_primary_sub_type(value), CASS_VALUE_TYPE_UNKNOWN);
        BOOST_REQUIRE_EQUAL(cass_value_secondary_sub_type(value), CASS_VALUE_TYPE_UNKNOWN);
        test_utils::CassIteratorPtr iterator(cass_iterator_from_tuple(value));
        cass_iterator_next(iterator.get());
        cass_int32_t value_integer;
        BOOST_REQUIRE_EQUAL(test_utils::Value<cass_int32_t>::get(cass_iterator_get_value(iterator.get()), &value_integer), CASS_OK);
        BOOST_REQUIRE(test_utils::Value<cass_int32_t>::equal(value_integer, size * 10));
        cass_iterator_next(iterator.get());
        CassString value_string;
        BOOST_REQUIRE_EQUAL(test_utils::Value<CassString>::get(cass_iterator_get_value(iterator.get()), &value_string), CASS_OK);
        BOOST_REQUIRE(test_utils::Value<CassString>::equal(value_string, tuple_string));
        cass_iterator_next(iterator.get());
        cass_float_t value_float;
        BOOST_REQUIRE_EQUAL(test_utils::Value<cass_float_t>::get(cass_iterator_get_value(iterator.get()), &value_float), CASS_OK);
        BOOST_REQUIRE(test_utils::Value<cass_float_t>::equal(value_float, static_cast<cass_float_t>(size * 100.0f)));
      }
    }

    // Partial tuple
    {
      test_utils::CassTuplePtr tuple(cass_tuple_new(3));
      test_utils::Value<cass_int32_t>::tuple_set(tuple.get(), 0, 123);
      test_utils::Value<CassString>::tuple_set(tuple.get(), 1, CassString("foo"));

      // Bind and insert the tuple collection into Cassandra
      test_utils::CassStatementPtr statement(cass_statement_new(insert_query.c_str(), 2));
      BOOST_REQUIRE_EQUAL(cass_statement_bind_int32(statement.get(), 0, 1), CASS_OK);
      BOOST_REQUIRE_EQUAL(cass_statement_bind_tuple(statement.get(), 1, tuple.get()), CASS_OK);
      test_utils::wait_and_check_error(test_utils::CassFuturePtr(cass_session_execute(tester.session, statement.get())).get());

      // Ensure the tuple collection can be read
      statement = test_utils::CassStatementPtr(cass_statement_new(select_query.c_str(), 1));
      BOOST_REQUIRE_EQUAL(cass_statement_bind_int32(statement.get(), 0, 1), CASS_OK);
      test_utils::CassFuturePtr future = test_utils::CassFuturePtr(cass_session_execute(tester.session, statement.get()));
      test_utils::wait_and_check_error(future.get());
      test_utils::CassResultPtr result(cass_future_get_result(future.get()));
      BOOST_REQUIRE_EQUAL(cass_result_row_count(result.get()), 1);
      BOOST_REQUIRE_EQUAL(cass_result_column_count(result.get()), 1);
      const CassRow* row = cass_result_first_row(result.get());
      const CassValue* value = cass_row_get_column(row, 0);
      BOOST_REQUIRE_EQUAL(cass_value_type(value), CASS_VALUE_TYPE_TUPLE);
      BOOST_REQUIRE_EQUAL(cass_value_primary_sub_type(value), CASS_VALUE_TYPE_UNKNOWN);
      BOOST_REQUIRE_EQUAL(cass_value_secondary_sub_type(value), CASS_VALUE_TYPE_UNKNOWN);
      test_utils::CassIteratorPtr iterator(cass_iterator_from_tuple(value));
      cass_iterator_next(iterator.get());
      cass_int32_t value_integer;
      BOOST_REQUIRE_EQUAL(test_utils::Value<cass_int32_t>::get(cass_iterator_get_value(iterator.get()), &value_integer), CASS_OK);
      BOOST_REQUIRE(test_utils::Value<cass_int32_t>::equal(value_integer, 123));
      cass_iterator_next(iterator.get());
      CassString value_string;
      BOOST_REQUIRE_EQUAL(test_utils::Value<CassString>::get(cass_iterator_get_value(iterator.get()), &value_string), CASS_OK);
      BOOST_REQUIRE(test_utils::Value<CassString>::equal(value_string, "foo"));
      cass_iterator_next(iterator.get());
      BOOST_REQUIRE(cass_value_is_null(cass_iterator_get_value(iterator.get())));
      cass_float_t value_float;
      BOOST_REQUIRE_EQUAL(test_utils::Value<cass_float_t>::get(cass_iterator_get_value(iterator.get()), &value_float), CASS_ERROR_LIB_NULL_VALUE);
    }
  } else {
    std::cout << "Unsupported Test for Cassandra v" << version.to_string() << ": Skipping tuples/read_write" << std::endl;
    BOOST_REQUIRE(true);
  }
}

/**
 * Tuples of varying size
 *
 * This test ensures tuples can be read/written using Cassandra v2.1+ with
 * varying sizes using primitives and collections (including tuples).
 *
 * @since 2.1.0-beta
 * @jira_ticket CPP-262
 * @test_category data_types:tuples
 * @cassandra_version 2.1.x
 */
BOOST_AUTO_TEST_CASE(varying_size) {
  CCM::CassVersion version = test_utils::get_version();
  if ((version.major_version >= 2 && version.minor_version >= 1) || version.major_version >= 3) {
    TupleTests tester;

    // Create some varying size tuple tests (primitives)
    for (unsigned int i = 0; i < (sizeof(tester.sizes_) / sizeof(int)); ++i) {
      unsigned int size = tester.sizes_[i];
      for (unsigned int j = 0; j < (sizeof(tester.nested_collection_types_) / sizeof(CassValueType)); ++j) {
        CassValueType nested_collection_type = tester.nested_collection_types_[j];
        if (nested_collection_type == CASS_VALUE_TYPE_UNKNOWN || size <= NESTED_COLLECTION_SIZE_LIMITER) {
          {
            CassString value("Test Value");
            tester.insert_varying_sized_value<CassString>(CASS_VALUE_TYPE_ASCII, value, size, nested_collection_type);
            tester.insert_varying_sized_value<CassString>(CASS_VALUE_TYPE_VARCHAR, value, size, nested_collection_type); // NOTE: text is alias for varchar
          }

          {
            cass_int64_t value = 1234567890;
            tester.insert_varying_sized_value<cass_int64_t>(CASS_VALUE_TYPE_BIGINT, value, size, nested_collection_type);
            tester.insert_varying_sized_value<cass_int64_t>(CASS_VALUE_TYPE_TIMESTAMP, value, size, nested_collection_type);
          }

          {
            CassBytes value = test_utils::bytes_from_string("012345678900123456789001234567890012345678900123456789001234567890");
            tester.insert_varying_sized_value<CassBytes>(CASS_VALUE_TYPE_BLOB, value, size, nested_collection_type);
            tester.insert_varying_sized_value<CassBytes>(CASS_VALUE_TYPE_VARINT, value, size, nested_collection_type);
          }

          tester.insert_varying_sized_value<cass_bool_t>(CASS_VALUE_TYPE_BOOLEAN, cass_true, size, nested_collection_type);

          {
            const cass_uint8_t pi[] = { 57, 115, 235, 135, 229, 215, 8, 125, 13, 43, 1, 25, 32, 135, 129, 180,
              112, 176, 158, 120, 246, 235, 29, 145, 238, 50, 108, 239, 219, 100, 250,
              84, 6, 186, 148, 76, 230, 46, 181, 89, 239, 247 };
            const cass_int32_t pi_scale = 100;
            CassDecimal value = CassDecimal(pi, sizeof(pi), pi_scale);
            tester.insert_varying_sized_value<CassDecimal>(CASS_VALUE_TYPE_DECIMAL, value, size, nested_collection_type);
          }

          if ((version.major_version >= 3 && version.minor_version >= 10) || version.major_version >= 4) {
            CassDuration value = CassDuration(1, 2, 3);
            tester.insert_varying_sized_value<CassDuration>(CASS_VALUE_TYPE_DURATION, value, size, nested_collection_type);
          }

          tester.insert_varying_sized_value<cass_double_t>(CASS_VALUE_TYPE_DOUBLE, 3.141592653589793, size, nested_collection_type);
          tester.insert_varying_sized_value<cass_float_t>(CASS_VALUE_TYPE_FLOAT, 3.1415926f, size, nested_collection_type);
          tester.insert_varying_sized_value<cass_int32_t>(CASS_VALUE_TYPE_INT, 123, size, nested_collection_type);
          if ((version.major_version >= 2 && version.minor_version >= 2) || version.major_version >= 3) {
            tester.insert_varying_sized_value<cass_int16_t>(CASS_VALUE_TYPE_SMALL_INT, 123, size, nested_collection_type);
            tester.insert_varying_sized_value<cass_int8_t>(CASS_VALUE_TYPE_TINY_INT, 123, size, nested_collection_type);
            tester.insert_varying_sized_value<CassDate>(CASS_VALUE_TYPE_DATE,
                                                                    test_utils::Value<CassDate>::min_value() + 1u,
                                                                    size, nested_collection_type);
            tester.insert_varying_sized_value<CassTime>(CASS_VALUE_TYPE_TIME, 123, size, nested_collection_type);
          }

          {
            CassUuid value = test_utils::generate_random_uuid(tester.uuid_gen);
            tester.insert_varying_sized_value<CassUuid>(CASS_VALUE_TYPE_UUID, value, size, nested_collection_type);
          }

          {
            CassInet value = test_utils::inet_v4_from_int(16777343); // 127.0.0.1
            tester.insert_varying_sized_value<CassInet>(CASS_VALUE_TYPE_INET, value, size, nested_collection_type);
          }

          {
            CassUuid value = test_utils::generate_time_uuid(tester.uuid_gen);
            tester.insert_varying_sized_value<CassUuid>(CASS_VALUE_TYPE_TIMEUUID, value, size, nested_collection_type);
          }
        }
      }
    }
  } else {
    std::cout << "Unsupported Test for Cassandra v" << version.to_string() << ": Skipping tuples/varying_size" << std::endl;
    BOOST_REQUIRE(true);
  }
}

/**
* Null Tuples
*
* This test ensures tuples can be read/written using Cassandra v2.1+ of varying
* sizes with null values.
*
* @since 2.1.0-beta
* @jira_ticket CPP-262
* @test_category data_types:tuples
* @cassandra_version 2.1.x
*/
BOOST_AUTO_TEST_CASE(null) {
  CCM::CassVersion version = test_utils::get_version();
  if ((version.major_version >= 2 && version.minor_version >= 1) || version.major_version >= 3) {
    TupleTests tester;

    // Create some varying size null tuple tests
    for (unsigned int i = 0; i < (sizeof(tester.sizes_) / sizeof(int)); ++i) {
      unsigned int size = tester.sizes_[i];
      for (unsigned int j = 0; j < (sizeof(tester.nested_collection_types_) / sizeof(CassValueType)); ++j) {
        CassValueType nested_collection_type = tester.nested_collection_types_[j];
        tester.insert_varying_sized_null_value<CassString>(CASS_VALUE_TYPE_ASCII, size, nested_collection_type);
        tester.insert_varying_sized_null_value<CassString>(CASS_VALUE_TYPE_VARCHAR, size, nested_collection_type); // NOTE: text is alias for varchar
        tester.insert_varying_sized_null_value<cass_int64_t>(CASS_VALUE_TYPE_BIGINT, size, nested_collection_type);
        tester.insert_varying_sized_null_value<cass_int64_t>(CASS_VALUE_TYPE_TIMESTAMP, size, nested_collection_type);
        tester.insert_varying_sized_null_value<CassBytes>(CASS_VALUE_TYPE_BLOB, size, nested_collection_type);
        tester.insert_varying_sized_null_value<CassBytes>(CASS_VALUE_TYPE_VARINT, size, nested_collection_type);
        tester.insert_varying_sized_null_value<cass_bool_t>(CASS_VALUE_TYPE_BOOLEAN, size, nested_collection_type);
        tester.insert_varying_sized_null_value<CassDecimal>(CASS_VALUE_TYPE_DECIMAL, size, nested_collection_type);
        tester.insert_varying_sized_null_value<cass_double_t>(CASS_VALUE_TYPE_DOUBLE, size, nested_collection_type);
        tester.insert_varying_sized_null_value<cass_float_t>(CASS_VALUE_TYPE_FLOAT, size, nested_collection_type);
        tester.insert_varying_sized_null_value<cass_int32_t>(CASS_VALUE_TYPE_INT, size, nested_collection_type);
        tester.insert_varying_sized_null_value<CassUuid>(CASS_VALUE_TYPE_UUID, size, nested_collection_type);
        tester.insert_varying_sized_null_value<CassInet>(CASS_VALUE_TYPE_INET, size, nested_collection_type);
        tester.insert_varying_sized_null_value<CassUuid>(CASS_VALUE_TYPE_TIMEUUID, size, nested_collection_type);
        if ((version.major_version >= 2 && version.minor_version >= 2) || version.major_version >= 3) {
          tester.insert_varying_sized_null_value<cass_int8_t>(CASS_VALUE_TYPE_TINY_INT, size, nested_collection_type);
          tester.insert_varying_sized_null_value<cass_int16_t>(CASS_VALUE_TYPE_SMALL_INT, size, nested_collection_type);
          tester.insert_varying_sized_null_value<CassDate>(CASS_VALUE_TYPE_DATE, size, nested_collection_type);
          tester.insert_varying_sized_null_value<CassTime>(CASS_VALUE_TYPE_TIME, size, nested_collection_type);
        }
        if ((version.major_version >= 3 && version.minor_version >= 10) || version.major_version >= 4) {
          tester.insert_varying_sized_null_value<CassDuration>(CASS_VALUE_TYPE_DURATION, size, nested_collection_type);
        }
      }
    }
  } else {
    std::cout << "Unsupported Test for Cassandra v" << version.to_string() << ": Skipping tuples/null" << std::endl;
    BOOST_REQUIRE(true);
  }
}

/**
* Invalid Tuple
*
* This test ensures invalid tuples cannot be written to Cassandra
*
* @since 2.1.0-beta
* @jira_ticket CPP-262
* @test_category data_types:tuples
* @cassandra_version 2.1.x
*/
BOOST_AUTO_TEST_CASE(invalid) {
  CCM::CassVersion version = test_utils::get_version();
  if ((version.major_version >= 2 && version.minor_version >= 1) || version.major_version >= 3) {
    TupleTests tester;
    std::string create_table = "CREATE TABLE tuple_invalid(key int PRIMARY KEY, value frozen<tuple<int, text, float>>)";
    std::string insert_query = "INSERT INTO tuple_invalid(key, value) VALUES (?, ?)";
    std::string select_query = "SELECT value FROM tuple_invalid WHERE key=?";

    // Create the table for the test
    test_utils::execute_query(tester.session, create_table.c_str());

    // Exceeding size tuple
    {
      test_utils::CassTuplePtr tuple(cass_tuple_new(5));
      test_utils::Value<cass_int32_t>::tuple_set(tuple.get(), 0, 123);
      test_utils::Value<CassString>::tuple_set(tuple.get(), 1, CassString("foo"));
      test_utils::Value<cass_float_t>::tuple_set(tuple.get(), 2, 3.1415926f);
      test_utils::Value<cass_int32_t>::tuple_set(tuple.get(), 3, 456);
      test_utils::Value<CassString>::tuple_set(tuple.get(), 4, CassString("bar"));

      // Bind and insert the tuple collection into Cassandra
      test_utils::CassStatementPtr statement(cass_statement_new(insert_query.c_str(), 2));
      BOOST_REQUIRE_EQUAL(cass_statement_bind_int32(statement.get(), 0, 1), CASS_OK);
      BOOST_REQUIRE_EQUAL(cass_statement_bind_tuple(statement.get(), 1, tuple.get()), CASS_OK);
      BOOST_REQUIRE_EQUAL(test_utils::wait_and_return_error(test_utils::CassFuturePtr(cass_session_execute(tester.session, statement.get())).get()),
                          CASS_ERROR_SERVER_INVALID_QUERY);
    }

    // Invalid type in tuple
    {
      test_utils::CassTuplePtr tuple(cass_tuple_new(3));
      test_utils::Value<CassString>::tuple_set(tuple.get(), 0, CassString("foo"));
      test_utils::Value<cass_int32_t>::tuple_set(tuple.get(), 1, 123);
      test_utils::Value<cass_float_t>::tuple_set(tuple.get(), 2, 3.1415926f);

      // Bind and insert the tuple collection into Cassandra
      test_utils::CassStatementPtr statement(cass_statement_new(insert_query.c_str(), 2));
      BOOST_REQUIRE_EQUAL(cass_statement_bind_int32(statement.get(), 0, 1), CASS_OK);
      BOOST_REQUIRE_EQUAL(cass_statement_bind_tuple(statement.get(), 1, tuple.get()), CASS_OK);
      BOOST_REQUIRE_EQUAL(test_utils::wait_and_return_error(test_utils::CassFuturePtr(cass_session_execute(tester.session, statement.get())).get()),
                          CASS_ERROR_SERVER_INVALID_QUERY);
    }
  } else {
    std::cout << "Unsupported Test for Cassandra v" << version.to_string() << ": Skipping tuples/invalid" << std::endl;
    BOOST_REQUIRE(true);
  }
}

BOOST_AUTO_TEST_SUITE_END()
