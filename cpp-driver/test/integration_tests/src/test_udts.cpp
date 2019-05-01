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
#include "testing.hpp"

#include <boost/format.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/thread.hpp>

#include <algorithm>
#include <map>

typedef std::map<CassString, CassString> PhoneMap;
typedef std::pair<CassString, CassString> PhonePair;

#define PHONE_UDT_CQL "CREATE TYPE phone (alias text, number text)"
#define ADDRESS_UDT_CQL \
  "CREATE TYPE address (street text, \"ZIP\" int, phone_numbers set<frozen<phone>>)"

struct UDTTests : public test_utils::SingleSessionTest {
private:
  /**
   * Session schema metadata
   */
  test_utils::CassSchemaMetaPtr schema_meta_;

  /**
   * Test keyspace
   */
  std::string keyspace_;

  /**
   * Update the session schema metadata
   */
  void update_schema() {
    schema_meta_ = test_utils::CassSchemaMetaPtr(cass_session_get_schema_meta(session));
  }

  /**
   * Verify the user data type exists (updating schema if needed up to 10 times)
   *
   * @param udt_name Name of the UDT to verify
   */
  void verify_user_type(const std::string& udt_name) {
    std::vector<std::string> udt_field_names;
    unsigned int count = 0;
    const CassDataType* datatype = NULL;
    while (udt_field_names.empty() && ++count <= 10) {
      update_schema();
      const CassKeyspaceMeta* keyspace_meta =
          cass_schema_meta_keyspace_by_name(schema_meta_.get(), keyspace_.c_str());
      BOOST_REQUIRE(keyspace_meta != NULL);
      datatype = cass_keyspace_meta_user_type_by_name(keyspace_meta, udt_name.c_str());
      if (datatype == NULL) {
        boost::this_thread::sleep_for(boost::chrono::milliseconds(100));
      }
    }
    BOOST_REQUIRE(datatype != NULL);
  }

public:
  UDTTests()
      : test_utils::SingleSessionTest(1, 0)
      , schema_meta_(NULL)
      , keyspace_(str(boost::format("ks_%s") % test_utils::generate_unique_str(uuid_gen))) {
    test_utils::execute_query(
        session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT) % keyspace_ % "1"));
    test_utils::execute_query(session, str(boost::format("USE %s") % keyspace_));
  }

  ~UDTTests() {
    // Drop the keyspace (ignore any and all errors)
    test_utils::execute_query_with_error(
        session, str(boost::format(test_utils::DROP_KEYSPACE_FORMAT) % keyspace_));
  }

  /**
   * Create the common UDTs used for the UDT tests
   */
  void create_udts() {
    test_utils::execute_query(session, PHONE_UDT_CQL);
    verify_user_type("phone");
    test_utils::execute_query(session, ADDRESS_UDT_CQL);
    verify_user_type("address");
  }

  /**
   * Create a new UDT from schema
   *
   * @param udt_name UDT name to create from schema
   * @return UDT
   */
  test_utils::CassUserTypePtr new_udt(const std::string& udt_name) {
    verify_user_type(udt_name);
    const CassKeyspaceMeta* keyspace_meta =
        cass_schema_meta_keyspace_by_name(schema_meta_.get(), keyspace_.c_str());
    BOOST_REQUIRE(keyspace_meta != NULL);
    const CassDataType* datatype =
        cass_keyspace_meta_user_type_by_name(keyspace_meta, udt_name.c_str());
    BOOST_REQUIRE(datatype != NULL);
    return test_utils::CassUserTypePtr(cass_user_type_new_from_data_type(datatype));
  }

  /**
   * Create a new phone UDT
   *
   * @return Phone UDT
   */
  test_utils::CassUserTypePtr new_phone_udt() { return new_udt("phone"); }

  /**
   * Create a new address UDT
   *
   * @return Address UDT
   */
  test_utils::CassUserTypePtr new_address_udt() { return new_udt("address"); }

  /**
   * Verify the phone UDT field names
   *
   * @param value The phone UDT value to iterate over
   */
  void verify_phone_udt_field_names(const CassValue* value) {
    // Ensure the value is a UDT and create the iterator for the validation
    BOOST_REQUIRE_EQUAL(cass_value_type(value), CASS_VALUE_TYPE_UDT);
    BOOST_REQUIRE_EQUAL(cass_value_item_count(value), 2);
    test_utils::CassIteratorPtr iterator(cass_iterator_fields_from_user_type(value));

    // Verify alias field name
    BOOST_REQUIRE(cass_iterator_next(iterator.get()));
    CassString alias_field_name;
    BOOST_REQUIRE_EQUAL(cass_iterator_get_user_type_field_name(
                            iterator.get(), &alias_field_name.data, &alias_field_name.length),
                        CASS_OK);
    BOOST_REQUIRE(test_utils::Value<CassString>::equal("alias", alias_field_name));

    // Verify number field name
    BOOST_REQUIRE(cass_iterator_next(iterator.get()));
    CassString number_field_name;
    BOOST_REQUIRE_EQUAL(cass_iterator_get_user_type_field_name(
                            iterator.get(), &number_field_name.data, &number_field_name.length),
                        CASS_OK);
    BOOST_REQUIRE(test_utils::Value<CassString>::equal("number", number_field_name));
  }

  /**
   * Verify the phone UDT (field names and results)
   *
   * @param value The phone UDT value to iterate over
   * @param expected_alias Expected alias to verify against result
   * @param expected_number Expected number to verify against result
   */
  void verify_phone_udt(const CassValue* value, CassString expected_alias,
                        CassString expected_number) {
    // Verify field names for phone UDT and create the iterator for validation
    verify_phone_udt_field_names(value);
    test_utils::CassIteratorPtr iterator(cass_iterator_fields_from_user_type(value));

    // Verify alias result
    BOOST_REQUIRE(cass_iterator_next(iterator.get()));
    const CassValue* alias_value = cass_iterator_get_user_type_field_value(iterator.get());
    BOOST_REQUIRE_EQUAL(cass_value_type(alias_value), CASS_VALUE_TYPE_VARCHAR);
    CassString alias_result;
    BOOST_REQUIRE_EQUAL(
        cass_value_get_string(alias_value, &alias_result.data, &alias_result.length), CASS_OK);
    BOOST_REQUIRE(test_utils::Value<CassString>::equal(alias_result, expected_alias));

    // Verify number result
    BOOST_REQUIRE(cass_iterator_next(iterator.get()));
    const CassValue* number_value = cass_iterator_get_user_type_field_value(iterator.get());
    BOOST_REQUIRE_EQUAL(cass_value_type(number_value), CASS_VALUE_TYPE_VARCHAR);
    CassString number_result;
    BOOST_REQUIRE_EQUAL(
        cass_value_get_string(number_value, &number_result.data, &number_result.length), CASS_OK);
    BOOST_REQUIRE(test_utils::Value<CassString>::equal(number_result, expected_number));
  }

  /**
   * Verify the address UDT field names
   *
   * @param value The address UDT value to iterate over
   */
  void verify_address_udt_field_names(const CassValue* value) {
    // Ensure the value is a UDT and create the iterator for the validation
    BOOST_REQUIRE_EQUAL(cass_value_type(value), CASS_VALUE_TYPE_UDT);
    BOOST_REQUIRE_EQUAL(cass_value_item_count(value), 3);
    test_utils::CassIteratorPtr iterator(cass_iterator_fields_from_user_type(value));

    // Verify street field name
    BOOST_REQUIRE(cass_iterator_next(iterator.get()));
    CassString street_field_name;
    BOOST_REQUIRE_EQUAL(cass_iterator_get_user_type_field_name(
                            iterator.get(), &street_field_name.data, &street_field_name.length),
                        CASS_OK);
    BOOST_REQUIRE(test_utils::Value<CassString>::equal("street", street_field_name));

    // Verify zip field name
    BOOST_REQUIRE(cass_iterator_next(iterator.get()));
    CassString zip_field_name;
    BOOST_REQUIRE_EQUAL(cass_iterator_get_user_type_field_name(iterator.get(), &zip_field_name.data,
                                                               &zip_field_name.length),
                        CASS_OK);
    BOOST_REQUIRE(test_utils::Value<CassString>::equal("ZIP", zip_field_name));

    // Verify phone numbers field name
    BOOST_REQUIRE(cass_iterator_next(iterator.get()));
    CassString phone_numbers_field_name;
    BOOST_REQUIRE_EQUAL(cass_iterator_get_user_type_field_name(iterator.get(),
                                                               &phone_numbers_field_name.data,
                                                               &phone_numbers_field_name.length),
                        CASS_OK);
    BOOST_REQUIRE(test_utils::Value<CassString>::equal("phone_numbers", phone_numbers_field_name));
  }

  /**
   * Verify the address UDT (field names and results)
   *
   * @param value The address UDT value to iterate over
   * @param expected_street Expected street address to verify against result
   * @param expected_zip Expected zip code to verify against result
   * @param expected_phone_numbers Expected numbers to verify against result
   */
  void verify_address_udt(const CassValue* value, CassString expected_street,
                          cass_int32_t expected_zip, PhoneMap expected_phone_numbers) {
    // Verify field names for address UDT and create the iterator for validation
    verify_address_udt_field_names(value);
    test_utils::CassIteratorPtr iterator(cass_iterator_fields_from_user_type(value));

    // Verify street result
    BOOST_REQUIRE(cass_iterator_next(iterator.get()));
    const CassValue* street_value = cass_iterator_get_user_type_field_value(iterator.get());
    BOOST_REQUIRE_EQUAL(cass_value_type(street_value), CASS_VALUE_TYPE_VARCHAR);
    CassString street_result;
    BOOST_REQUIRE_EQUAL(
        cass_value_get_string(street_value, &street_result.data, &street_result.length), CASS_OK);
    BOOST_REQUIRE(test_utils::Value<CassString>::equal(street_result, expected_street));

    // Verify zip result
    BOOST_REQUIRE(cass_iterator_next(iterator.get()));
    const CassValue* zip_value = cass_iterator_get_user_type_field_value(iterator.get());
    BOOST_REQUIRE_EQUAL(cass_value_type(zip_value), CASS_VALUE_TYPE_INT);
    cass_int32_t zip_result;
    BOOST_REQUIRE_EQUAL(cass_value_get_int32(zip_value, &zip_result), CASS_OK);
    BOOST_REQUIRE(test_utils::Value<cass_int32_t>::equal(zip_result, expected_zip));

    // Verify phone numbers result
    BOOST_REQUIRE(cass_iterator_next(iterator.get()));
    const CassValue* phone_numbers_value = cass_iterator_get_user_type_field_value(iterator.get());
    BOOST_REQUIRE_EQUAL(cass_value_type(phone_numbers_value), CASS_VALUE_TYPE_SET);
    BOOST_REQUIRE_EQUAL(cass_value_item_count(phone_numbers_value), expected_phone_numbers.size());
    test_utils::CassIteratorPtr phone_numbers_iterator(
        cass_iterator_from_collection(phone_numbers_value));
    unsigned int count = 0;
    PhoneMap::iterator phone_iterator = expected_phone_numbers.begin();
    while (cass_iterator_next(phone_numbers_iterator.get()) &&
           phone_iterator != expected_phone_numbers.end()) {
      verify_phone_udt(cass_iterator_get_value(phone_numbers_iterator.get()), phone_iterator->first,
                       phone_iterator->second);
      ++phone_iterator;
      ++count;
    }
    BOOST_REQUIRE_EQUAL(expected_phone_numbers.size(), count);
  }
};

BOOST_AUTO_TEST_SUITE(udts)

/**
 * Read/Write User Defined Type (UDT)
 *
 * This test ensures UDTs can be read/written using Cassandra v2.1+
 *
 * @since 2.1.0-beta
 * @jira_ticket CPP-96
 * @test_category data_types:udt
 * @cassandra_version 2.1.x
 */
BOOST_AUTO_TEST_CASE(read_write) {
  CCM::CassVersion version = test_utils::get_version();
  if ((version.major_version >= 2 && version.minor_version >= 1) || version.major_version >= 3) {
    UDTTests tester;
    std::string create_table = "CREATE TABLE user (id uuid PRIMARY KEY, addr frozen<address>)";
    std::string insert_query = "INSERT INTO user(id, addr) VALUES (?, ?)";
    std::string select_query = "SELECT addr FROM user WHERE id=?";

    // Create the UDTs and table for the test
    tester.create_udts();
    test_utils::execute_query(tester.session, create_table.c_str());

    // Full UDT
    {
      // Phone numbers UDT
      PhoneMap phone_numbers;
      CassString home_phone_alias("Home");
      CassString home_phone_number("555-911-1212");
      phone_numbers.insert(PhonePair(home_phone_alias, home_phone_number));
      test_utils::CassUserTypePtr home_phone(tester.new_phone_udt());
      BOOST_REQUIRE_EQUAL(
          test_utils::Value<CassString>::user_type_set(home_phone.get(), 0, home_phone_alias),
          CASS_OK);
      BOOST_REQUIRE_EQUAL(
          test_utils::Value<CassString>::user_type_set(home_phone.get(), 1, home_phone_number),
          CASS_OK);
      CassString work_phone_alias("Work");
      CassString work_phone_number("650-389-6000");
      phone_numbers.insert(PhonePair(work_phone_alias, work_phone_number));
      test_utils::CassUserTypePtr work_phone(tester.new_phone_udt());
      BOOST_REQUIRE_EQUAL(
          test_utils::Value<CassString>::user_type_set(work_phone.get(), 0, work_phone_alias),
          CASS_OK);
      BOOST_REQUIRE_EQUAL(
          test_utils::Value<CassString>::user_type_set(work_phone.get(), 1, work_phone_number),
          CASS_OK);

      // Create a collection for the phone numbers
      test_utils::CassCollectionPtr phone_numbers_set(
          cass_collection_new(CASS_COLLECTION_TYPE_SET, 2));
      BOOST_REQUIRE_EQUAL(
          cass_collection_append_user_type(phone_numbers_set.get(), home_phone.get()), CASS_OK);
      BOOST_REQUIRE_EQUAL(
          cass_collection_append_user_type(phone_numbers_set.get(), work_phone.get()), CASS_OK);

      // Address number UDT (nested UDT)
      CassString street_address("3975 Freedom Circle");
      cass_int32_t zip_code = 95054;
      test_utils::CassUserTypePtr address(tester.new_address_udt());
      BOOST_REQUIRE_EQUAL(
          test_utils::Value<CassString>::user_type_set(address.get(), 0, street_address), CASS_OK);
      BOOST_REQUIRE_EQUAL(
          test_utils::Value<cass_int32_t>::user_type_set(address.get(), 1, zip_code), CASS_OK);
      BOOST_REQUIRE_EQUAL(cass_user_type_set_collection(address.get(), 2, phone_numbers_set.get()),
                          CASS_OK);

      // Bind and insert the UDT into Cassandra
      CassUuid key = test_utils::generate_time_uuid(tester.uuid_gen);
      test_utils::CassStatementPtr statement(cass_statement_new(insert_query.c_str(), 2));
      BOOST_REQUIRE_EQUAL(cass_statement_bind_uuid(statement.get(), 0, key), CASS_OK);
      BOOST_REQUIRE_EQUAL(cass_statement_bind_user_type(statement.get(), 1, address.get()),
                          CASS_OK);
      test_utils::wait_and_check_error(
          test_utils::CassFuturePtr(cass_session_execute(tester.session, statement.get())).get());

      // Ensure the UDT can be read
      statement = test_utils::CassStatementPtr(cass_statement_new(select_query.c_str(), 1));
      BOOST_REQUIRE_EQUAL(cass_statement_bind_uuid(statement.get(), 0, key), CASS_OK);
      test_utils::CassFuturePtr future =
          test_utils::CassFuturePtr(cass_session_execute(tester.session, statement.get()));
      test_utils::wait_and_check_error(future.get());
      test_utils::CassResultPtr result(cass_future_get_result(future.get()));
      BOOST_REQUIRE_EQUAL(cass_result_row_count(result.get()), 1);
      BOOST_REQUIRE_EQUAL(cass_result_column_count(result.get()), 1);
      const CassRow* row = cass_result_first_row(result.get());
      const CassValue* value = cass_row_get_column(row, 0);
      tester.verify_address_udt(value, street_address, zip_code, phone_numbers);
    }

    // Partial UDT
    {
      // Street only address UDT (no nested UDT)
      CassString street_address("1 Furzeground Way");
      test_utils::CassUserTypePtr address(tester.new_address_udt());
      BOOST_REQUIRE_EQUAL(
          test_utils::Value<CassString>::user_type_set(address.get(), 0, street_address), CASS_OK);

      // Bind and insert the UDT into Cassandra
      CassUuid key = test_utils::generate_time_uuid(tester.uuid_gen);
      test_utils::CassStatementPtr statement(cass_statement_new(insert_query.c_str(), 2));
      BOOST_REQUIRE_EQUAL(cass_statement_bind_uuid(statement.get(), 0, key), CASS_OK);
      BOOST_REQUIRE_EQUAL(cass_statement_bind_user_type(statement.get(), 1, address.get()),
                          CASS_OK);
      test_utils::wait_and_check_error(
          test_utils::CassFuturePtr(cass_session_execute(tester.session, statement.get())).get());

      // Ensure the UDT can be read
      statement = test_utils::CassStatementPtr(cass_statement_new(select_query.c_str(), 1));
      BOOST_REQUIRE_EQUAL(cass_statement_bind_uuid(statement.get(), 0, key), CASS_OK);
      test_utils::CassFuturePtr future =
          test_utils::CassFuturePtr(cass_session_execute(tester.session, statement.get()));
      test_utils::wait_and_check_error(future.get());
      test_utils::CassResultPtr result(cass_future_get_result(future.get()));
      BOOST_REQUIRE_EQUAL(cass_result_row_count(result.get()), 1);
      BOOST_REQUIRE_EQUAL(cass_result_column_count(result.get()), 1);
      const CassRow* row = cass_result_first_row(result.get());
      const CassValue* value = cass_row_get_column(row, 0);
      BOOST_REQUIRE_EQUAL(cass_value_type(value), CASS_VALUE_TYPE_UDT);
      tester.verify_address_udt_field_names(value);
      test_utils::CassIteratorPtr iterator(cass_iterator_fields_from_user_type(value));
      // Verify street result
      BOOST_REQUIRE(cass_iterator_next(iterator.get()));
      const CassValue* street_value = cass_iterator_get_user_type_field_value(iterator.get());
      BOOST_REQUIRE_EQUAL(cass_value_type(street_value), CASS_VALUE_TYPE_VARCHAR);
      CassString street_result;
      BOOST_REQUIRE_EQUAL(
          cass_value_get_string(street_value, &street_result.data, &street_result.length), CASS_OK);
      BOOST_REQUIRE(test_utils::Value<CassString>::equal(street_result, street_address));
      // Verify zip result is NULL
      BOOST_REQUIRE(cass_iterator_next(iterator.get()));
      BOOST_REQUIRE(cass_value_is_null(cass_iterator_get_user_type_field_value(iterator.get())));
      // Verify phone numbers result is NULL
      BOOST_REQUIRE(cass_iterator_next(iterator.get()));
      BOOST_REQUIRE(cass_value_is_null(cass_iterator_get_user_type_field_value(iterator.get())));
    }
  } else {
    std::cout << "Unsupported Test for Cassandra v" << version.to_string()
              << ": Skipping udts/read_write" << std::endl;
    BOOST_REQUIRE(true);
  }
}

/**
 * Invalid User Defined Type (UDT) tests
 *
 * This test ensures invalid UDTs return errors using Cassandra v2.1+
 *
 * @since 2.1.0-beta
 * @jira_ticket CPP-96
 * @test_category data_types:udt
 * @cassandra_version 2.1.x
 */
BOOST_AUTO_TEST_CASE(invalid) {
  CCM::CassVersion version = test_utils::get_version();
  if ((version.major_version >= 2 && version.minor_version >= 1) || version.major_version >= 3) {
    UDTTests tester;
    std::string invalid_udt_missing_frozen_keyword =
        "CREATE TYPE invalid_udt (id uuid, address address)";
    std::string invalid_parent_udt = "CREATE TYPE invalid_udt (address frozen<address>)";
    std::string create_table =
        "CREATE TABLE invalid_udt_user (id uuid PRIMARY KEY, invalid frozen<invalid_udt>)";
    std::string insert_query = "INSERT INTO invalid_udt_user(id, invalid) VALUES (?, ?)";

    {
      if (version.major_version < 3) {
        // Ensure UDT cannot be created when missing frozen keyword
        BOOST_REQUIRE_EQUAL(test_utils::execute_query_with_error(
                                tester.session, invalid_udt_missing_frozen_keyword.c_str()),
                            CASS_ERROR_SERVER_INVALID_QUERY);
      }
      // Ensure UDT cannot be created when referencing non-existent UDT
      BOOST_REQUIRE_EQUAL(
          test_utils::execute_query_with_error(tester.session, invalid_parent_udt.c_str()),
          CASS_ERROR_SERVER_INVALID_QUERY);
    }

    // Create the UDTs and table for the test
    tester.create_udts();
    test_utils::execute_query(tester.session, invalid_parent_udt.c_str());
    test_utils::execute_query(tester.session, create_table.c_str());

    // Invalid UDT set
    {
      test_utils::CassUserTypePtr phone(tester.new_phone_udt());
      test_utils::CassUserTypePtr invalid_udt(tester.new_udt("invalid_udt"));
      BOOST_REQUIRE_EQUAL(cass_user_type_set_user_type(invalid_udt.get(), 0, phone.get()),
                          CASS_ERROR_LIB_INVALID_VALUE_TYPE);
    }

    // Invalid UDT bound to statement
    {
      test_utils::CassUserTypePtr phone(tester.new_phone_udt());
      CassUuid key = test_utils::generate_time_uuid(tester.uuid_gen);
      test_utils::CassStatementPtr statement(cass_statement_new(insert_query.c_str(), 2));
      BOOST_REQUIRE_EQUAL(cass_statement_bind_uuid(statement.get(), 0, key), CASS_OK);
      BOOST_REQUIRE_EQUAL(cass_statement_bind_user_type(statement.get(), 1, phone.get()), CASS_OK);
      CassError expected_error = CASS_ERROR_SERVER_INVALID_QUERY;
      if (version >= "3.11.0") {
        expected_error = CASS_ERROR_SERVER_SERVER_ERROR;
      }
      BOOST_REQUIRE_EQUAL(
          test_utils::wait_and_return_error(
              test_utils::CassFuturePtr(cass_session_execute(tester.session, statement.get()))
                  .get()),
          expected_error);
    }
  } else {
    std::cout << "Unsupported Test for Cassandra v" << version.to_string()
              << ": Skipping udts/invalid" << std::endl;
    BOOST_REQUIRE(true);
  }
}

/**
 * Ensure varchar/text datatypes are correctly handled with nested UDTs
 *
 * This test ensures the text datatypes are handled correctly using Cassandra
 * v2.1+ with nested UDTs
 *
 * @since 2.2.0-beta
 * @jira_ticket CPP-287
 * @test_category data_types:udt
 * @cassandra_version 2.1.x
 */
BOOST_AUTO_TEST_CASE(text_types) {
  CCM::CassVersion version = test_utils::get_version();
  if ((version.major_version >= 2 && version.minor_version >= 1) || version.major_version >= 3) {
    UDTTests tester;
    std::string nested_type = "CREATE TYPE nested_type (value_1 int, value_2 int)";
    std::string parent_type = "CREATE TYPE parent_type (name text, values frozen<nested_type>)";
    std::string create_table =
        "CREATE TABLE key_value_pair (key int PRIMARY KEY, value frozen<parent_type>)";
    std::string insert_query = "INSERT INTO key_value_pair(key, value) VALUES (?, ?)";
    std::string select_query = "SELECT value FROM key_value_pair WHERE key=?";

    // Create the UDTs and table for the test
    test_utils::execute_query(tester.session, nested_type.c_str());
    test_utils::execute_query(tester.session, parent_type.c_str());
    test_utils::CassUserTypePtr nested_udt = tester.new_udt("nested_type");
    test_utils::CassUserTypePtr parent_udt = tester.new_udt("parent_type");
    test_utils::execute_query(tester.session, create_table.c_str());

    // Fill in the nested UDT values
    BOOST_REQUIRE_EQUAL(
        test_utils::Value<cass_int32_t>::user_type_set_by_name(nested_udt.get(), "value_1", 100),
        CASS_OK);
    BOOST_REQUIRE_EQUAL(
        test_utils::Value<cass_int32_t>::user_type_set_by_name(nested_udt.get(), "value_2", 200),
        CASS_OK);

    // Fill in the parent UDT values
    BOOST_REQUIRE_EQUAL(
        test_utils::Value<CassString>::user_type_set_by_name(parent_udt.get(), "name", "DataStax"),
        CASS_OK);
    BOOST_REQUIRE_EQUAL(
        cass_user_type_set_user_type_by_name(parent_udt.get(), "values", nested_udt.get()),
        CASS_OK);

    // Bind and insert the nested UDT into Cassandra
    test_utils::CassStatementPtr statement(cass_statement_new(insert_query.c_str(), 2));
    BOOST_REQUIRE_EQUAL(test_utils::Value<cass_int32_t>::bind_by_name(statement.get(), "key", 1),
                        CASS_OK);
    BOOST_REQUIRE_EQUAL(
        cass_statement_bind_user_type_by_name(statement.get(), "value", parent_udt.get()), CASS_OK);
    test_utils::wait_and_check_error(
        test_utils::CassFuturePtr(cass_session_execute(tester.session, statement.get())).get());

    // Ensure the UDT can be read
    statement = test_utils::CassStatementPtr(cass_statement_new(select_query.c_str(), 1));
    BOOST_REQUIRE_EQUAL(test_utils::Value<cass_int32_t>::bind(statement.get(), 0, 1), CASS_OK);
    test_utils::CassFuturePtr future =
        test_utils::CassFuturePtr(cass_session_execute(tester.session, statement.get()));
    test_utils::wait_and_check_error(future.get());
    test_utils::CassResultPtr result(cass_future_get_result(future.get()));
    BOOST_REQUIRE_EQUAL(cass_result_row_count(result.get()), 1);
    BOOST_REQUIRE_EQUAL(cass_result_column_count(result.get()), 1);
    const CassRow* row = cass_result_first_row(result.get());
    const CassValue* value = cass_row_get_column(row, 0);
    BOOST_REQUIRE_EQUAL(cass_value_type(value), CASS_VALUE_TYPE_UDT);

    // Verify the parent key
    test_utils::CassIteratorPtr iterator(cass_iterator_fields_from_user_type(value));
    BOOST_REQUIRE(cass_iterator_next(iterator.get()));
    const CassValue* name_value = cass_iterator_get_user_type_field_value(iterator.get());
    BOOST_REQUIRE_EQUAL(cass_value_type(name_value), CASS_VALUE_TYPE_VARCHAR);
    CassString name_result;
    BOOST_REQUIRE_EQUAL(test_utils::Value<CassString>::get(name_value, &name_result), CASS_OK);
    BOOST_REQUIRE(test_utils::Value<CassString>::equal(name_result, "DataStax"));

    // Ensure the nested value is a UDT and create the iterator for the validation
    BOOST_REQUIRE(cass_iterator_next(iterator.get()));
    value = cass_iterator_get_user_type_field_value(iterator.get());
    BOOST_REQUIRE_EQUAL(cass_value_type(value), CASS_VALUE_TYPE_UDT);
    BOOST_REQUIRE_EQUAL(cass_value_item_count(value), 2);

    // Verify the values in the nested UDT
    test_utils::CassIteratorPtr nested_iterator(cass_iterator_fields_from_user_type(value));
    BOOST_REQUIRE(cass_iterator_next(nested_iterator.get()));
    const CassValue* value_value = cass_iterator_get_user_type_field_value(nested_iterator.get());
    BOOST_REQUIRE_EQUAL(cass_value_type(value_value), CASS_VALUE_TYPE_INT);
    cass_int32_t value_result;
    BOOST_REQUIRE_EQUAL(test_utils::Value<cass_int32_t>::get(value_value, &value_result), CASS_OK);
    BOOST_REQUIRE(test_utils::Value<cass_int32_t>::equal(value_result, 100));
    BOOST_REQUIRE(cass_iterator_next(nested_iterator.get()));
    value_value = cass_iterator_get_user_type_field_value(nested_iterator.get());
    BOOST_REQUIRE_EQUAL(cass_value_type(value_value), CASS_VALUE_TYPE_INT);
    BOOST_REQUIRE_EQUAL(test_utils::Value<cass_int32_t>::get(value_value, &value_result), CASS_OK);
    BOOST_REQUIRE(test_utils::Value<cass_int32_t>::equal(value_result, 200));
  } else {
    std::cout << "Unsupported Test for Cassandra v" << version.to_string()
              << ": Skipping udts/text_types" << std::endl;
    BOOST_REQUIRE(true);
  }
}

BOOST_AUTO_TEST_SUITE_END()
