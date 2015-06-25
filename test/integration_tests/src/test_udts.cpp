/*
  Copyright (c) 2014-2015 DataStax

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

#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include "test_utils.hpp"
#include "testing.hpp"
#include "cassandra.h"

#include <boost/format.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/thread.hpp>

#include <algorithm>

#define PHONE_UDT_CQL "CREATE TYPE phone (alias text, number text)"
#define ADDRESS_UDT_CQL "CREATE TYPE address (street text, \"ZIP\" int, phone_numbers set<frozen<phone>>)"

struct UDTTests : public test_utils::SingleSessionTest {
private:
  /**
   * Session schema metadata
   */
  test_utils::CassSchemaPtr schema_;

  /**
   * Update the session schema metadata
   */
  void update_schema() {
    schema_ = test_utils::CassSchemaPtr(cass_session_get_schema(session));
  }

  /**
   * Verify the user data type exists (updating schema if needed up to 10 times)
   *
   * @param udt_name Name of the UDT to verify
   */
  void verify_user_type(const std::string& udt_name) {
    std::vector<std::string> udt_field_names;
    unsigned int count = 0;
    while (udt_field_names.empty() && ++count <= 10) {
      update_schema();
      udt_field_names = cass::get_user_data_type_field_names(schema_.get(), test_utils::SIMPLE_KEYSPACE.c_str(), udt_name);
      if (udt_field_names.empty()) {
        boost::this_thread::sleep_for(boost::chrono::milliseconds(100));
      }
    }
    BOOST_REQUIRE(!udt_field_names.empty());
  }

public:
  UDTTests() : test_utils::SingleSessionTest(1, 0), schema_(NULL) {
    test_utils::execute_query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT) % test_utils::SIMPLE_KEYSPACE % "1"));
    test_utils::execute_query(session, str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));
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
   * Create a new phone UDT
   *
   * @return Phone UDT
   */
  test_utils::CassUserTypePtr new_phone_udt() {
    const CassDataType* phone = cass_schema_get_udt(schema_.get(), test_utils::SIMPLE_KEYSPACE.c_str(), "phone");
    BOOST_REQUIRE(phone != NULL);
    return test_utils::CassUserTypePtr(cass_user_type_new_from_data_type(phone));
  }

  /**
  * Create a new address UDT
  *
  * @return Address UDT
  */
  test_utils::CassUserTypePtr new_address_udt() {
    const CassDataType* address = cass_schema_get_udt(schema_.get(), test_utils::SIMPLE_KEYSPACE.c_str(), "address");
    BOOST_REQUIRE(address != NULL);
    return test_utils::CassUserTypePtr(cass_user_type_new_from_data_type(address));
  }
};

BOOST_AUTO_TEST_SUITE(udts);

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
  CassVersion version = test_utils::get_version();
  if ((version.major >= 2 && version.minor >= 1) || version.major > 2) {
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
      CassString home_phone_number("555-911-1212");
      test_utils::CassUserTypePtr home_phone(tester.new_phone_udt());
      BOOST_REQUIRE_EQUAL(test_utils::Value<CassString>::user_type_set(home_phone.get(), 0, CassString("Home")), CASS_OK);
      BOOST_REQUIRE_EQUAL(test_utils::Value<CassString>::user_type_set(home_phone.get(), 1, home_phone_number), CASS_OK);
      CassString work_phone_number("650-389-6000");
      test_utils::CassUserTypePtr work_phone(tester.new_phone_udt());
      BOOST_REQUIRE_EQUAL(test_utils::Value<CassString>::user_type_set(work_phone.get(), 0, CassString("Work")), CASS_OK);
      BOOST_REQUIRE_EQUAL(test_utils::Value<CassString>::user_type_set(work_phone.get(), 1, work_phone_number), CASS_OK);

      // Create a collection for the phone numbers
      test_utils::CassCollectionPtr phone_numbers(cass_collection_new(CASS_COLLECTION_TYPE_SET, 2));
      BOOST_REQUIRE_EQUAL(cass_collection_append_user_type(phone_numbers.get(), home_phone.get()), CASS_OK);
      BOOST_REQUIRE_EQUAL(cass_collection_append_user_type(phone_numbers.get(), work_phone.get()), CASS_OK);

      // Address number UDT (nested UDT)
      CassString street_address("3975 Freedom Circle");
      cass_int32_t zip_code = 95054;
      test_utils::CassUserTypePtr address(tester.new_address_udt());
      BOOST_REQUIRE_EQUAL(test_utils::Value<CassString>::user_type_set(address.get(), 0, street_address), CASS_OK);
      BOOST_REQUIRE_EQUAL(test_utils::Value<cass_int32_t>::user_type_set(address.get(), 1, zip_code), CASS_OK);
      BOOST_REQUIRE_EQUAL(cass_user_type_set_collection(address.get(), 2, phone_numbers.get()), CASS_OK);

      // Bind and insert the UDT into Cassandra
      CassUuid key = test_utils::generate_time_uuid(tester.uuid_gen);
      test_utils::CassStatementPtr statement(cass_statement_new(insert_query.c_str(), 2));
      BOOST_REQUIRE_EQUAL(cass_statement_bind_uuid(statement.get(), 0, key), CASS_OK);
      BOOST_REQUIRE_EQUAL(cass_statement_bind_user_type(statement.get(), 1, address.get()), CASS_OK);
      test_utils::wait_and_check_error(cass_session_execute(tester.session, statement.get()));

      // Ensure the UDT can be read
      statement = test_utils::CassStatementPtr(cass_statement_new(select_query.c_str(), 1));
      BOOST_REQUIRE_EQUAL(cass_statement_bind_uuid(statement.get(), 0, key), CASS_OK);
      test_utils::CassFuturePtr future = test_utils::CassFuturePtr(cass_session_execute(tester.session, statement.get()));
      test_utils::wait_and_check_error(future.get());
      test_utils::CassResultPtr result(cass_future_get_result(future.get()));
      size_t row_count = cass_result_row_count(result.get());
      BOOST_REQUIRE_EQUAL(cass_result_row_count(result.get()), 1);
      BOOST_REQUIRE_EQUAL(cass_result_column_count(result.get()), 1);
      const CassRow* row = cass_result_first_row(result.get());
      const CassValue* value = cass_row_get_column(row, 0);
      BOOST_REQUIRE_EQUAL(cass_value_type(value), CASS_VALUE_TYPE_UDT);
      //TODO: Determine why the count for the UDT is not correct
      //BOOST_REQUIRE_EQUAL(cass_value_item_count(value), 3);
      test_utils::CassIteratorPtr fields(cass_iterator_from_user_type(value));
      // Verify street address
      BOOST_REQUIRE(cass_iterator_next(fields.get()));
      CassString street_address_field;
      BOOST_REQUIRE_EQUAL(cass_iterator_get_user_type_field_name(fields.get(), &street_address_field.data, &street_address_field.length), CASS_OK);
      BOOST_REQUIRE(test_utils::Value<CassString>::equal("street", street_address_field));
      const CassValue* street_address_value = cass_iterator_get_user_type_field_value(fields.get());
      BOOST_REQUIRE_EQUAL(cass_value_type(street_address_value), CASS_VALUE_TYPE_VARCHAR);
      CassString street_address_result;
      BOOST_REQUIRE_EQUAL(cass_value_get_string(street_address_value, &street_address_result.data, &street_address_result.length), CASS_OK);
      BOOST_REQUIRE(test_utils::Value<CassString>::equal(street_address_result, street_address));
      // Verify zip code
      BOOST_REQUIRE(cass_iterator_next(fields.get()));
      CassString zip_code_field;
      BOOST_REQUIRE_EQUAL(cass_iterator_get_user_type_field_name(fields.get(), &zip_code_field.data, &zip_code_field.length), CASS_OK);
      BOOST_REQUIRE(test_utils::Value<CassString>::equal("ZIP", zip_code_field));
      const CassValue* zip_code_value = cass_iterator_get_user_type_field_value(fields.get());
      BOOST_REQUIRE_EQUAL(cass_value_type(street_address_value), CASS_VALUE_TYPE_INT);
      cass_int32_t zip_code_result;
      BOOST_REQUIRE_EQUAL(cass_value_get_int32(zip_code_value, &zip_code_result), CASS_OK);
      BOOST_REQUIRE(test_utils::Value<cass_int32_t>::equal(zip_code_result, zip_code));
      /* Verify phone numbers in set collection */
      // Verify collection
      BOOST_REQUIRE(cass_iterator_next(fields.get()));
      CassString phone_numbers_field;
      BOOST_REQUIRE_EQUAL(cass_iterator_get_user_type_field_name(fields.get(), &phone_numbers_field.data, &phone_numbers_field.length), CASS_OK);
      BOOST_REQUIRE(test_utils::Value<CassString>::equal("phone_numbers", phone_numbers_field));
      const CassValue* collection_value = cass_iterator_get_user_type_field_value(fields.get());
      BOOST_REQUIRE_EQUAL(cass_value_type(collection_value), CASS_VALUE_TYPE_SET);
      BOOST_REQUIRE_EQUAL(cass_value_item_count(collection_value), 2);
      /* Verify home phone number */
      // Verify alias
      test_utils::CassIteratorPtr phone_numbers_iterator(cass_iterator_from_collection(collection_value));
      BOOST_REQUIRE(cass_iterator_next(phone_numbers_iterator.get()));
      const CassValue* home_phone_number_udt_value = cass_iterator_get_value(phone_numbers_iterator.get());
      BOOST_REQUIRE_EQUAL(cass_value_type(home_phone_number_udt_value), CASS_VALUE_TYPE_UDT);
      //TODO: Determine why the count for the UDT is not correct
      //BOOST_REQUIRE_EQUAL(cass_value_item_count(home_phone_number_udt_value), 2);
      test_utils::CassIteratorPtr home_phone_number_iterator(cass_iterator_from_user_type(home_phone_number_udt_value));
      BOOST_REQUIRE(cass_iterator_next(home_phone_number_iterator.get()));
      CassString home_phone_alias_field;
      BOOST_REQUIRE_EQUAL(cass_iterator_get_user_type_field_name(home_phone_number_iterator.get(), &home_phone_alias_field.data, &home_phone_alias_field.length), CASS_OK);
      BOOST_REQUIRE(test_utils::Value<CassString>::equal("alias", home_phone_alias_field));
      const CassValue* home_phone_alias_value = cass_iterator_get_user_type_field_value(home_phone_number_iterator.get());
      BOOST_REQUIRE_EQUAL(cass_value_type(home_phone_alias_value), CASS_VALUE_TYPE_VARCHAR);
      CassString home_phone_alias_result;
      BOOST_REQUIRE_EQUAL(cass_value_get_string(home_phone_alias_value, &home_phone_alias_result.data, &home_phone_alias_result.length), CASS_OK);
      BOOST_REQUIRE(test_utils::Value<CassString>::equal(home_phone_alias_result, CassString("Home")));
      // Verify phone number
      BOOST_REQUIRE(cass_iterator_next(home_phone_number_iterator.get()));
      CassString home_phone_number_field;
      BOOST_REQUIRE_EQUAL(cass_iterator_get_user_type_field_name(home_phone_number_iterator.get(), &home_phone_number_field.data, &home_phone_number_field.length), CASS_OK);
      BOOST_REQUIRE(test_utils::Value<CassString>::equal("number", home_phone_number_field));
      const CassValue* home_phone_number_value = cass_iterator_get_user_type_field_value(home_phone_number_iterator.get());
      BOOST_REQUIRE_EQUAL(cass_value_type(home_phone_number_value), CASS_VALUE_TYPE_VARCHAR);
      CassString home_phone_number_result;
      BOOST_REQUIRE_EQUAL(cass_value_get_string(home_phone_number_value, &home_phone_number_result.data, &home_phone_number_result.length), CASS_OK);
      BOOST_REQUIRE(test_utils::Value<CassString>::equal(home_phone_number_result, home_phone_number));
      /* Verify work phone number */
      // Verify alias
      BOOST_REQUIRE(cass_iterator_next(phone_numbers_iterator.get()));
      const CassValue* work_phone_number_udt_value = cass_iterator_get_value(phone_numbers_iterator.get());
      BOOST_REQUIRE_EQUAL(cass_value_type(work_phone_number_udt_value), CASS_VALUE_TYPE_UDT);
      //TODO: Determine why the count for the UDT is not correct
      //BOOST_REQUIRE_EQUAL(cass_value_item_count(work_phone_number_udt_value), 2);
      test_utils::CassIteratorPtr work_phone_number_iterator(cass_iterator_from_user_type(work_phone_number_udt_value));
      BOOST_REQUIRE(cass_iterator_next(work_phone_number_iterator.get()));
      CassString work_phone_alias_field;
      BOOST_REQUIRE_EQUAL(cass_iterator_get_user_type_field_name(work_phone_number_iterator.get(), &work_phone_alias_field.data, &work_phone_alias_field.length), CASS_OK);
      BOOST_REQUIRE(test_utils::Value<CassString>::equal("alias", work_phone_alias_field));
      const CassValue* work_phone_alias_value = cass_iterator_get_user_type_field_value(work_phone_number_iterator.get());
      BOOST_REQUIRE_EQUAL(cass_value_type(work_phone_alias_value), CASS_VALUE_TYPE_VARCHAR);
      CassString work_phone_alias_result;
      BOOST_REQUIRE_EQUAL(cass_value_get_string(work_phone_alias_value, &work_phone_alias_result.data, &work_phone_alias_result.length), CASS_OK);
      BOOST_REQUIRE(test_utils::Value<CassString>::equal(work_phone_alias_result, CassString("Work")));
      // Verify phone number
      BOOST_REQUIRE(cass_iterator_next(work_phone_number_iterator.get()));
      CassString work_phone_number_field;
      BOOST_REQUIRE_EQUAL(cass_iterator_get_user_type_field_name(work_phone_number_iterator.get(), &work_phone_number_field.data, &work_phone_number_field.length), CASS_OK);
      BOOST_REQUIRE(test_utils::Value<CassString>::equal("number", work_phone_number_field));
      const CassValue* work_phone_number_value = cass_iterator_get_user_type_field_value(work_phone_number_iterator.get());
      BOOST_REQUIRE_EQUAL(cass_value_type(work_phone_number_value), CASS_VALUE_TYPE_VARCHAR);
      CassString work_phone_number_result;
      BOOST_REQUIRE_EQUAL(cass_value_get_string(work_phone_number_value, &work_phone_number_result.data, &work_phone_number_result.length), CASS_OK);
      BOOST_REQUIRE(test_utils::Value<CassString>::equal(work_phone_number_result, work_phone_number));
    }

    // Partial UDT
    {

    }
  } else {
    boost::unit_test::unit_test_log_t::instance().set_threshold_level(boost::unit_test::log_messages);
    BOOST_TEST_MESSAGE("Unsupported Test for Cassandra v" << version.to_string() << ": Skipping udts/read_write");
    BOOST_REQUIRE(true);
  }
}

BOOST_AUTO_TEST_SUITE_END()
