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

#include <algorithm>

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/cstdint.hpp>
#include <boost/format.hpp>
#include <boost/thread.hpp>
#include <boost/chrono.hpp>

#include "cassandra.h"
#include "test_utils.hpp"

struct IteratorTests : public test_utils::SingleSessionTest {
  IteratorTests() : SingleSessionTest(1, 0) {
    test_utils::execute_query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT)
                                           % test_utils::SIMPLE_KEYSPACE % "1"));
    test_utils::execute_query(session, str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));
  }
};

BOOST_FIXTURE_TEST_SUITE(iterators, IteratorTests)

BOOST_AUTO_TEST_CASE(result_iterator)
{
  std::string table_name = str(boost::format("table_%s") % test_utils::generate_unique_str(uuid_gen));
  std::string create_table_query = str(boost::format("CREATE TABLE %s (part timeuuid, key int, value int, PRIMARY KEY(part, key));")
                                       % table_name);

  test_utils::execute_query(session, create_table_query);

  std::string part =  test_utils::string_from_uuid(test_utils::generate_time_uuid(uuid_gen));

  int num_rows = 10;

  for (cass_int32_t i = 0; i < num_rows; ++i) {
    std::string insert_query = str(boost::format("INSERT INTO %s (part, key, value) VALUES (%s, %d, %d)")
                                   % table_name
                                   % part % i % i);
    test_utils::execute_query(session, insert_query);
  }

  test_utils::CassResultPtr result;
  test_utils::execute_query(session, str(boost::format("SELECT key FROM %s WHERE part = %s")
                                         % table_name
                                         % part),
                            &result);

  BOOST_REQUIRE(cass_result_row_count(result.get()) == static_cast<size_t>(num_rows));

  test_utils::CassIteratorPtr iterator(cass_iterator_from_result(result.get()));

  cass_int32_t count = 0;
  while (cass_iterator_next(iterator.get())) {
    const CassRow* row = cass_iterator_get_row(iterator.get());

    cass_int32_t key;
    BOOST_REQUIRE(cass_value_get_int32(cass_row_get_column(row, 0), &key) == CASS_OK);
    BOOST_REQUIRE(key == count++);
  }
}

BOOST_AUTO_TEST_CASE(row_iterator)
{
  std::string table_name = str(boost::format("table_%s") % test_utils::generate_unique_str(uuid_gen));
  std::string create_table_query = str(boost::format("CREATE TABLE %s (key int PRIMARY KEY, first int, second int, third int);")
                                       % table_name);

  test_utils::execute_query(session, create_table_query);

  std::string insert_query = str(boost::format("INSERT INTO %s (key, first, second, third) VALUES (0, 1, 2, 3)")
                                 % table_name);
  test_utils::execute_query(session, insert_query);

  test_utils::CassResultPtr result;
  test_utils::execute_query(session, str(boost::format("SELECT * FROM %s")
                                         % table_name),
                            &result);

  BOOST_REQUIRE(cass_result_row_count(result.get()) > 0);

  const CassRow* row = cass_result_first_row(result.get());

  test_utils::CassIteratorPtr iterator(cass_iterator_from_row(row));

  cass_int32_t count = 0;
  while (cass_iterator_next(iterator.get())) {
    cass_int32_t column;
    BOOST_REQUIRE(cass_value_get_int32(cass_iterator_get_column(iterator.get()), &column) == CASS_OK);
    BOOST_REQUIRE(column == count++);
  }
  BOOST_REQUIRE(cass_result_column_count(result.get()) == static_cast<size_t>(count));
}

BOOST_AUTO_TEST_CASE(collection_list_iterator)
{
  std::string table_name = str(boost::format("table_%s") % test_utils::generate_unique_str(uuid_gen));
  std::string create_table_query = str(boost::format("CREATE TABLE %s (key int PRIMARY KEY, value list<int>);")
                                       % table_name);

  test_utils::execute_query(session, create_table_query);

  std::string insert_query = str(boost::format("INSERT INTO %s (key, value) VALUES (0, [ 0, 1, 2, 3 ])")
                                 % table_name);
  test_utils::execute_query(session, insert_query);

  test_utils::CassResultPtr result;
  test_utils::execute_query(session, str(boost::format("SELECT * FROM %s")
                                         % table_name),
                            &result);
  BOOST_REQUIRE(cass_result_row_count(result.get()) > 0);
  BOOST_REQUIRE(cass_result_column_count(result.get()) == 2);

  const CassRow* row = cass_result_first_row(result.get());
  const CassValue* collection = cass_row_get_column(row, 1);

  test_utils::CassIteratorPtr iterator(cass_iterator_from_collection(collection));
  cass_int32_t count = 0;
  while (cass_iterator_next(iterator.get())) {
    cass_int32_t value;
    BOOST_REQUIRE(cass_value_get_int32(cass_iterator_get_value(iterator.get()), &value) == CASS_OK);
    BOOST_REQUIRE(value == count++);
  }
  BOOST_REQUIRE(count == 4);
}

BOOST_AUTO_TEST_CASE(collection_set_iterator)
{
  std::string table_name = str(boost::format("table_%s") % test_utils::generate_unique_str(uuid_gen));
  std::string create_table_query = str(boost::format("CREATE TABLE %s (key int PRIMARY KEY, value set<int>);")
                                       % table_name);

  test_utils::execute_query(session, create_table_query);

  std::string insert_query = str(boost::format("INSERT INTO %s (key, value) VALUES (0, { 0, 1, 2, 3 })")
                                 % table_name);
  test_utils::execute_query(session, insert_query);

  test_utils::CassResultPtr result;
  test_utils::execute_query(session, str(boost::format("SELECT * FROM %s")
                                         % table_name),
                            &result);
  BOOST_REQUIRE(cass_result_row_count(result.get()) > 0);
  BOOST_REQUIRE(cass_result_column_count(result.get()) == 2);

  const CassRow* row = cass_result_first_row(result.get());
  const CassValue* collection = cass_row_get_column(row, 1);

  test_utils::CassIteratorPtr iterator(cass_iterator_from_collection(collection));

  cass_int32_t count = 0;
  while (cass_iterator_next(iterator.get())) {
    cass_int32_t value;
    BOOST_REQUIRE(cass_value_get_int32(cass_iterator_get_value(iterator.get()), &value) == CASS_OK);
    BOOST_REQUIRE(value == count++);
  }
  BOOST_REQUIRE(count == 4);
}

BOOST_AUTO_TEST_CASE(collection_map_iterator)
{
  std::string table_name = str(boost::format("table_%s") % test_utils::generate_unique_str(uuid_gen));
  std::string create_table_query = str(boost::format("CREATE TABLE %s (key int PRIMARY KEY, value map<text, int>);")
                                       % table_name);

  test_utils::execute_query(session, create_table_query);

  std::string insert_query = str(boost::format("INSERT INTO %s (key, value) VALUES (0, { 'a': 0, 'b': 1, 'c': 2, 'd': 3 })")
                                 % table_name);
  test_utils::execute_query(session, insert_query);

  test_utils::CassResultPtr result;
  test_utils::execute_query(session, str(boost::format("SELECT * FROM %s")
                                         % table_name),
                            &result);
  BOOST_REQUIRE(cass_result_row_count(result.get()) > 0);
  BOOST_REQUIRE(cass_result_column_count(result.get()) == 2);

  const CassRow* row = cass_result_first_row(result.get());
  const CassValue* collection = cass_row_get_column(row, 1);

  test_utils::CassIteratorPtr iterator(cass_iterator_from_collection(collection));

  cass_int32_t count = 0;
  while (cass_iterator_next(iterator.get())) {
    CassString key;
    BOOST_REQUIRE(cass_value_type(cass_iterator_get_value(iterator.get())) == CASS_VALUE_TYPE_VARCHAR);
    BOOST_REQUIRE(cass_value_get_string(cass_iterator_get_value(iterator.get()), &key.data, &key.length) == CASS_OK);
    BOOST_REQUIRE(key.length == 1 && key.data[0] == 'a' + count);
    BOOST_REQUIRE(cass_iterator_next(iterator.get()));

    cass_int32_t value;
    BOOST_REQUIRE(cass_value_type(cass_iterator_get_value(iterator.get())) == CASS_VALUE_TYPE_INT);
    BOOST_REQUIRE(cass_value_get_int32(cass_iterator_get_value(iterator.get()), &value) == CASS_OK);
    BOOST_REQUIRE(value == count++);
  }
  BOOST_REQUIRE(count == 4);
}

BOOST_AUTO_TEST_CASE(map_iterator)
{
  std::string table_name = str(boost::format("table_%s") % test_utils::generate_unique_str(uuid_gen));
  std::string create_table_query = str(boost::format("CREATE TABLE %s (key int PRIMARY KEY, value map<text, int>);")
                                       % table_name);

  test_utils::execute_query(session, create_table_query);

  std::string insert_query = str(boost::format("INSERT INTO %s (key, value) VALUES (0, { 'a': 0, 'b': 1, 'c': 2, 'd': 3 })")
                                 % table_name);
  test_utils::execute_query(session, insert_query);

  test_utils::CassResultPtr result;
  test_utils::execute_query(session, str(boost::format("SELECT * FROM %s")
                                         % table_name),
                            &result);
  BOOST_REQUIRE(cass_result_row_count(result.get()) > 0);
  BOOST_REQUIRE(cass_result_column_count(result.get()) == 2);

  const CassRow* row = cass_result_first_row(result.get());
  const CassValue* map = cass_row_get_column(row, 1);

  test_utils::CassIteratorPtr iterator(cass_iterator_from_map(map));

  cass_int32_t count = 0;
  while (cass_iterator_next(iterator.get())) {
    CassString key;
    BOOST_REQUIRE(cass_value_type(cass_iterator_get_map_key(iterator.get())) == CASS_VALUE_TYPE_VARCHAR);
    BOOST_REQUIRE(cass_value_get_string(cass_iterator_get_map_key(iterator.get()), &key.data, &key.length) == CASS_OK);
    BOOST_REQUIRE(key.length == 1 && key.data[0] == 'a' + count);

    cass_int32_t value;
    BOOST_REQUIRE(cass_value_type(cass_iterator_get_map_value(iterator.get())) == CASS_VALUE_TYPE_INT);
    BOOST_REQUIRE(cass_value_get_int32(cass_iterator_get_map_value(iterator.get()), &value) == CASS_OK);
    BOOST_REQUIRE(value == count++);
  }
  BOOST_REQUIRE(count == 4);
}

BOOST_AUTO_TEST_CASE(empty)
{
  std::string table_name = str(boost::format("table_%s") % test_utils::generate_unique_str(uuid_gen));
  std::string create_table_query = str(boost::format("CREATE TABLE %s (part timeuuid, key int, value int, PRIMARY KEY(part, key));")
                                       % table_name);

  test_utils::execute_query(session, create_table_query);


  test_utils::CassResultPtr result;
  test_utils::execute_query(session, str(boost::format("SELECT * FROM %s")
                                         % table_name),
                            &result);

  BOOST_REQUIRE(cass_result_row_count(result.get()) == 0);

  test_utils::CassIteratorPtr iterator(cass_iterator_from_result(result.get()));
  BOOST_REQUIRE(cass_iterator_next(iterator.get()) == cass_false);
}

BOOST_AUTO_TEST_CASE(invalid_value_types)
{
  std::string table_name = str(boost::format("table_%s") % test_utils::generate_unique_str(uuid_gen));
  std::string create_table_query = str(boost::format("CREATE TABLE %s (key int PRIMARY KEY, value list<int>);")
                                       % table_name);

  test_utils::execute_query(session, create_table_query);

  std::string insert_query = str(boost::format("INSERT INTO %s (key, value) VALUES (0, [ 0, 1, 2, 3 ])")
                                 % table_name);
  test_utils::execute_query(session, insert_query);

  test_utils::CassResultPtr result;
  test_utils::execute_query(session, str(boost::format("SELECT * FROM %s")
                                         % table_name),
                            &result);
  BOOST_REQUIRE(cass_result_row_count(result.get()) > 0);
  BOOST_REQUIRE(cass_result_column_count(result.get()) == 2);

  const CassRow* row = cass_result_first_row(result.get());
  const CassValue* key = cass_row_get_column(row, 0);
  const CassValue* value = cass_row_get_column(row, 1);

  BOOST_CHECK(cass_iterator_from_map(key) == NULL);
  BOOST_CHECK(cass_iterator_from_map(value) == NULL);
  BOOST_CHECK(cass_iterator_from_collection(key) == NULL);
}

BOOST_AUTO_TEST_SUITE_END()
