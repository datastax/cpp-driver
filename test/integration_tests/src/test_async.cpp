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

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/cstdint.hpp>
#include <boost/format.hpp>
#include <boost/thread.hpp>

#include "cassandra.h"
#include "test_utils.hpp"

struct AsyncTests : public test_utils::SingleSessionTest {
  AsyncTests() : test_utils::SingleSessionTest(3, 0) {
    test_utils::execute_query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT)
                                           % test_utils::SIMPLE_KEYSPACE % "3"));
    test_utils::execute_query(session, str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));
  }

  static std::vector<CassUuid> insert_async(CassSession* session,
                                            CassUuidGen* uuid_gen,
                                            const std::string& table_name,
                                            size_t num_concurrent_requests,
                                            std::vector<test_utils::CassFuturePtr>* futures) {
    std::string create_table_query = str(boost::format("CREATE TABLE %s (id timeuuid PRIMARY KEY, num int, str text);") % table_name);

    test_utils::execute_query(session, create_table_query);

    std::string insert_query = str(boost::format("INSERT INTO %s (id, num, str) VALUES(?, ?, ?)") % table_name);

    std::vector<CassUuid> ids;
    for (size_t i = 0; i < num_concurrent_requests; ++i) {
      CassUuid id = test_utils::generate_time_uuid(uuid_gen);
      test_utils::CassStatementPtr statement(cass_statement_new_n(insert_query.data(), insert_query.size(), 3));
      cass_statement_set_consistency(statement.get(), CASS_CONSISTENCY_QUORUM);
      BOOST_REQUIRE(cass_statement_bind_uuid(statement.get(), 0, id) == CASS_OK);
      BOOST_REQUIRE(cass_statement_bind_int32(statement.get(), 1, i) == CASS_OK);
      std::string str_value = str(boost::format("row%d") % i);
      BOOST_REQUIRE(cass_statement_bind_string_n(statement.get(), 2, str_value.data(), str_value.size()) == CASS_OK);
      futures->push_back(test_utils::CassFuturePtr(cass_session_execute(session, statement.get())));
      ids.push_back(id);
    }

    return ids;
  }

  void validate_results(const std::string& table_name,
                        size_t num_concurrent_requests,
                        const std::vector<CassUuid>& ids)
  {
    std::string select_query = str(boost::format("SELECT * FROM %s;") % table_name);
    test_utils::CassResultPtr result;
    test_utils::execute_query(session, select_query, &result, CASS_CONSISTENCY_QUORUM);
    BOOST_REQUIRE_EQUAL(cass_result_row_count(result.get()), num_concurrent_requests);

    test_utils::CassIteratorPtr iterator(cass_iterator_from_result(result.get()));

    while (cass_iterator_next(iterator.get())) {
      const CassRow* row = cass_iterator_get_row(iterator.get());
      CassUuid result_id;
      cass_value_get_uuid(cass_row_get_column(row, 0), &result_id);
      BOOST_REQUIRE(std::find(ids.begin(), ids.end(), result_id) != ids.end());
    }
  }
};

BOOST_FIXTURE_TEST_SUITE(async, AsyncTests)

BOOST_AUTO_TEST_CASE(simple)
{
  std::string table_name = str(boost::format("table_%s") % test_utils::generate_unique_str(uuid_gen));
  const size_t num_concurrent_requests = 4096;

  std::vector<test_utils::CassFuturePtr> futures;
  std::vector<CassUuid> ids = insert_async(session, uuid_gen, table_name, num_concurrent_requests, &futures);

  for (std::vector<test_utils::CassFuturePtr>::iterator it = futures.begin(),
      end = futures.end(); it != end; ++it) {
    test_utils::wait_and_check_error(it->get());
  }

  validate_results(table_name, num_concurrent_requests, ids);
}

BOOST_AUTO_TEST_CASE(close)
{
  std::string table_name = str(boost::format("table_%s") % test_utils::generate_unique_str(uuid_gen));
  const size_t num_concurrent_requests = 4096;

  test_utils::CassSessionPtr temp_session(test_utils::create_session(cluster));

  test_utils::execute_query(temp_session.get(), str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));

  std::vector<test_utils::CassFuturePtr> futures;
  std::vector<CassUuid> ids = insert_async(temp_session.get(), uuid_gen, table_name, num_concurrent_requests, &futures);

  // Close session, this should wait for all pending requests to finish

  temp_session.reset();

  // All requests should be finished, validate

  validate_results(table_name, num_concurrent_requests, ids);
}

BOOST_AUTO_TEST_SUITE_END()
