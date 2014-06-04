#define BOOST_TEST_DYN_LINK
#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include <memory>
#include <future>
#include <chrono>

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
                                                   % test_utils::SIMPLE_KEYSPACE % "1"));
      test_utils::execute_query(session, str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));
    }
};

BOOST_FIXTURE_TEST_SUITE(async, AsyncTests)




BOOST_AUTO_TEST_CASE(test_async)
{
  std::string table_name = str(boost::format("table_%s") % test_utils::generate_unique_str());
  std::string create_table_query = str(boost::format("CREATE TABLE %s (id timeuuid PRIMARY KEY, num int, str text);") % table_name);

  test_utils::execute_query(session, create_table_query);

  std::string insert_query = str(boost::format("INSERT INTO %s (id, numb, label) VALUES(?, ?, ?)") % table_name);

  std::vector<test_utils::Uuid> ids;
  std::vector<test_utils::CassFuturePtr> futures;
  for(int i = 0; i < 10; ++i) {
    test_utils::Uuid id = test_utils::generate_time_uuid();
    test_utils::CassStatementPtr statement(cass_statement_new(cass_string_init2(insert_query.data(), insert_query.size()), 3, CASS_CONSISTENCY_ONE));
    BOOST_REQUIRE(cass_statement_bind_uuid(statement.get(), 0, id) == CASS_OK);
    BOOST_REQUIRE(cass_statement_bind_int32(statement.get(), 1, i) == CASS_OK);
    std::string str_value = str(boost::format("row%d") % i);
    BOOST_REQUIRE(cass_statement_bind_string(statement.get(), 2, cass_string_init2(str_value.data(), str_value.size())) == CASS_OK);
    futures.emplace_back(test_utils::CassFuturePtr(cass_session_execute(session, statement.get())));
    ids.push_back(id);
  }

  std::this_thread::sleep_for(std::chrono::seconds(30));

//  for(auto& future : futures) {
//    test_utils::wait_and_check_error(future.get());
//  }

}

BOOST_AUTO_TEST_SUITE_END()
