#define BOOST_TEST_DYN_LINK
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
                                           % test_utils::SIMPLE_KEYSPACE % "1"));
    test_utils::execute_query(session, str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));
  }
};

BOOST_FIXTURE_TEST_SUITE(async, AsyncTests)

std::vector<test_utils::Uuid> insert_async(CassSession* session,
                                           const std::string& table_name,
                                           size_t num_concurrent_requests,
                                           std::vector<test_utils::CassFuturePtr>* futures) {
  std::string create_table_query = str(boost::format("CREATE TABLE %s (id timeuuid PRIMARY KEY, num int, str text);") % table_name);

  test_utils::execute_query(session, create_table_query);

  std::string insert_query = str(boost::format("INSERT INTO %s (id, num, str) VALUES(?, ?, ?)") % table_name);

  std::vector<test_utils::Uuid> ids;
  for(size_t i = 0; i < num_concurrent_requests; ++i) {
    test_utils::Uuid id = test_utils::generate_time_uuid();
    test_utils::CassStatementPtr statement(cass_statement_new(cass_string_init2(insert_query.data(), insert_query.size()), 3));
    cass_statement_set_consistency(statement.get(), CASS_CONSISTENCY_QUORUM);
    BOOST_REQUIRE(cass_statement_bind_uuid(statement.get(), 0, id) == CASS_OK);
    BOOST_REQUIRE(cass_statement_bind_int32(statement.get(), 1, i) == CASS_OK);
    std::string str_value = str(boost::format("row%d") % i);
    BOOST_REQUIRE(cass_statement_bind_string(statement.get(), 2, cass_string_init2(str_value.data(), str_value.size())) == CASS_OK);
    futures->push_back(test_utils::CassFuturePtr(cass_session_execute(session, statement.get())));
    ids.push_back(id);
  }

  return ids;
}

void validate_results(CassSession* session,
                      const std::string& table_name,
                      size_t num_concurrent_requests,
                      const std::vector<test_utils::Uuid>& ids)
{
  std::string select_query = str(boost::format("SELECT * FROM %s;") % table_name);
  test_utils::CassResultPtr result;
  test_utils::execute_query(session, select_query, &result, CASS_CONSISTENCY_QUORUM);
  BOOST_REQUIRE(cass_result_row_count(result.get()) == num_concurrent_requests);

  test_utils::CassIteratorPtr iterator(cass_iterator_from_result(result.get()));

  while(cass_iterator_next(iterator.get())) {
    const CassRow* row = cass_iterator_get_row(iterator.get());
    test_utils::Uuid result_id;
    cass_value_get_uuid(cass_row_get_column(row, 0), result_id.uuid);
    BOOST_REQUIRE(std::find(ids.begin(), ids.end(), result_id) != ids.end());
  }
}

BOOST_AUTO_TEST_CASE(test_async)
{
  std::string table_name = str(boost::format("table_%s") % test_utils::generate_unique_str());
  const size_t num_concurrent_requests = 4096;

  std::vector<test_utils::CassFuturePtr> futures;
  std::vector<test_utils::Uuid> ids = insert_async(session, table_name, num_concurrent_requests, &futures);

  for(std::vector<test_utils::CassFuturePtr>::iterator it = futures.begin(),
      end = futures.end(); it != end; ++it) {
    test_utils::wait_and_check_error(it->get());
  }

  validate_results(session, table_name, num_concurrent_requests, ids);
}

BOOST_AUTO_TEST_CASE(test_async_close)
{
  std::string table_name = str(boost::format("table_%s") % test_utils::generate_unique_str());
  const size_t num_concurrent_requests = 4096;

  test_utils::CassFuturePtr session_future(cass_cluster_connect(cluster));
  test_utils::wait_and_check_error(session_future.get());
  test_utils::CassSessionPtr temp_session(cass_future_get_session(session_future.get()));

  test_utils::execute_query(temp_session.get(), str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));

  std::vector<test_utils::CassFuturePtr> futures;
  std::vector<test_utils::Uuid> ids = insert_async(temp_session.get(), table_name, num_concurrent_requests, &futures);

  temp_session.reset(); // close session

  validate_results(session, table_name, num_concurrent_requests, ids);
}

BOOST_AUTO_TEST_SUITE_END()
