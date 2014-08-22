#include <boost/test/test_tools.hpp>
#include <boost/test/debug.hpp>
#include <boost/thread.hpp>
#include <boost/format.hpp>

#include "policy_tools.hpp"
#include "future.hpp"
#include "types.hpp"
#include "request_handler.hpp"

void PolicyTool::show_coordinators()	// show what queries went to what nodes IP.
{
  for(std::map<cass::Address, int>::const_iterator p = coordinators.begin(); p != coordinators.end(); ++p) {
    std::cout << p->first.to_string() << " : " << p->second << std::endl;
  }
}

void PolicyTool::reset_coordinators()
{
  coordinators.clear();
}

void PolicyTool::create_schema(CassSession* session, int replicationFactor)
{
  test_utils::execute_query(session,
                            str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT) % test_utils::SIMPLE_KEYSPACE % replicationFactor));
  test_utils::execute_query(session, str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));
  test_utils::execute_query(session,
                            str(boost::format("CREATE TABLE %s (k int PRIMARY KEY, i int)") % test_utils::SIMPLE_TABLE));
}

void PolicyTool::init(CassSession* session, int n, CassConsistency cl, bool batch)
{
  std::string query = str(boost::format("INSERT INTO %s (k, i) VALUES (0, 0)") % test_utils::SIMPLE_TABLE);

  if (batch) {
    std::string bth;
    bth.append("BEGIN BATCH ");
    bth.append(str(boost::format("INSERT INTO %s (k, i) VALUES (0, 0)") % test_utils::SIMPLE_TABLE));
    bth.append(" APPLY BATCH");
    query = bth;
  }

  for (int i = 0; i < n; ++i) {
    test_utils::CassResultPtr result;
    test_utils::execute_query(session,
                              query,
                              &result,
                              cl);
  }
}

CassError PolicyTool::init_return_error(CassSession* session, int n, CassConsistency cl, bool batch)
{
  std::string query = str(boost::format("INSERT INTO %s (k, i) VALUES (0, 0)") % test_utils::SIMPLE_TABLE);

  if(batch) {
    std::string batch_query;
    batch_query.append("BEGIN BATCH ");
    batch_query.append(str(boost::format("INSERT INTO %s (k, i) VALUES (0, 0)") % test_utils::SIMPLE_TABLE));
    batch_query.append(" APPLY BATCH");
    query = batch_query;
  }

  for (int i = 0; i < n; ++i) {
    test_utils::CassResultPtr result;
    CassError rc = test_utils::execute_query_with_error(session,
                                                        query,
                                                        &result,
                                                        cl);
    if (rc != CASS_OK) {
      return rc;
    }
  }
  return CASS_OK;
}

void PolicyTool::add_coordinator(cass::Address address)
{
  if(coordinators.count(address) == 0) {
    coordinators.insert(std::make_pair(address, 1));
  } else {
    coordinators[address] += 1;
  }
}

void PolicyTool::assert_queried(cass::Address address, int n)
{
  if(coordinators.count(address) != 0) {
    int c = coordinators[address];
    BOOST_REQUIRE(c == n);
  } else {
    BOOST_REQUIRE(n == 0);
  }
}

void PolicyTool::assertQueriedAtLeast(cass::Address address, int n)
{
  int queried = coordinators[address];
  BOOST_REQUIRE(queried >= n);
}

void PolicyTool::query(CassSession* session, int n, CassConsistency cl)
{
  std::string select_query = str(boost::format("SELECT * FROM %s WHERE k = 0") % test_utils::SIMPLE_TABLE);
  for (int i = 0; i < n; ++i) {
    test_utils::CassStatementPtr statement(cass_statement_new(
                                             cass_string_init2(select_query.data(), select_query.size()),
                                             0));
    cass_statement_set_consistency(statement.get(), cl);
    test_utils::CassFuturePtr future(cass_session_execute(session, statement.get()));
    test_utils::wait_and_check_error(future.get());
    add_coordinator(static_cast<cass::ResponseFuture*>(future->from())->get_host().address());
  }
}

CassError PolicyTool::query_return_error(CassSession* session, int n, CassConsistency cl)
{
  std::string select_query = str(boost::format("SELECT * FROM %s WHERE k = 0") % test_utils::SIMPLE_TABLE);
  for (int i = 0; i < n; ++i) {
    test_utils::CassStatementPtr statement
        (cass_statement_new(cass_string_init2(select_query.data(), select_query.size()), 0));
    cass_statement_set_consistency(statement.get(), cl);
    test_utils::CassFuturePtr future(cass_session_execute(session, statement.get()));
    CassError rc = test_utils::wait_and_return_error(future.get());

    if (rc != CASS_OK) {
      return rc;
    }
    add_coordinator(static_cast<cass::ResponseFuture*>(future->from())->get_host().address());
  }
  return CASS_OK;
}
