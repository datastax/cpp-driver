/*
  Copyright (c) 2014 DataStax

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

#define BOOST_TEST_DYN_LINK
#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include "cassandra.h"
#include "test_utils.hpp"
#include "cql_ccm_bridge.hpp"

#include <boost/format.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/thread.hpp>

#include <algorithm>

struct ClusterInit {
  ClusterInit()
    : inst(3, 0)
    , session(NULL) {
    new_session();
  }

  ~ClusterInit() {
    close_session();
  }

  void new_session() {
    close_session();
    test_utils::CassFuturePtr connect_future(cass_cluster_connect(inst.cluster));
    test_utils::wait_and_check_error(connect_future.get());
    session = cass_future_get_session(connect_future.get());
  }

  void close_session() {
    if (session != NULL) {
      test_utils::CassFuturePtr close_future(cass_session_close(session));
      cass_future_wait(close_future.get());
      session = NULL;
    }
  }

  test_utils::MultipleNodesTest inst;
  CassSession* session;
};

BOOST_FIXTURE_TEST_SUITE(schema_agreement, ClusterInit)

// only doing a keyspace for now since there is no difference for types or tables
BOOST_AUTO_TEST_CASE(keyspace_add_drop)
{
  test_utils::CassLog::reset("Found schema agreement in");

  // "USE" in fast succession would normally fail on the next node if the previous query did not wait
  const std::string use_simple = str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE);
  test_utils::execute_query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT)
                                         % test_utils::SIMPLE_KEYSPACE % 2));
  test_utils::execute_query(session, use_simple);
  test_utils::execute_query(session, "USE system");
  test_utils::execute_query(session, str(boost::format("DROP KEYSPACE %s") % test_utils::SIMPLE_KEYSPACE));
  test_utils::CassResultPtr result;
  CassError rc = test_utils::execute_query_with_error(session, use_simple, &result);
  BOOST_CHECK_EQUAL(rc, CASS_ERROR_SERVER_INVALID_QUERY);

  // close session to flush logger
  close_session();

  BOOST_CHECK_EQUAL(test_utils::CassLog::message_count(), 2ul);
}

BOOST_AUTO_TEST_CASE(agreement_node_down) {
  test_utils::CassLog::reset("ControlConnection: Node " + inst.conf.ip_prefix() + "3 is down");

  inst.ccm->stop(3);

  size_t t = 0;
  size_t max_tries = 15;
  for (; t < max_tries; ++t) {
    boost::this_thread::sleep_for(boost::chrono::seconds(1));
    if (test_utils::CassLog::message_count() > 0) break;
  }
  BOOST_REQUIRE_MESSAGE(t < max_tries, "Timed out waiting for node down log message");

  test_utils::CassLog::reset("Found schema agreement in");
  const std::string use_simple = str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE);
  test_utils::execute_query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT)
                                         % test_utils::SIMPLE_KEYSPACE % 2));
  test_utils::execute_query(session, use_simple);
  test_utils::execute_query(session, "USE system");
  test_utils::execute_query(session, str(boost::format("DROP KEYSPACE %s") % test_utils::SIMPLE_KEYSPACE));
  test_utils::CassResultPtr result;
  CassError rc = test_utils::execute_query_with_error(session, use_simple, &result);
  BOOST_CHECK_EQUAL(rc, CASS_ERROR_SERVER_INVALID_QUERY);

  // close session to flush logger
  close_session();

  BOOST_CHECK_EQUAL(test_utils::CassLog::message_count(), 2ul);

  inst.ccm->start(3);
}

#define MAX_SCHEMA_AGREEMENT_WAIT_MS 10000
BOOST_AUTO_TEST_CASE(no_agreement_timeout) {
  std::string update_peer("UPDATE system.peers SET schema_version=? WHERE peer='" + inst.conf.ip_prefix() + "1'");
  test_utils::CassFuturePtr prepared_future(
        cass_session_prepare(session, cass_string_init2(update_peer.data(), update_peer.size())));
  test_utils::wait_and_check_error(prepared_future.get());
  test_utils::CassPreparedPtr prep = cass_future_get_prepared(prepared_future.get());
  test_utils::CassStatementPtr schema_stmt(cass_prepared_bind(prep.get()));

  test_utils::CassLog::reset("SchemaChangeHandler: No schema agreement on live nodes after ");
  test_utils::CassStatementPtr create_stmt =
      cass_statement_new(
        cass_string_init(str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT)
                             % test_utils::SIMPLE_KEYSPACE % 2).c_str()), 0);
  test_utils::CassFuturePtr create_future(cass_session_execute(session, create_stmt.get()));

  boost::chrono::steady_clock::time_point end =
      boost::chrono::steady_clock::now() + boost::chrono::milliseconds(MAX_SCHEMA_AGREEMENT_WAIT_MS + 1000);
  // mess with system.peers for more than the wait time
  // this is hitting more than the required node, but should cycle around to the one being queried enough
  do {
    cass_statement_bind_uuid(schema_stmt.get(), 0, test_utils::generate_random_uuid());

    test_utils::CassFuturePtr future(cass_session_execute(session, schema_stmt.get()));
    cass_future_wait(future.get());
    BOOST_REQUIRE_EQUAL(cass_future_error_code(future.get()), CASS_OK);
  } while(boost::chrono::steady_clock::now() < end && test_utils::CassLog::message_count() == 0);

  cass_future_wait(create_future.get());
  BOOST_CHECK_EQUAL(cass_future_error_code(create_future.get()), CASS_OK);
  close_session();

  BOOST_CHECK_EQUAL(test_utils::CassLog::message_count(), 1ul);
}

BOOST_AUTO_TEST_SUITE_END()
