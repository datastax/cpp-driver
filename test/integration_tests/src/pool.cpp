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
#include "cluster.hpp"

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_SUITE(pool)

BOOST_AUTO_TEST_CASE(pending_request_timeout)
{
  const size_t TIME_THRESHOLD_MS = 3;
  const size_t CONNECT_TIMEOUT_MS = 50;

  test_utils::MultipleNodesTest inst(1, 0);
  cass_cluster_set_log_level(inst.cluster, CASS_LOG_DEBUG);
  cass_cluster_set_connect_timeout(inst.cluster, CONNECT_TIMEOUT_MS);
  cass_cluster_set_pending_requests_high_water_mark(inst.cluster, 1);
  cass_cluster_set_pending_requests_low_water_mark(inst.cluster, 1);
  cass_cluster_set_num_threads_io(inst.cluster, 1);
  reinterpret_cast<cass::Cluster*>(inst.cluster)->config().set_core_connections_per_host(0);

  test_utils::CassFuturePtr connect_future(cass_cluster_connect(inst.cluster));
  test_utils::wait_and_check_error(connect_future.get());

  test_utils::CassSessionPtr session = cass_future_get_session(connect_future.get());

  test_utils::CassStatementPtr statement(cass_statement_new(cass_string_init("SELECT * FROM system.local"), 0));

  boost::chrono::steady_clock::time_point start = boost::chrono::steady_clock::now();
  //+ boost::chrono::milliseconds(MAX_SCHEMA_AGREEMENT_WAIT_MS + 1000);
  test_utils::CassFuturePtr future_pend(cass_session_execute(session.get(), statement.get()));
  test_utils::CassFuturePtr future_reject(cass_session_execute(session.get(), statement.get()));

  // reject should come almost immediately
  CassError code_reject = test_utils::wait_and_return_error(future_reject.get());
  boost::chrono::steady_clock::time_point reject_time = boost::chrono::steady_clock::now();
  // pend should come after connect timeout
  CassError code_pend = test_utils::wait_and_return_error(future_pend.get());
  boost::chrono::steady_clock::time_point pend_fail_time = boost::chrono::steady_clock::now();

  boost::chrono::milliseconds reject_ms = boost::chrono::duration_cast<boost::chrono::milliseconds>(reject_time - start);
  boost::chrono::milliseconds pend_ms = boost::chrono::duration_cast<boost::chrono::milliseconds>(pend_fail_time - start);

  BOOST_CHECK_LT(reject_ms.count(), TIME_THRESHOLD_MS);
  BOOST_CHECK_GE(pend_ms.count(), CONNECT_TIMEOUT_MS);
  BOOST_CHECK_LT(pend_ms.count(), CONNECT_TIMEOUT_MS + TIME_THRESHOLD_MS);
  BOOST_CHECK_EQUAL(code_pend, CASS_ERROR_LIB_NO_HOSTS_AVAILABLE);
  BOOST_CHECK_EQUAL(code_reject, CASS_ERROR_LIB_NO_HOSTS_AVAILABLE);
}

BOOST_AUTO_TEST_SUITE_END()
