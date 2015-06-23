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

#if !defined(WIN32) && !defined(_WIN32)
#include <signal.h>
#endif

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/cstdint.hpp>
#include <boost/format.hpp>
#include <boost/asio.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/future.hpp>
#include <boost/bind.hpp>
#include <boost/chrono.hpp>
#include <boost/atomic.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int_distribution.hpp>

#include "cassandra.h"
#include "test_utils.hpp"
#include "cql_ccm_bridge.hpp"

#define TEST_DURATION_SECS 300 // 5 minutes
#define NUM_NODES 3

bool execute_insert(CassSession* session, const std::string& table_name);

struct OutageTests : public test_utils::MultipleNodesTest {
  enum NodeState {
    UP,
    DOWN,
    REMOVED,
    GOSSIP_DISABLED,
    BINARY_DISABLED
  };

  OutageTests()
    : test_utils::MultipleNodesTest(NUM_NODES, 0)
    , is_done(false)
    , timer(io_service) {
    test_utils::CassLog::set_output_log_level(CASS_LOG_DEBUG);
    printf("Warning! This test is going to take %d minutes\n", TEST_DURATION_SECS / 60);
    std::fill(nodes_states, nodes_states + NUM_NODES, UP);
    // TODO(mpenick): This is a stopgap. To be fixed in CPP-140
#if !defined(WIN32) && !defined(_WIN32)
    signal(SIGPIPE, SIG_IGN);
#endif
  }

  int random_int(int s, int e) {
    boost::random::uniform_int_distribution<> dist(s, e);
    return dist(rng);
  }

  bool client_thread(CassSession* session, const std::string table_name) {
    std::string query = str(boost::format("SELECT * FROM %s LIMIT 10000") % table_name);

    for (int i = 0 ; i < 10; ++i)  execute_insert(session, table_name);

    boost::posix_time::ptime start = boost::posix_time::second_clock::universal_time();

    test_utils::CassStatementPtr statement(cass_statement_new(query.c_str(), 0));
    cass_statement_set_consistency(statement.get(), CASS_CONSISTENCY_ONE);

    while ((boost::posix_time::second_clock::universal_time() - start).total_seconds() < TEST_DURATION_SECS) {
      test_utils::CassFuturePtr future(cass_session_execute(session, statement.get()));
      cass_future_wait(future.get());

      CassError code = cass_future_error_code(future.get());
      if (code != CASS_OK
         && code != CASS_ERROR_LIB_REQUEST_TIMED_OUT
         && code != CASS_ERROR_SERVER_READ_TIMEOUT) { // Timeout is okay
        CassString message;
        cass_future_error_message(future.get(), &message.data, &message.length);
        fprintf(stderr, "Error occurred during select '%.*s'\n", static_cast<int>(message.length), message.data);
        is_done = true;
        return false;
      }

      if (code == CASS_OK) {
        test_utils::CassResultPtr result(cass_future_get_result(future.get()));
        if (cass_result_row_count(result.get()) == 0) {
          fprintf(stderr, "No rows returned from query\n");
          is_done = true;
          return false;
        }
      }
    }

    is_done = true;
    return true;
  }

  void outage_thread() {
    io_service.run();
  }

  void handle_timeout(const boost::system::error_code& error) {
    if (is_done) {
      timer.cancel();
      return;
    }

    int num_up = std::count(nodes_states, nodes_states + NUM_NODES, UP);

    if (num_up > 1 && random_int(1, 100) <= 75) {
      int n = random_int(1, num_up);
      for (size_t i = 0; i < NUM_NODES; ++i) {
        if (nodes_states[i] == UP) {
          bool disableBinaryGossip = (n == random_int(1, num_up));
          if (--n == 0) {
            if (disableBinaryGossip) {
              if (random_int(1, 100) <= 50) {
                ccm->binary(i, false);
                nodes_states[i] = BINARY_DISABLED;
              } else {
                ccm->gossip(i, false);
                nodes_states[i] = GOSSIP_DISABLED;
              }
            } else if (random_int(1, 100) <= 50) {
              ccm->decommission(i);
              nodes_states[i] = REMOVED;
              ccm->stop(i);
            } else {
              nodes_states[i] = DOWN;
              ccm->stop(i);
            }
            break;
          }
        }
      }
    } else if (NUM_NODES - num_up > 0){
      int n = random_int(1, NUM_NODES - num_up);
      for (size_t i = 0; i < NUM_NODES; ++i) {
        if (nodes_states[i] == DOWN || nodes_states[i] == REMOVED) {
          if (--n == 0) {
            nodes_states[i] = UP;
            ccm->start(i);
            break;
          }
        } else if (nodes_states[i] == GOSSIP_DISABLED) {
           nodes_states[i] = UP;
           ccm->gossip(i, true);
           break;
        } else if (nodes_states[i] == BINARY_DISABLED) {
           nodes_states[i] = UP;
           ccm->binary(i, true);
           break;
        }
      }
    }

    timer.expires_from_now(boost::posix_time::seconds(random_int(10, 30)));
    timer.async_wait(boost::bind(&OutageTests::handle_timeout, this, _1));
  }

  bool execute_insert(CassSession* session, const std::string& table_name) {
    std::string query = str(boost::format("INSERT INTO %s (id, event_time, text_sample) VALUES (?, ?, ?)") % table_name);

    test_utils::CassStatementPtr statement(cass_statement_new_n(query.data(), query.size(), 3));

    boost::chrono::system_clock::time_point now(boost::chrono::system_clock::now());
    boost::chrono::milliseconds event_time(boost::chrono::duration_cast<boost::chrono::milliseconds>(now.time_since_epoch()));
    std::string text_sample(test_utils::string_from_time_point(now));

    cass_statement_bind_uuid(statement.get(), 0, test_utils::generate_time_uuid(uuid_gen));
    cass_statement_bind_int64(statement.get(), 1, event_time.count());
    cass_statement_bind_string_n(statement.get(), 2, text_sample.data(), text_sample.size());

    test_utils::CassFuturePtr future(cass_session_execute(session, statement.get()));
    cass_future_wait(future.get());
    CassError code = cass_future_error_code(future.get());
    if (code != CASS_OK && code != CASS_ERROR_LIB_REQUEST_TIMED_OUT) { // Timeout is okay
      CassString message;
      cass_future_error_message(future.get(), &message.data, &message.length);
      fprintf(stderr, "Error occurred during insert '%.*s'\n", static_cast<int>(message.length), message.data);
      return false;
    }

    return true;
  }

  NodeState nodes_states[NUM_NODES];
  boost::atomic<bool> is_done;
  boost::asio::io_service io_service;
  boost::asio::deadline_timer timer;
  boost::mt19937 rng;
};


BOOST_FIXTURE_TEST_SUITE(outage, OutageTests)

BOOST_AUTO_TEST_CASE(test)
{
  test_utils::CassSessionPtr session(test_utils::create_session(cluster));
  test_utils::execute_query(session.get(), "CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};");
  test_utils::execute_query(session.get(), "USE test;");

  std::string table_name = str(boost::format("table_%s") % test_utils::generate_unique_str(uuid_gen));

  test_utils::execute_query(session.get(), str(boost::format(test_utils::CREATE_TABLE_TIME_SERIES) % table_name));

  boost::unique_future<bool> client_future
      = boost::async(boost::launch::async, boost::bind(&OutageTests::client_thread, this, session.get(), table_name));

  timer.expires_from_now(boost::posix_time::seconds(2));
  timer.async_wait(boost::bind(&OutageTests::handle_timeout, this, _1));

  boost::unique_future<void> outage_future
      = boost::async(boost::launch::async, boost::bind(&OutageTests::outage_thread, this));

  BOOST_CHECK(client_future.get());
  io_service.stop();
  outage_future.get();
}

BOOST_AUTO_TEST_SUITE_END()

