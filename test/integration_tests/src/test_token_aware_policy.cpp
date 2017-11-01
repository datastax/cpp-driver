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

#include <algorithm>

#if !defined(WIN32) && !defined(_WIN32)
#include <signal.h>
#endif

#include "cassandra.h"
#include "testing.hpp"
#include "test_utils.hpp"

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/cstdint.hpp>
#include <boost/format.hpp>
#include <boost/asio/io_service.hpp>
#include <boost/thread/future.hpp>
#include <boost/bind.hpp>
#include <boost/chrono.hpp>
#include <boost/cstdint.hpp>

#include <map>
#include <set>
#include <string>

BOOST_AUTO_TEST_SUITE(token_aware_policy)

struct TestTokenMap {
  struct Host {
    Host() {}
    Host(const std::string& ip, const std::string& dc)
      : ip(ip), dc(dc) {}
    std::string ip;
    std::string dc;
  };

  typedef std::map<int64_t, Host> TokenHostMap;
  typedef std::set<std::string> ReplicaSet;

  TokenHostMap tokens;

  void build(const std::string& ip_prefix, int num_nodes) {
    test_utils::CassClusterPtr cluster(cass_cluster_new());
    test_utils::initialize_contact_points(cluster.get(), ip_prefix, num_nodes);
    cass_cluster_set_load_balance_round_robin(cluster.get());
    cass_cluster_set_token_aware_routing(cluster.get(), cass_false);

    test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

    for (int i = 0; i < num_nodes; ++i) {
      test_utils::CassStatementPtr statement(
            cass_statement_new("SELECT tokens, data_center FROM system.local", 0));
      test_utils::CassFuturePtr future(cass_session_execute(session.get(), statement.get()));
      test_utils::wait_and_check_error(future.get());
      test_utils::CassResultPtr result(cass_future_get_result(future.get()));
      const CassRow* row = cass_result_first_row(result.get());
      const CassValue* data_center = cass_row_get_column_by_name(row, "data_center");
      const CassValue* token_set = cass_row_get_column_by_name(row, "tokens");

      CassString str;
      cass_value_get_string(data_center, &str.data, &str.length);
      std::string dc(str.data, str.length);

      std::string ip = cass::get_host_from_future(future.get());
      test_utils::CassIteratorPtr iterator(cass_iterator_from_collection(token_set));
      while (cass_iterator_next(iterator.get())) {
        cass_value_get_string(cass_iterator_get_value(iterator.get()), &str.data, &str.length);
        std::string token(str.data, str.length);
        tokens[boost::lexical_cast<int64_t>(token)] = Host(ip, dc);
      }
    }
  }

  ReplicaSet get_expected_replicas(size_t rf, const std::string& value, const std::string& local_dc = "") {
    ReplicaSet replicas;
    TokenHostMap::iterator i = tokens.upper_bound(cass::create_murmur3_hash_from_string(value));
    while  (replicas.size() < rf) {
      if (local_dc.empty() || local_dc == i->second.dc) {
        replicas.insert(i->second.ip);
      }
      ++i;
      if (i == tokens.end()) {
        i = tokens.begin();
      }
    }
    return replicas;
  }
};

std::string get_replica(test_utils::CassSessionPtr session,
                        const std::string& keyspace,
                        const std::string& value) {
  // The query doesn't matter
  test_utils::CassStatementPtr statement(
        cass_statement_new("SELECT * FROM system.local", 1));
  cass_statement_set_consistency(statement.get(), CASS_CONSISTENCY_ONE);
  cass_statement_bind_string_n(statement.get(), 0, value.data(), value.size());
  cass_statement_add_key_index(statement.get(), 0);
  cass_statement_set_keyspace(statement.get(), keyspace.c_str());
  test_utils::CassFuturePtr future(
        cass_session_execute(session.get(), statement.get()));
  return cass::get_host_from_future(future.get());
}

TestTokenMap::ReplicaSet get_replicas(size_t rf,
                                      test_utils::CassSessionPtr session,
                                      const std::string& keyspace,
                                      const std::string& value) {
  TestTokenMap::ReplicaSet replicas;
  for (size_t i = 0; i < rf * rf; ++i) {
    replicas.insert(get_replica(session, keyspace, "abc"));
    if (replicas.size() == rf) break;
  }
  return replicas;
}

BOOST_AUTO_TEST_CASE(simple)
{
  const size_t rf = 2;
  const std::string value = "abc";

  test_utils::CassClusterPtr cluster(cass_cluster_new());

  boost::shared_ptr<CCM::Bridge> ccm(new CCM::Bridge("config.txt"));
  if (ccm->create_cluster(rf)) {
    ccm->start_cluster();
  }

  cass_cluster_set_load_balance_round_robin(cluster.get());
  cass_cluster_set_use_schema(cluster.get(), cass_false);
  cass_cluster_set_token_aware_routing(cluster.get(), cass_true);

  std::string ip_prefix = ccm->get_ip_prefix();
  test_utils::initialize_contact_points(cluster.get(), ip_prefix, 1);

  test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

  std::string keyspace = "ks";

  test_utils::execute_query(session.get(),
                            str(boost::format("CREATE KEYSPACE %s "
                                              "WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': %d }") %
                                keyspace % rf));

  for (size_t i = 0; i < rf; ++i)
  {
    TestTokenMap token_map;
    token_map.build(ip_prefix, rf);

    TestTokenMap::ReplicaSet replicas = get_replicas(rf, session, keyspace, value);
    BOOST_REQUIRE(replicas.size() == rf - i);
    TestTokenMap::ReplicaSet expected_replicas = token_map.get_expected_replicas(rf - i, value);
    BOOST_CHECK(replicas == expected_replicas);

    if (i + 1 == rf) break;

    ccm->stop_node(i + 1);
  }

  // Drop the keyspace (ignore any and all errors)
  test_utils::execute_query_with_error(session.get(),
    str(boost::format(test_utils::DROP_KEYSPACE_FORMAT)
    % keyspace));
}

bool intersects(const TestTokenMap::ReplicaSet& set1, const TestTokenMap::ReplicaSet& set2) {
  TestTokenMap::ReplicaSet intersection;
  std::set_intersection(set1.begin(), set1.end(),
                        set2.begin(), set2.end(),
                        std::inserter(intersection, intersection.begin()));
  return intersection.size() > 0;
}

BOOST_AUTO_TEST_CASE(network_topology)
{
  const size_t rf = 2;
  const std::string value = "abc";

  test_utils::CassClusterPtr cluster(cass_cluster_new());

  boost::shared_ptr<CCM::Bridge> ccm(new CCM::Bridge("config.txt"));
  if (ccm->create_cluster(rf, rf)) {
    ccm->start_cluster();
  }

  cass_cluster_set_load_balance_dc_aware(cluster.get(), "dc1", rf, cass_false);
  cass_cluster_set_use_schema(cluster.get(), cass_false);
  cass_cluster_set_token_aware_routing(cluster.get(), cass_true);

  std::string ip_prefix = ccm->get_ip_prefix();
  test_utils::initialize_contact_points(cluster.get(), ip_prefix, 1);

  test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

  std::string keyspace = "ks";

  test_utils::execute_query(session.get(),
                            str(boost::format("CREATE KEYSPACE %s "
                                              "WITH replication = { 'class': 'NetworkTopologyStrategy', 'dc1': %d , 'dc2': %d }") %
                                keyspace % rf % rf));

  TestTokenMap token_map;
  token_map.build(ip_prefix, 2 * rf);

  // Using local nodes
  TestTokenMap::ReplicaSet replicas = get_replicas(rf, session, keyspace, value);
  BOOST_REQUIRE(replicas.size() == rf);
  TestTokenMap::ReplicaSet local_replicas = token_map.get_expected_replicas(rf, value, "dc1");
  BOOST_CHECK(replicas == local_replicas);

  // Still using local nodes
  ccm->stop_node(1);
  replicas = get_replicas(rf, session, keyspace, value);
  BOOST_CHECK(replicas.size() == 1 && local_replicas.count(*replicas.begin()) > 0);

  // Using remote nodes
  ccm->stop_node(2);
  replicas = get_replicas(rf, session, keyspace, value);
  BOOST_CHECK(replicas.size() > 0 && !intersects(replicas, local_replicas));

  // Using last of the remote nodes
  ccm->stop_node(3);
  replicas = get_replicas(rf, session, keyspace, value);
  BOOST_CHECK(replicas.size() > 0 && !intersects(replicas, local_replicas));

  // Drop the keyspace (ignore any and all errors)
  test_utils::execute_query_with_error(session.get(),
    str(boost::format(test_utils::DROP_KEYSPACE_FORMAT)
    % keyspace));
}

/**
 * Invalid Key Index: Single Entry for Token-Aware Routing Key
 *
 * This test addresses an issue where single entry routing keys caused a driver
 * error when values were empty on insert.
 *
 * @since 1.0.1
 * @jira_ticket CPP-214
 * @test_category load_balancing:token_aware
 * @test_subcategory collections
 */
BOOST_AUTO_TEST_CASE(single_entry_routing_key)
{
  const size_t rf = 2;
  test_utils::CassClusterPtr cluster(cass_cluster_new());

  boost::shared_ptr<CCM::Bridge> ccm(new CCM::Bridge("config.txt"));
  if (ccm->create_cluster(rf, rf)) {
    ccm->start_cluster();
  }

  cass_cluster_set_load_balance_dc_aware(cluster.get(), "dc1", rf, cass_false);
  cass_cluster_set_use_schema(cluster.get(), cass_false);
  cass_cluster_set_token_aware_routing(cluster.get(), cass_true);

  test_utils::initialize_contact_points(cluster.get(), ccm->get_ip_prefix(), 1);

  test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

  std::string keyspace = "ks";
  test_utils::execute_query(session.get(),
                            str(boost::format("CREATE KEYSPACE %s "
                                              "WITH replication = { 'class': 'NetworkTopologyStrategy', 'dc1': %d , 'dc2': %d }") %
                                keyspace % rf % rf));
  test_utils::execute_query(session.get(), str(boost::format("USE %s") % keyspace));
  test_utils::execute_query(session.get(), "CREATE TABLE invalid_routing_key (routing_key text PRIMARY KEY,"
                                            "cass_collection map<text,text>);");

  std::string insert_query = "UPDATE invalid_routing_key SET cass_collection = ? WHERE routing_key = ?";
  test_utils::CassFuturePtr prepared_future(cass_session_prepare_n(session.get(),
                                                                   insert_query.data(), insert_query.size()));
  test_utils::wait_and_check_error(prepared_future.get());
  test_utils::CassPreparedPtr prepared(cass_future_get_prepared(prepared_future.get()));

  test_utils::CassStatementPtr statement(cass_prepared_bind(prepared.get()));

  test_utils::CassCollectionPtr collection(cass_collection_new(CASS_COLLECTION_TYPE_MAP, 0));
  cass_statement_bind_collection(statement.get(), 0, collection.get());
  cass_statement_bind_string(statement.get(), 1, "cassandra cpp-driver");

  test_utils::CassFuturePtr future(cass_session_execute(session.get(), statement.get()));

  test_utils::wait_and_check_error(future.get());

  // Drop the keyspace (ignore any and all errors)
  test_utils::execute_query_with_error(session.get(),
    str(boost::format(test_utils::DROP_KEYSPACE_FORMAT)
    % keyspace));
}

/**
 * Ensure the control connection is decoupled from request timeout
 *
 * This test addresses an issue where the control connection would timeout due
 * to the rebuilding of the token map, re-establish the connection, re-build
 * the token map and then rinse and repeat causing high CPU load and an
 * infinite loop.
 *
 * @since 2.4.3
 * @jira_ticket CPP-388
 * @test_category load_balancing:token_aware
 * @test_category control_connection
 */
BOOST_AUTO_TEST_CASE(no_timeout_control_connection)
{
  int num_of_keyspaces = 50;
  int num_of_tables = 10;
  std::string keyspace_prefix = "tap_";
  std::string table_prefix = "table_";
  test_utils::CassLog::reset("Request timed out");

  // Create four data centers with single nodes
  std::vector<unsigned short> data_center_nodes;
  for (int i = 0; i < 4; ++i) {
    data_center_nodes.push_back(1);
  }

  boost::shared_ptr<CCM::Bridge> ccm(new CCM::Bridge("config.txt"));
  if (ccm->create_cluster(data_center_nodes, true)) {
    ccm->start_cluster();
  }

  // Create a session with a quick request timeout
  test_utils::CassClusterPtr cluster(cass_cluster_new());
  cass_cluster_set_token_aware_routing(cluster.get(), cass_true);
  test_utils::initialize_contact_points(cluster.get(), ccm->get_ip_prefix(), 4);
  cass_cluster_set_request_timeout(cluster.get(), 500);
  test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

  // Create keyspaces, tables, and perform selects)
  for (int i = 1; i <= num_of_keyspaces; ++i) {
    // Randomly create keyspaces with valid and invalid data centers)
    bool is_valid_keyspace = true;
    std::string nts_dcs = "'dc1': 1, 'dc2': 1, 'dc3': 1, 'dc4': 1";
    if ((rand() % 4) == 0) {
      // Create the invalid data center network topology
      int unknown_dcs = (rand() % 250) + 50; // random number [50 - 250]
      for (int j = 5; j <= 4 + unknown_dcs; ++j) {
        nts_dcs += ", 'dc" + boost::lexical_cast<std::string>(j) + "': 1";
      }
      is_valid_keyspace = false;
    }

    // Create the keyspace (handling errors to avoid test failure))
    CassError error_code;
    do {
      error_code = test_utils::execute_query_with_error(session.get(),
        str(boost::format("CREATE KEYSPACE " + keyspace_prefix + "%d "
        "WITH replication = { 'class': 'NetworkTopologyStrategy', "
        + nts_dcs + " }") % i));
    } while (error_code != CASS_OK && error_code != CASS_ERROR_SERVER_ALREADY_EXISTS);

    // Perform table creation and random selects (iff keyspace is valid))
    if (is_valid_keyspace) {
      // Create the table (handling errors to avoid test failures)
      for (int j = 0; j < num_of_tables; ++j) {
        std::stringstream full_table_name;
        full_table_name << keyspace_prefix << i << "."
          << table_prefix << j;
        CassError error_code = CASS_ERROR_SERVER_READ_TIMEOUT;
        do {
          error_code = test_utils::execute_query_with_error(session.get(),
          str(boost::format(test_utils::CREATE_TABLE_SIMPLE) %
            full_table_name.str()));
        } while (error_code != CASS_OK && error_code != CASS_ERROR_SERVER_ALREADY_EXISTS);

        // Randomly perform select statements on the newly created table
        if (rand() % 2 == 0) {
          std::string query = "SELECT * FROM " + full_table_name.str();
          do {
            error_code = test_utils::execute_query_with_error(session.get(), query.c_str());
          } while (error_code != CASS_OK);
        }
      }
    }
  }

  /*
   * Ensure timeouts occurred
   *
   * NOTE: This also ensures (if reached) that infinite loop did not occur
   */
  BOOST_REQUIRE_GT(test_utils::CassLog::message_count(), 0);
}

BOOST_AUTO_TEST_SUITE_END()
