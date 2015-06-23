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

#include "cassandra.h"
#include "testing.hpp"
#include "test_utils.hpp"
#include "cql_ccm_bridge.hpp"

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
    test_utils::initialize_contact_points(cluster.get(), ip_prefix, num_nodes, 0);
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
  for (size_t i = 0; i < rf; ++i) {
    replicas.insert(get_replica(session, keyspace, "abc"));
  }
  return replicas;
}

BOOST_AUTO_TEST_CASE(simple)
{
  const size_t rf = 2;
  const std::string value = "abc";

  test_utils::CassClusterPtr cluster(cass_cluster_new());

  const cql::cql_ccm_bridge_configuration_t& conf = cql::get_ccm_bridge_configuration();
  boost::shared_ptr<cql::cql_ccm_bridge_t> ccm = cql::cql_ccm_bridge_t::create_and_start(conf, "test", rf);

  cass_cluster_set_load_balance_round_robin(cluster.get());
  cass_cluster_set_token_aware_routing(cluster.get(), cass_true);

  test_utils::initialize_contact_points(cluster.get(), conf.ip_prefix(), 1, 0);

  test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

  std::string keyspace = "ks";

  test_utils::execute_query(session.get(),
                            str(boost::format("CREATE KEYSPACE %s "
                                              "WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': %d }") %
                                keyspace % rf));

  for (size_t i = 0; i < rf; ++i)
  {
    TestTokenMap token_map;
    token_map.build(conf.ip_prefix(), rf);

    TestTokenMap::ReplicaSet replicas = get_replicas(rf, session, keyspace, value);
    BOOST_REQUIRE(replicas.size() == rf - i);
    TestTokenMap::ReplicaSet expected_replicas = token_map.get_expected_replicas(rf - i, value);
    BOOST_CHECK(replicas == expected_replicas);

    if (i + 1 == rf) break;

    ccm->stop(i + 1);
  }
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

  const cql::cql_ccm_bridge_configuration_t& conf = cql::get_ccm_bridge_configuration();
  boost::shared_ptr<cql::cql_ccm_bridge_t> ccm = cql::cql_ccm_bridge_t::create_and_start(conf, "test", rf, rf);

  cass_cluster_set_load_balance_dc_aware(cluster.get(), "dc1", rf, cass_false);
  cass_cluster_set_token_aware_routing(cluster.get(), cass_true);

  test_utils::initialize_contact_points(cluster.get(), conf.ip_prefix(), 1, 0);

  test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

  std::string keyspace = "ks";

  test_utils::execute_query(session.get(),
                            str(boost::format("CREATE KEYSPACE %s "
                                              "WITH replication = { 'class': 'NetworkTopologyStrategy', 'dc1': %d , 'dc2': %d }") %
                                keyspace % rf % rf));

  TestTokenMap token_map;
  token_map.build(conf.ip_prefix(), 2 * rf);

  // Using local nodes
  TestTokenMap::ReplicaSet replicas = get_replicas(rf, session, keyspace, value);
  BOOST_REQUIRE(replicas.size() == rf);
  TestTokenMap::ReplicaSet local_replicas = token_map.get_expected_replicas(rf, value, "dc1");
  BOOST_CHECK(replicas == local_replicas);

  // Still using local nodes
  ccm->stop(1);
  replicas = get_replicas(rf, session, keyspace, value);
  BOOST_CHECK(replicas.size() == 1 && local_replicas.count(*replicas.begin()) > 0);

  // Using remote nodes
  ccm->stop(2);
  replicas = get_replicas(rf, session, keyspace, value);
  BOOST_CHECK(replicas.size() > 0 && !intersects(replicas, local_replicas));

  // Using last of the remote nodes
  ccm->stop(3);
  replicas = get_replicas(rf, session, keyspace, value);
  BOOST_CHECK(replicas.size() > 0 && !intersects(replicas, local_replicas));
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

  const cql::cql_ccm_bridge_configuration_t& conf = cql::get_ccm_bridge_configuration();
  boost::shared_ptr<cql::cql_ccm_bridge_t> ccm = cql::cql_ccm_bridge_t::create_and_start(conf, "test", rf, rf);

  cass_cluster_set_load_balance_dc_aware(cluster.get(), "dc1", rf, cass_false);
  cass_cluster_set_token_aware_routing(cluster.get(), cass_true);

  test_utils::initialize_contact_points(cluster.get(), conf.ip_prefix(), 1, 0);

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
  
  CassCollection* collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, 0);
  cass_statement_bind_collection(statement.get(), 0, collection);
  cass_statement_bind_string(statement.get(), 1, "cassandra cpp-driver");

  test_utils::CassFuturePtr future(cass_session_execute(session.get(), statement.get()));

  test_utils::wait_and_check_error(future.get());
}

BOOST_AUTO_TEST_SUITE_END()
