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

#include "event_loop_test.hpp"
#include "query_request.hpp"
#include "session.hpp"

#define KEYSPACE "datastax"
#define NUM_THREADS 2 // Number of threads to execute queries using a session
#define OUTAGE_PLAN_DELAY 250 // Reduced delay to incorporate larger outage plan

class SessionUnitTest : public EventLoopTest {
public:
  SessionUnitTest()
    : EventLoopTest("SessionUnitTest") { }

  void populate_outage_plan(OutagePlan* outage_plan) {
    // Multiple rolling restarts
    for (int i = 1; i <= 9; ++i) {
      int node = i % 3;
      outage_plan->stop_node(node, OUTAGE_PLAN_DELAY);
      outage_plan->start_node(node, OUTAGE_PLAN_DELAY);
    }

    // Add/Remove entries from the "system" tables
    outage_plan->remove_node(2, OUTAGE_PLAN_DELAY);
    outage_plan->stop_node(1, OUTAGE_PLAN_DELAY);
    outage_plan->add_node(2, OUTAGE_PLAN_DELAY);
    outage_plan->start_node(1, OUTAGE_PLAN_DELAY);
    outage_plan->stop_node(3, OUTAGE_PLAN_DELAY);
    outage_plan->stop_node(1, OUTAGE_PLAN_DELAY);
  }

  void query_on_threads(cass::Session* session) {
    uv_thread_t threads[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; ++i) {
      ASSERT_EQ(0, uv_thread_create(&threads[i], query, session));
    }
    for (int i = 0; i < NUM_THREADS; ++i) {
      uv_thread_join(&threads[i]);
    }
  }

  static void connect(cass::Session* session,
                      cass::SslContext* ssl_context = NULL,
                      uint64_t wait_for_time_us = WAIT_FOR_TIME) {
    cass::Config config;
    config.set_reconnect_wait_time(100); // Faster reconnect time to handle cluster starts and stops
    config.contact_points().push_back("127.0.0.1");
    config.contact_points().push_back("127.0.0.2"); // Handle three node clusters (for chaotic scenarios)
    config.contact_points().push_back("127.0.0.3");
    if (ssl_context) {
      config.set_ssl_context(ssl_context);
    }
    cass::Future::Ptr connect_future(session->connect(config));
    ASSERT_TRUE(connect_future->wait_for(wait_for_time_us)) << "Timed out waiting for session to connect";
    ASSERT_FALSE(connect_future->error())
      << cass_error_desc(connect_future->error()->code) << ": "
      << connect_future->error()->message;
  }

  static void close(cass::Session* session,
                    uint64_t wait_for_time_us = WAIT_FOR_TIME) {
    cass::Future::Ptr close_future(session->close());
    ASSERT_TRUE(close_future->wait_for(wait_for_time_us)) << "Timed out waiting for session to close";
    ASSERT_FALSE(close_future->error())
      << cass_error_desc(close_future->error()->code) << ": "
      << close_future->error()->message;
  }

  static void query(cass::Session* session) {
    cass::QueryRequest::Ptr request(cass::Memory::allocate<cass::QueryRequest>("blah", 0));
    request->set_is_idempotent(true);

    cass::Future::Ptr future = session->execute(request, NULL);
    ASSERT_TRUE(future->wait_for(WAIT_FOR_TIME)) << "Timed out executing query";
    ASSERT_FALSE(future->error())
      << cass_error_desc(future->error()->code) << ": "
      << future->error()->message;
  }

  // uv_thread_create
  static void query(void* arg) {
    cass::Session* session = static_cast<cass::Session*>(arg);
    query(session);
  }
};

TEST_F(SessionUnitTest, ExecuteQueryNotConnected) {
  cass::QueryRequest::Ptr request(cass::Memory::allocate<cass::QueryRequest>("blah", 0));

  cass::Session session;
  cass::Future::Ptr future = session.execute(request, NULL);
  ASSERT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, future->error()->code);
}

TEST_F(SessionUnitTest, InvalidKeyspace) {
  mockssandra::SimpleRequestHandlerBuilder builder;
  builder.on(mockssandra::OPCODE_QUERY)
    .system_local()
    .system_peers()
    .use_keyspace("blah")
    .empty_rows_result(1);
  mockssandra::SimpleCluster cluster(builder.build());
  ASSERT_EQ(cluster.start_all(), 0);

  cass::Config config;
  config.contact_points().push_back("127.0.0.1");
  cass::Session session;

  cass::Future::Ptr connect_future(session.connect(config, "invalid"));
  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  ASSERT_EQ(CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE, connect_future->error()->code);

  ASSERT_TRUE(session.close()->wait_for(WAIT_FOR_TIME));
}

TEST_F(SessionUnitTest, InvalidDataCenter) {
  mockssandra::SimpleCluster cluster(simple());
  ASSERT_EQ(cluster.start_all(), 0);

  cass::Config config;
  config.contact_points().push_back("127.0.0.1");
  config.set_load_balancing_policy(cass::Memory::allocate<cass::DCAwarePolicy>(
                                     "invalid_data_center",
                                     0,
                                     false));
  cass::Session session;

  cass::Future::Ptr connect_future(session.connect(config));
  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  ASSERT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, connect_future->error()->code);

  ASSERT_TRUE(session.close()->wait_for(WAIT_FOR_TIME));
}


TEST_F(SessionUnitTest, InvalidLocalAddress) {
  mockssandra::SimpleCluster cluster(simple());
  ASSERT_EQ(cluster.start_all(), 0);

  cass::Config config;
  config.set_local_address(Address("1.1.1.1", PORT)); // Invalid
  config.contact_points().push_back("127.0.0.1");
  config.set_load_balancing_policy(cass::Memory::allocate<cass::DCAwarePolicy>(
                                     "invalid_data_center",
                                     0,
                                     false));
  cass::Session session;

  cass::Future::Ptr connect_future(session.connect(config, "invalid"));
  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  ASSERT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, connect_future->error()->code);

  ASSERT_TRUE(session.close()->wait_for(WAIT_FOR_TIME));
}

TEST_F(SessionUnitTest, ExecuteQueryReusingSession) {
  mockssandra::SimpleCluster cluster(simple());
  ASSERT_EQ(cluster.start_all(), 0);

  cass::Session session;
  for (int i = 0; i < 2; ++i) {
    connect(&session);
    query(&session);
    close(&session);
  }
}

TEST_F(SessionUnitTest, ExecuteQueryReusingSessionUsingSsl) {
  mockssandra::SimpleCluster cluster(simple());
  cass::SslContext::Ptr ssl_context = use_ssl(&cluster).socket_settings.ssl_context;
  ASSERT_EQ(cluster.start_all(), 0);

  cass::Session session;
  for (int i = 0; i < 2; ++i) {
    connect(&session, ssl_context.get());
    query(&session);
    close(&session);
  }
}

TEST_F(SessionUnitTest, ExecuteQueryReusingSessionChaotic) {
  mockssandra::SimpleCluster cluster(simple(), 3);
  ASSERT_EQ(cluster.start_all(), 0);

  OutagePlan outage_plan(loop(), &cluster);
  populate_outage_plan(&outage_plan);

  cass::Session session;
  cass::Future::Ptr outage_future = execute_outage_plan(&outage_plan);
  while (!outage_future->wait_for(1000)) { // 1 millisecond wait
    connect(&session, NULL, WAIT_FOR_TIME * 3);
    query(&session);
    close(&session, WAIT_FOR_TIME * 3);
  }
}

TEST_F(SessionUnitTest, ExecuteQueryReusingSessionUsingSslChaotic) {
  mockssandra::SimpleCluster cluster(simple(), 3);
  cass::SslContext::Ptr ssl_context = use_ssl(&cluster).socket_settings.ssl_context;
  ASSERT_EQ(cluster.start_all(), 0);

  OutagePlan outage_plan(loop(), &cluster);
  populate_outage_plan(&outage_plan);

  cass::Session session;
  cass::Future::Ptr outage_future = execute_outage_plan(&outage_plan);
  while (!outage_future->wait_for(1000)) { // 1 millisecond wait
    connect(&session, ssl_context.get(), WAIT_FOR_TIME * 3);
    query(&session);
    close(&session, WAIT_FOR_TIME * 3);
  }
}

TEST_F(SessionUnitTest, ExecuteQueryWithCompleteOutage) {
  mockssandra::SimpleCluster cluster(simple(), 3);
  ASSERT_EQ(cluster.start_all(), 0);

  cass::Session session;
  connect(&session);

  // Full outage
  cluster.stop_all();
  cass::QueryRequest::Ptr request(cass::Memory::allocate<cass::QueryRequest>("blah", 0));
  cass::Future::Ptr future = session.execute(request, NULL);
  ASSERT_TRUE(future->wait_for(WAIT_FOR_TIME));
  ASSERT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, future->error()->code);

  // Restart a node and execute query to ensure session recovers
  ASSERT_EQ(cluster.start(2), 0);
  test::Utils::msleep(200); // Give time for the reconnect to start
  query(&session);

  close(&session);
}

TEST_F(SessionUnitTest, ExecuteQueryWithCompleteOutageSpinDown) {
  mockssandra::SimpleCluster cluster(simple(), 3);
  ASSERT_EQ(cluster.start_all(), 0);

  cass::Session session;
  connect(&session);

  // Spin down nodes while querying
  query(&session);
  cluster.stop(3);
  query(&session);
  cluster.stop(1);
  query(&session);
  cluster.stop(2);

  // Full outage
  cass::QueryRequest::Ptr request(cass::Memory::allocate<cass::QueryRequest>("blah", 0));
  cass::Future::Ptr future = session.execute(request, NULL);
  ASSERT_TRUE(future->wait_for(WAIT_FOR_TIME));
  ASSERT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, future->error()->code);

  // Restart a node and execute query to ensure session recovers
  ASSERT_EQ(cluster.start(2), 0);
  test::Utils::msleep(200); // Give time for the reconnect to start
  query(&session);

  close(&session);
}

TEST_F(SessionUnitTest, ExecuteQueryWithThreads) {
  mockssandra::SimpleCluster cluster(simple());
  ASSERT_EQ(cluster.start_all(), 0);

  cass::Session session;
  connect(&session);
  query_on_threads(&session);
  close(&session);
}

TEST_F(SessionUnitTest, ExecuteQueryWithThreadsUsingSsl) {
  mockssandra::SimpleCluster cluster(simple());
  cass::SslContext::Ptr ssl_context = use_ssl(&cluster).socket_settings.ssl_context;
  ASSERT_EQ(cluster.start_all(), 0);

  cass::Session session;
  connect(&session, ssl_context.get());
  query_on_threads(&session);
  close(&session);
}

TEST_F(SessionUnitTest, ExecuteQueryWithThreadsChaotic) {
  mockssandra::SimpleCluster cluster(simple(), 3);
  ASSERT_EQ(cluster.start_all(), 0);

  cass::Session session;
  connect(&session);

  OutagePlan outage_plan(loop(), &cluster);
  populate_outage_plan(&outage_plan);

  cass::Future::Ptr outage_future = execute_outage_plan(&outage_plan);
  while (!outage_future->wait_for(1000)) { // 1 millisecond wait
    query_on_threads(&session);
  }

  close(&session);
}

TEST_F(SessionUnitTest, ExecuteQueryWithThreadsUsingSslChaotic) {
  mockssandra::SimpleCluster cluster(simple(), 3);
  cass::SslContext::Ptr ssl_context = use_ssl(&cluster).socket_settings.ssl_context;
  ASSERT_EQ(cluster.start_all(), 0);

  cass::Session session;
  connect(&session, ssl_context.get());

  OutagePlan outage_plan(loop(), &cluster);
  populate_outage_plan(&outage_plan);

  cass::Future::Ptr outage_future = execute_outage_plan(&outage_plan);
  while (!outage_future->wait_for(1000)) { // 1 millisecond wait
    query_on_threads(&session);
  }

  close(&session);
}
