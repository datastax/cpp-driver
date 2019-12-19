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
#define NUM_THREADS 2         // Number of threads to execute queries using a session
#define OUTAGE_PLAN_DELAY 250 // Reduced delay to incorporate larger outage plan

using namespace datastax::internal;
using namespace datastax::internal::core;

class SessionUnitTest : public EventLoopTest {
public:
  SessionUnitTest()
      : EventLoopTest("SessionUnitTest") {}

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

  void query_on_threads(Session* session, bool is_chaotic = false) {
    uv_thread_t threads[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; ++i) {
      if (is_chaotic) {
        ASSERT_EQ(0, uv_thread_create(&threads[i], query_is_chaotic, session));
      } else {
        ASSERT_EQ(0, uv_thread_create(&threads[i], query, session));
      }
    }
    for (int i = 0; i < NUM_THREADS; ++i) {
      uv_thread_join(&threads[i]);
    }
  }

  static void connect(const Config& config, Session* session,
                      uint64_t wait_for_time_us = WAIT_FOR_TIME) {
    Future::Ptr connect_future(session->connect(config));
    ASSERT_TRUE(connect_future->wait_for(wait_for_time_us))
        << "Timed out waiting for session to connect";
    ASSERT_FALSE(connect_future->error()) << cass_error_desc(connect_future->error()->code) << ": "
                                          << connect_future->error()->message;
  }

  static void connect(Session* session, SslContext* ssl_context = NULL,
                      uint64_t wait_for_time_us = WAIT_FOR_TIME, size_t num_nodes = 3) {
    Config config;
    config.set_constant_reconnect(100); // Faster reconnect time to handle cluster starts and stops
    for (size_t i = 1; i <= num_nodes; ++i) {
      OStringStream ss;
      ss << "127.0.0." << i;
      config.contact_points().push_back(Address(ss.str(), 9042));
    }
    if (ssl_context) {
      config.set_ssl_context(ssl_context);
    }
    connect(config, session, wait_for_time_us);
  }

  static void close(Session* session, uint64_t wait_for_time_us = WAIT_FOR_TIME) {
    Future::Ptr close_future(session->close());
    ASSERT_TRUE(close_future->wait_for(wait_for_time_us))
        << "Timed out waiting for session to close";
    ASSERT_FALSE(close_future->error())
        << cass_error_desc(close_future->error()->code) << ": " << close_future->error()->message;
  }

  static void query(Session* session, bool is_chaotic = false) {
    QueryRequest::Ptr request(new QueryRequest("blah", 0));
    request->set_is_idempotent(true);

    Future::Ptr future = session->execute(Request::ConstPtr(request));
    ASSERT_TRUE(future->wait_for(WAIT_FOR_TIME)) << "Timed out executing query";
    if (future->error()) fprintf(stderr, "%s\n", cass_error_desc(future->error()->code));
    if (is_chaotic) {
      ASSERT_TRUE(future->error() == NULL ||
                  future->error()->code == CASS_ERROR_LIB_NO_HOSTS_AVAILABLE)
          << cass_error_desc(future->error()->code) << ": " << future->error()->message;
    } else {
      ASSERT_FALSE(future->error())
          << cass_error_desc(future->error()->code) << ": " << future->error()->message;
    }
  }

  // uv_thread_create
  static void query(void* arg) {
    Session* session = static_cast<Session*>(arg);
    query(session);
  }
  static void query_is_chaotic(void* arg) {
    Session* session = static_cast<Session*>(arg);
    query(session, true);
  }

  bool check_consistency(const Session& session, CassConsistency expected_consistency,
                         CassConsistency expected_profile_consistency) {
    Config session_config = session.config();
    EXPECT_EQ(expected_consistency, session_config.consistency());

    const ExecutionProfile::Map& profiles = session_config.profiles();
    for (ExecutionProfile::Map::const_iterator it = profiles.begin(), end = profiles.end();
         it != end; ++it) {
      if (expected_profile_consistency != it->second.consistency()) {
        return false;
      }
    }
    return true;
  }

  class HostEventFuture : public Future {
  public:
    typedef SharedRefPtr<HostEventFuture> Ptr;

    enum Type { INVALID, START_NODE, STOP_NODE, ADD_NODE, REMOVE_NODE };

    typedef std::pair<Type, Address> Event;

    HostEventFuture()
        : Future(Future::FUTURE_TYPE_GENERIC) {}

    Type type() { return event_.first; }

    void set_event(Type type, const Address& host) {
      ScopedMutex lock(&mutex_);
      if (!is_set()) {
        event_ = Event(type, host);
        internal_set(lock);
      }
    }

    Event wait_for_event(uint64_t timeout_us) {
      ScopedMutex lock(&mutex_);
      return internal_wait_for(lock, timeout_us) ? event_ : Event(INVALID, Address());
    }

  private:
    Event event_;
  };

  class TestHostListener : public DefaultHostListener {
  public:
    typedef SharedRefPtr<TestHostListener> Ptr;

    TestHostListener() {
      events_.push_back(HostEventFuture::Ptr(new HostEventFuture()));
      uv_mutex_init(&mutex_);
    }

    ~TestHostListener() { uv_mutex_destroy(&mutex_); }

    virtual void on_host_up(const Host::Ptr& host) { push_back(HostEventFuture::START_NODE, host); }

    virtual void on_host_down(const Host::Ptr& host) {
      push_back(HostEventFuture::STOP_NODE, host);
    }

    virtual void on_host_added(const Host::Ptr& host) {
      push_back(HostEventFuture::ADD_NODE, host);
    }

    virtual void on_host_removed(const Host::Ptr& host) {
      push_back(HostEventFuture::REMOVE_NODE, host);
    }

    HostEventFuture::Event wait_for_event(uint64_t timeout_us) {
      HostEventFuture::Event event(front()->wait_for_event(timeout_us));
      if (event.first != HostEventFuture::INVALID) pop_front();
      return event;
    }

    size_t event_count() {
      ScopedMutex lock(&mutex_);
      size_t count = events_.size();
      return events_.front()->ready() ? count : count - 1;
    }

  private:
    typedef Deque<HostEventFuture::Ptr> EventQueue;

    HostEventFuture::Ptr front() {
      ScopedMutex lock(&mutex_);
      return events_.front();
    }

    void pop_front() {
      ScopedMutex lock(&mutex_);
      events_.pop_front();
    }

    void push_back(HostEventFuture::Type type, const Host::Ptr& host) {
      ScopedMutex lock(&mutex_);
      events_.back()->set_event(type, host->address());
      events_.push_back(HostEventFuture::Ptr(new HostEventFuture()));
    }

  private:
    uv_mutex_t mutex_;
    EventQueue events_;
  };

  class LocalDcClusterMetadataResolver : public ClusterMetadataResolver {
  public:
    LocalDcClusterMetadataResolver(const String& local_dc)
        : desired_local_dc_(local_dc) {}

  private:
    virtual void internal_resolve(uv_loop_t* loop, const AddressVec& contact_points) {
      resolved_contact_points_ = contact_points;
      local_dc_ = desired_local_dc_;
      callback_(this);
    }

    virtual void internal_cancel() {}

  private:
    String desired_local_dc_;
  };

  class LocalDcClusterMetadataResolverFactory : public ClusterMetadataResolverFactory {
  public:
    LocalDcClusterMetadataResolverFactory(const String& local_dc)
        : local_dc_(local_dc) {}

    virtual ClusterMetadataResolver::Ptr new_instance(const ClusterSettings& settings) const {
      return ClusterMetadataResolver::Ptr(new LocalDcClusterMetadataResolver(local_dc_));
    }

    virtual const char* name() const { return "LocalDc"; }

  private:
    String local_dc_;
  };

  class SupportedDbaasOptions : public mockssandra::Action {
  public:
    virtual void on_run(mockssandra::Request* request) const {
      Vector<String> product_type;
      product_type.push_back("DATASTAX_APOLLO");

      StringMultimap supported;
      supported["PRODUCT_TYPE"] = product_type;

      String body;
      mockssandra::encode_string_map(supported, &body);
      request->write(mockssandra::OPCODE_SUPPORTED, body);
    }
  };
};

TEST_F(SessionUnitTest, ExecuteQueryNotConnected) {
  Session session;
  Future::Ptr future = session.execute(Request::ConstPtr(new QueryRequest("blah", 0)));
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

  Config config;
  config.contact_points().push_back(Address("127.0.0.1", 9042));
  Session session;

  Future::Ptr connect_future(session.connect(config, "invalid"));
  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  ASSERT_EQ(CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE, connect_future->error()->code);

  ASSERT_TRUE(session.close()->wait_for(WAIT_FOR_TIME));
}

TEST_F(SessionUnitTest, InvalidDataCenter) {
  mockssandra::SimpleCluster cluster(simple());
  ASSERT_EQ(cluster.start_all(), 0);

  Config config;
  config.contact_points().push_back(Address("127.0.0.1", 9042));
  config.set_load_balancing_policy(new DCAwarePolicy("invalid_data_center", 0, false));
  Session session;

  Future::Ptr connect_future(session.connect(config));
  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  ASSERT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, connect_future->error()->code);

  ASSERT_TRUE(session.close()->wait_for(WAIT_FOR_TIME));
}

TEST_F(SessionUnitTest, InvalidLocalAddress) {
  mockssandra::SimpleCluster cluster(simple());
  ASSERT_EQ(cluster.start_all(), 0);

  Config config;
  config.set_local_address(Address("1.1.1.1", PORT)); // Invalid
  config.contact_points().push_back(Address("127.0.0.1", 9042));
  config.set_load_balancing_policy(new DCAwarePolicy("invalid_data_center", 0, false));
  Session session;

  Future::Ptr connect_future(session.connect(config, "invalid"));
  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  ASSERT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, connect_future->error()->code);

  ASSERT_TRUE(session.close()->wait_for(WAIT_FOR_TIME));
}

TEST_F(SessionUnitTest, ExecuteQueryReusingSession) {
  mockssandra::SimpleCluster cluster(simple());
  ASSERT_EQ(cluster.start_all(), 0);

  Session session;
  for (int i = 0; i < 2; ++i) {
    connect(&session);
    query(&session);
    close(&session);
  }
}

TEST_F(SessionUnitTest, ExecuteQueryReusingSessionUsingSsl) {
  mockssandra::SimpleCluster cluster(simple());
  SslContext::Ptr ssl_context = use_ssl(&cluster).socket_settings.ssl_context;
  ASSERT_EQ(cluster.start_all(), 0);

  Session session;
  for (int i = 0; i < 2; ++i) {
    connect(&session, ssl_context.get());
    query(&session);
    close(&session);
  }
}

TEST_F(SessionUnitTest, ExecuteQueryReusingSessionChaotic) {
  mockssandra::SimpleCluster cluster(simple(), 4);
  ASSERT_EQ(cluster.start_all(), 0);

  OutagePlan outage_plan(loop(), &cluster);
  populate_outage_plan(&outage_plan);

  Session session;
  Future::Ptr outage_future = execute_outage_plan(&outage_plan);
  while (!outage_future->wait_for(1000)) { // 1 millisecond wait
    connect(&session, NULL, WAIT_FOR_TIME * 3, 4);
    query(&session, true);
    close(&session, WAIT_FOR_TIME * 3);
  }
}

TEST_F(SessionUnitTest, ExecuteQueryReusingSessionUsingSslChaotic) {
  mockssandra::SimpleCluster cluster(simple(), 4);
  SslContext::Ptr ssl_context = use_ssl(&cluster).socket_settings.ssl_context;
  ASSERT_EQ(cluster.start_all(), 0);

  OutagePlan outage_plan(loop(), &cluster);
  populate_outage_plan(&outage_plan);

  Session session;
  Future::Ptr outage_future = execute_outage_plan(&outage_plan);
  while (!outage_future->wait_for(1000)) { // 1 millisecond wait
    connect(&session, ssl_context.get(), WAIT_FOR_TIME * 3, 4);
    query(&session, true);
    close(&session, WAIT_FOR_TIME * 3);
  }
}

TEST_F(SessionUnitTest, ExecuteQueryWithCompleteOutage) {
  mockssandra::SimpleCluster cluster(simple(), 3);
  ASSERT_EQ(cluster.start_all(), 0);

  Session session;
  connect(&session);

  // Full outage
  cluster.stop_all();
  Future::Ptr future = session.execute(Request::ConstPtr(new QueryRequest("blah", 0)));
  ASSERT_TRUE(future->wait_for(WAIT_FOR_TIME));
  ASSERT_TRUE(future->error());
  EXPECT_TRUE(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE == future->error()->code ||
              CASS_ERROR_LIB_REQUEST_TIMED_OUT == future->error()->code);

  // Restart a node and execute query to ensure session recovers
  ASSERT_EQ(cluster.start(2), 0);
  test::Utils::msleep(200); // Give time for the reconnect to start
  query(&session);

  close(&session);
}

TEST_F(SessionUnitTest, ExecuteQueryWithCompleteOutageSpinDown) {
  mockssandra::SimpleCluster cluster(simple(), 3);
  ASSERT_EQ(cluster.start_all(), 0);

  Session session;
  connect(&session);

  // Spin down nodes while querying
  query(&session);
  cluster.stop(3);
  query(&session);
  cluster.stop(1);
  query(&session);
  cluster.stop(2);

  // Full outage
  Future::Ptr future = session.execute(Request::ConstPtr(new QueryRequest("blah", 0)));
  ASSERT_TRUE(future->wait_for(WAIT_FOR_TIME));
  EXPECT_TRUE(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE == future->error()->code ||
              CASS_ERROR_LIB_REQUEST_TIMED_OUT == future->error()->code);

  // Restart a node and execute query to ensure session recovers
  ASSERT_EQ(cluster.start(2), 0);
  test::Utils::msleep(200); // Give time for the reconnect to start
  query(&session);

  close(&session);
}

TEST_F(SessionUnitTest, ExecuteQueryWithThreads) {
  mockssandra::SimpleCluster cluster(simple());
  ASSERT_EQ(cluster.start_all(), 0);

  Session session;
  connect(&session);
  query_on_threads(&session);
  close(&session);
}

TEST_F(SessionUnitTest, ExecuteQueryWithThreadsUsingSsl) {
  mockssandra::SimpleCluster cluster(simple());
  SslContext::Ptr ssl_context = use_ssl(&cluster).socket_settings.ssl_context;
  ASSERT_EQ(cluster.start_all(), 0);

  Session session;
  connect(&session, ssl_context.get());
  query_on_threads(&session);
  close(&session);
}

TEST_F(SessionUnitTest, ExecuteQueryWithThreadsChaotic) {
  mockssandra::SimpleCluster cluster(simple(), 4);
  ASSERT_EQ(cluster.start_all(), 0);

  Session session;
  connect(&session);

  OutagePlan outage_plan(loop(), &cluster);
  populate_outage_plan(&outage_plan);

  Future::Ptr outage_future = execute_outage_plan(&outage_plan);
  while (!outage_future->wait_for(1000)) { // 1 millisecond wait
    query_on_threads(&session, true);
  }

  close(&session);
}

TEST_F(SessionUnitTest, ExecuteQueryWithThreadsUsingSslChaotic) {
  mockssandra::SimpleCluster cluster(simple(), 4);
  SslContext::Ptr ssl_context = use_ssl(&cluster).socket_settings.ssl_context;
  ASSERT_EQ(cluster.start_all(), 0);

  Session session;
  connect(&session, ssl_context.get());

  OutagePlan outage_plan(loop(), &cluster);
  populate_outage_plan(&outage_plan);

  Future::Ptr outage_future = execute_outage_plan(&outage_plan);
  while (!outage_future->wait_for(1000)) { // 1 millisecond wait
    query_on_threads(&session, true);
  }

  close(&session);
}

TEST_F(SessionUnitTest, HostListener) {
  mockssandra::SimpleCluster cluster(simple(), 2);
  ASSERT_EQ(cluster.start_all(), 0);

  TestHostListener::Ptr listener(new TestHostListener());

  Config config;
  config.set_constant_reconnect(100); // Reconnect immediately
  config.contact_points().push_back(Address("127.0.0.2", 9042));
  config.set_host_listener(listener);

  Session session;
  connect(config, &session);

  { // Initial nodes available from peers table
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::ADD_NODE, Address("127.0.0.1", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::START_NODE, Address("127.0.0.1", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::ADD_NODE, Address("127.0.0.2", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::START_NODE, Address("127.0.0.2", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
  }

  {
    cluster.remove(1);
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::STOP_NODE, Address("127.0.0.1", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::REMOVE_NODE, Address("127.0.0.1", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
  }

  {
    cluster.add(1);
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::ADD_NODE, Address("127.0.0.1", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::START_NODE, Address("127.0.0.1", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
  }

  {
    cluster.stop(2);
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::STOP_NODE, Address("127.0.0.2", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
  }

  {
    cluster.start(2);
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::START_NODE, Address("127.0.0.2", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
  }

  close(&session);

  ASSERT_EQ(0u, listener->event_count());
}

TEST_F(SessionUnitTest, HostListenerDCAwareLocal) {
  mockssandra::SimpleCluster cluster(simple(), 2, 1);
  ASSERT_EQ(cluster.start_all(), 0);

  TestHostListener::Ptr listener(new TestHostListener());

  Config config;
  config.set_constant_reconnect(100); // Reconnect immediately
  config.contact_points().push_back(Address("127.0.0.1", 9042));
  config.set_host_listener(listener);

  Session session;
  connect(config, &session);

  { // Initial nodes available from peers table
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::ADD_NODE, Address("127.0.0.1", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::START_NODE, Address("127.0.0.1", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::ADD_NODE, Address("127.0.0.2", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::START_NODE, Address("127.0.0.2", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
  }

  { // Node 3 is DC2 should be ignored
    cluster.stop(3);
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::INVALID, Address()),
              listener->wait_for_event(WAIT_FOR_TIME));
  }

  close(&session);

  ASSERT_EQ(0u, listener->event_count());
}

// TODO: Remove HostListenerDCAwareRemote after remote DC settings are removed from API
TEST_F(SessionUnitTest, HostListenerDCAwareRemote) {
  mockssandra::SimpleCluster cluster(simple(), 2, 1);
  ASSERT_EQ(cluster.start_all(), 0);

  TestHostListener::Ptr listener(new TestHostListener());

  Config config;
  config.set_constant_reconnect(100); // Reconnect immediately
  config.contact_points().push_back(Address("127.0.0.1", 9042));
  config.set_load_balancing_policy(new DCAwarePolicy("dc1", 1, false));
  config.set_host_listener(listener);

  Session session;
  connect(config, &session);

  { // Initial nodes available from peers table
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::ADD_NODE, Address("127.0.0.1", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::START_NODE, Address("127.0.0.1", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::ADD_NODE, Address("127.0.0.2", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::START_NODE, Address("127.0.0.2", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::ADD_NODE, Address("127.0.0.3", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::START_NODE, Address("127.0.0.3", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
  }

  {
    cluster.stop(3);
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::STOP_NODE, Address("127.0.0.3", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
  }

  close(&session);

  ASSERT_EQ(0u, listener->event_count());
}

TEST_F(SessionUnitTest, HostListenerNodeDown) {
  mockssandra::SimpleCluster cluster(simple(), 3);
  ASSERT_EQ(cluster.start(1), 0);
  ASSERT_EQ(cluster.start(3), 0);

  TestHostListener::Ptr listener(new TestHostListener());

  Config config;
  config.set_constant_reconnect(100); // Reconnect immediately
  config.contact_points().push_back(Address("127.0.0.1", 9042));
  config.set_host_listener(listener);

  Session session;
  connect(config, &session);

  { // Initial nodes available from peers table
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::ADD_NODE, Address("127.0.0.1", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::START_NODE, Address("127.0.0.1", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::ADD_NODE, Address("127.0.0.2", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::START_NODE, Address("127.0.0.2", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::ADD_NODE, Address("127.0.0.3", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::START_NODE, Address("127.0.0.3", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
  }

  { // Node 2 connection should not be established (node down event)
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::STOP_NODE, Address("127.0.0.2", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
  }

  {
    cluster.start(2);
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::START_NODE, Address("127.0.0.2", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
  }

  close(&session);

  ASSERT_EQ(0u, listener->event_count());
}

TEST_F(SessionUnitTest, LocalDcUpdatedOnPolicy) {
  mockssandra::SimpleCluster cluster(simple(), 3, 1);
  ASSERT_EQ(cluster.start_all(), 0);

  TestHostListener::Ptr listener(new TestHostListener());

  Config config;
  config.contact_points().push_back(Address("127.0.0.4", 9042));
  config.set_cluster_metadata_resolver_factory(
      ClusterMetadataResolverFactory::Ptr(new LocalDcClusterMetadataResolverFactory("dc2")));
  config.set_host_listener(listener);

  Session session;
  connect(config, &session);

  { // Initial nodes available from peers table (should skip DC1)
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::ADD_NODE, Address("127.0.0.4", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::START_NODE, Address("127.0.0.4", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
  }

  for (int i = 0; i < 20; ++i) { // Validate the request processors are using DC2 only
    ResponseFuture::Ptr future = session.execute(Request::ConstPtr(new QueryRequest("blah", 0)));
    EXPECT_TRUE(future->wait_for(WAIT_FOR_TIME));
    EXPECT_FALSE(future->error());
    EXPECT_EQ("127.0.0.4", future->address().to_string());
  }

  close(&session);

  ASSERT_EQ(0u, listener->event_count());
}

TEST_F(SessionUnitTest, LocalDcNotOverriddenOnPolicy) {
  mockssandra::SimpleCluster cluster(simple(), 1, 3);
  ASSERT_EQ(cluster.start_all(), 0);

  TestHostListener::Ptr listener(new TestHostListener());

  Config config;
  config.contact_points().push_back(Address("127.0.0.1", 9042));
  config.set_load_balancing_policy(new DCAwarePolicy("dc1"));
  config.set_cluster_metadata_resolver_factory(
      ClusterMetadataResolverFactory::Ptr(new LocalDcClusterMetadataResolverFactory("dc2")));
  config.set_host_listener(listener);

  Session session;
  connect(config, &session);

  { // Initial nodes available from peers table (should be DC1)
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::ADD_NODE, Address("127.0.0.1", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::START_NODE, Address("127.0.0.1", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
  }

  for (int i = 0; i < 20; ++i) { // Validate the request processors are using DC1 only
    ResponseFuture::Ptr future = session.execute(Request::ConstPtr(new QueryRequest("blah", 0)));
    EXPECT_TRUE(future->wait_for(WAIT_FOR_TIME));
    EXPECT_FALSE(future->error());
    EXPECT_EQ("127.0.0.1", future->address().to_string());
  }

  close(&session);

  ASSERT_EQ(0u, listener->event_count());
}

TEST_F(SessionUnitTest, LocalDcOverriddenOnPolicyUsingExecutionProfiles) {
  mockssandra::SimpleCluster cluster(simple(), 3, 1);
  ASSERT_EQ(cluster.start_all(), 0);

  TestHostListener::Ptr listener(new TestHostListener());

  Config config;
  config.contact_points().push_back(Address("127.0.0.4", 9042));
  config.set_use_randomized_contact_points(
      false); // Ensure round robin order over DC for query execution
  config.set_cluster_metadata_resolver_factory(
      ClusterMetadataResolverFactory::Ptr(new LocalDcClusterMetadataResolverFactory("dc2")));
  config.set_host_listener(listener);

  ExecutionProfile profile;
  profile.set_load_balancing_policy(new DCAwarePolicy());
  config.set_execution_profile("use_propagated_local_dc", &profile);

  Session session;
  connect(config, &session);

  { // Initial nodes available from peers table (should be DC2)
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::ADD_NODE, Address("127.0.0.4", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::START_NODE, Address("127.0.0.4", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
  }

  for (int i = 0; i < 20; ++i) { // Validate the default profile is using DC2 only
    ResponseFuture::Ptr future = session.execute(Request::ConstPtr(new QueryRequest("blah", 0)));
    EXPECT_TRUE(future->wait_for(WAIT_FOR_TIME));
    EXPECT_FALSE(future->error());
    EXPECT_EQ("127.0.0.4", future->address().to_string());
  }

  for (int i = 0; i < 20; ++i) { // Validate the default profile is using DC2 only
    QueryRequest::Ptr request(new QueryRequest("blah", 0));
    request->set_execution_profile_name("use_propagated_local_dc");

    ResponseFuture::Ptr future = session.execute(Request::ConstPtr(request));
    EXPECT_TRUE(future->wait_for(WAIT_FOR_TIME));
    EXPECT_FALSE(future->error());
    EXPECT_EQ("127.0.0.4", future->address().to_string());
  }

  close(&session);

  ASSERT_EQ(0u, listener->event_count());
}

TEST_F(SessionUnitTest, LocalDcNotOverriddenOnPolicyUsingExecutionProfiles) {
  mockssandra::SimpleCluster cluster(simple(), 3, 1);
  ASSERT_EQ(cluster.start_all(), 0);

  TestHostListener::Ptr listener(new TestHostListener());

  Config config;
  config.contact_points().push_back(Address("127.0.0.4", 9042));
  config.set_use_randomized_contact_points(
      false); // Ensure round robin order over DC for query execution
  config.set_cluster_metadata_resolver_factory(
      ClusterMetadataResolverFactory::Ptr(new LocalDcClusterMetadataResolverFactory("dc2")));
  config.set_host_listener(listener);

  ExecutionProfile profile;
  profile.set_load_balancing_policy(new DCAwarePolicy("dc1"));
  config.set_execution_profile("use_dc1", &profile);

  Session session;
  connect(config, &session);

  { // Initial nodes available from peers table (should be DC1 and DC2)
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::ADD_NODE, Address("127.0.0.1", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::START_NODE, Address("127.0.0.1", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::ADD_NODE, Address("127.0.0.2", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::START_NODE, Address("127.0.0.2", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::ADD_NODE, Address("127.0.0.3", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::START_NODE, Address("127.0.0.3", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::ADD_NODE, Address("127.0.0.4", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
    EXPECT_EQ(HostEventFuture::Event(HostEventFuture::START_NODE, Address("127.0.0.4", 9042)),
              listener->wait_for_event(WAIT_FOR_TIME));
  }

  for (int i = 0; i < 20; ++i) { // Validate the default profile is using DC2 only
    ResponseFuture::Ptr future = session.execute(Request::ConstPtr(new QueryRequest("blah", 0)));
    EXPECT_TRUE(future->wait_for(WAIT_FOR_TIME));
    EXPECT_FALSE(future->error());
    EXPECT_EQ("127.0.0.4", future->address().to_string());
  }

  for (int i = 0; i < 20; ++i) { // Validate the default profile is using DC1 only
    QueryRequest::Ptr request(new QueryRequest("blah", 0));
    request->set_execution_profile_name("use_dc1");

    ResponseFuture::Ptr future = session.execute(Request::ConstPtr(request));
    EXPECT_TRUE(future->wait_for(WAIT_FOR_TIME));
    EXPECT_FALSE(future->error());
    EXPECT_NE("127.0.0.4", future->address().to_string());
  }

  close(&session);

  ASSERT_EQ(0u, listener->event_count());
}

TEST_F(SessionUnitTest, NoContactPoints) {
  // No cluster needed

  Config config;
  config.contact_points().clear();

  Session session;
  Future::Ptr connect_future(session.connect(config));
  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME))
      << "Timed out waiting for session to connect";
  ASSERT_TRUE(connect_future->error());
  EXPECT_EQ(connect_future->error()->code, CASS_ERROR_LIB_NO_HOSTS_AVAILABLE);
}

TEST_F(SessionUnitTest, DefaultConsistency) {
  mockssandra::SimpleCluster cluster(simple());
  ASSERT_EQ(cluster.start_all(), 0);

  Session session;
  {
    Config session_config = session.config();
    EXPECT_EQ(CASS_CONSISTENCY_UNKNOWN, session_config.consistency());
  }

  ExecutionProfile profile;
  Config config;
  config.contact_points().push_back(Address("127.0.0.1", 9042));
  config.set_execution_profile("profile", &profile);
  connect(config, &session);

  EXPECT_TRUE(check_consistency(session, CASS_DEFAULT_CONSISTENCY, CASS_DEFAULT_CONSISTENCY));

  close(&session);
}

TEST_F(SessionUnitTest, DefaultConsistencyExecutionProfileNotUpdated) {
  mockssandra::SimpleCluster cluster(simple());
  ASSERT_EQ(cluster.start_all(), 0);

  Session session;
  {
    Config session_config = session.config();
    EXPECT_EQ(CASS_CONSISTENCY_UNKNOWN, session_config.consistency());
  }

  ExecutionProfile profile;
  profile.set_consistency(CASS_CONSISTENCY_LOCAL_QUORUM);
  Config config;
  config.contact_points().push_back(Address("127.0.0.1", 9042));
  config.set_execution_profile("profile", &profile);
  connect(config, &session);

  EXPECT_TRUE(check_consistency(session, CASS_DEFAULT_CONSISTENCY, CASS_CONSISTENCY_LOCAL_QUORUM));

  close(&session);
}

TEST_F(SessionUnitTest, RemoteDCNodeRecovery) {
  mockssandra::SimpleCluster cluster(simple(), 1, 1); // 1 local DC node and 1 remote DC node
  ASSERT_EQ(cluster.start_all(), 0);

  ExecutionProfile profile;
  Config config;
  config.set_constant_reconnect(100); // Faster reconnect time to handle node outages
  config.contact_points().push_back(Address("127.0.0.1", 9042));
  config.set_load_balancing_policy(new DCAwarePolicy("dc1", 1));

  Session session;
  connect(config, &session);

  cluster.stop(1); // Force using the remote node

  cluster.stop(2); // Force the remote node down and up
  cluster.start(2);

  bool remote_dc_node_recovered = false;

  // Wait for the remote DC node to become available
  for (int i = 0; i < 20; ++i) { // Around 2 seconds
    QueryRequest::Ptr request(new QueryRequest("blah", 0));
    request->set_consistency(CASS_CONSISTENCY_ONE); // Don't use a LOCAL consistency
    request->set_record_attempted_addresses(true);
    ResponseFuture::Ptr future = session.execute(Request::ConstPtr(request));
    EXPECT_TRUE(future->wait_for(WAIT_FOR_TIME));
    if (!future->error() && !future->attempted_addresses().empty() &&
        Address("127.0.0.2", 9042) == future->attempted_addresses()[0]) {
      remote_dc_node_recovered = true;
      break;
    }
    test::Utils::msleep(100);
  }

  EXPECT_TRUE(remote_dc_node_recovered);

  close(&session);
}

TEST_F(SessionUnitTest, DbaasDetectionUpdateDefaultConsistency) {
  mockssandra::SimpleRequestHandlerBuilder builder;
  builder.on(mockssandra::OPCODE_OPTIONS).execute(new SupportedDbaasOptions());
  mockssandra::SimpleCluster cluster(builder.build());
  ASSERT_EQ(cluster.start_all(), 0);

  Session session;
  {
    Config session_config = session.config();
    EXPECT_EQ(CASS_CONSISTENCY_UNKNOWN, session_config.consistency());
  }

  ExecutionProfile profile;
  Config config;
  config.contact_points().push_back(Address("127.0.0.1", 9042));
  config.set_execution_profile("profile", &profile);
  connect(config, &session);

  EXPECT_TRUE(
      check_consistency(session, CASS_DEFAULT_DBAAS_CONSISTENCY, CASS_DEFAULT_DBAAS_CONSISTENCY));

  close(&session);
}

TEST_F(SessionUnitTest, DbaasDefaultConsistencyExecutionProfileNotUpdate) {
  mockssandra::SimpleRequestHandlerBuilder builder;
  builder.on(mockssandra::OPCODE_OPTIONS).execute(new SupportedDbaasOptions());
  mockssandra::SimpleCluster cluster(builder.build());
  ASSERT_EQ(cluster.start_all(), 0);

  Session session;
  {
    Config session_config = session.config();
    EXPECT_EQ(CASS_CONSISTENCY_UNKNOWN, session_config.consistency());
  }

  ExecutionProfile profile;
  profile.set_consistency(CASS_CONSISTENCY_LOCAL_ONE);
  Config config;
  config.contact_points().push_back(Address("127.0.0.1", 9042));
  config.set_execution_profile("profile", &profile);
  connect(config, &session);

  EXPECT_TRUE(
      check_consistency(session, CASS_DEFAULT_DBAAS_CONSISTENCY, CASS_CONSISTENCY_LOCAL_ONE));

  close(&session);
}
