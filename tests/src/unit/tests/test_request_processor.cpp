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

#include "event_loop.hpp"
#include "query_request.hpp"
#include "ref_counted.hpp"
#include "request_handler.hpp"
#include "request_processor_initializer.hpp"

#define NUM_NODES 3

using namespace datastax::internal;
using namespace datastax::internal::core;

class InorderLoadBalancingPolicy : public LoadBalancingPolicy {
public:
  typedef SharedRefPtr<LoadBalancingPolicy> Ptr;
  typedef Vector<Ptr> Vec;

  InorderLoadBalancingPolicy()
      : LoadBalancingPolicy()
      , hosts_(new HostVec()) {}

  virtual void init(const Host::Ptr& connected_host, const HostMap& hosts, Random* random,
                    const String& local_dc) {
    hosts_->reserve(hosts.size());
    std::transform(hosts.begin(), hosts.end(), std::back_inserter(*hosts_), GetHost());
  }

  virtual CassHostDistance distance(const Host::Ptr& host) const {
    return CASS_HOST_DISTANCE_LOCAL;
  }

  virtual bool is_host_up(const Address& address) const {
    return std::find_if(hosts_->begin(), hosts_->end(), FindAddress(address)) != hosts_->end();
  }

  virtual void on_host_added(const Host::Ptr& host) { add_host(hosts_, host); }

  virtual void on_host_removed(const Host::Ptr& host) { remove_host(hosts_, host); }

  virtual void on_host_up(const Host::Ptr& host) { add_host(hosts_, host); }

  virtual void on_host_down(const Address& address) { remove_host(hosts_, address); }

  virtual QueryPlan* new_query_plan(const String& keyspace, RequestHandler* request_handler,
                                    const TokenMap* token_map) {
    return new InternalQueryPlan(hosts_);
  }

  virtual LoadBalancingPolicy* new_instance() { return new InorderLoadBalancingPolicy(); }

private:
  struct FindAddress {

    FindAddress(const Address& address)
        : address(address) {}

    bool operator()(const Host::Ptr& host) const { return host->address() == address; }

    Address address;
  };

  class InternalQueryPlan : public datastax::internal::core::QueryPlan {
  public:
    InternalQueryPlan(const CopyOnWriteHostVec& hosts)
        : index_(0)
        , hosts_(hosts) {}

    virtual Host::Ptr compute_next() {
      if (index_ < hosts_->size()) {
        return (*hosts_)[index_++];
      }
      return Host::Ptr();
    }

  private:
    size_t index_;
    CopyOnWriteHostVec hosts_;
  };

private:
  CopyOnWriteHostVec hosts_;
};

class RequestProcessorUnitTest : public EventLoopTest {
public:
  RequestProcessorUnitTest()
      : EventLoopTest("RequestProcessorUnitTest") {}

  HostMap generate_hosts(size_t num_hosts = 3) {
    HostMap hosts;
    num_hosts = std::min(num_hosts, static_cast<size_t>(255));
    for (size_t i = 1; i <= num_hosts; ++i) {
      char buf[64];
      sprintf(buf, "127.0.0.%d", static_cast<int>(i));
      Host::Ptr host(new Host(Address(buf, PORT)));
      hosts[host->address()] = host;
    }
    return hosts;
  }

  void try_request(const RequestProcessor::Ptr& processor,
                   uint64_t wait_for_time_us = WAIT_FOR_TIME) {
    ResponseFuture::Ptr response_future(new ResponseFuture());
    QueryRequest::Ptr query_request(new QueryRequest("SELECT * FROM table"));
    query_request->set_is_idempotent(true);
    Request::ConstPtr request(query_request);
    RequestHandler::Ptr request_handler(new RequestHandler(request, response_future));

    processor->process_request(request_handler);
    ASSERT_TRUE(response_future->wait_for(wait_for_time_us)) << "Timed out waiting for response";
    ASSERT_FALSE(response_future->error()) << cass_error_desc(response_future->error()->code)
                                           << ": " << response_future->error()->message;
    ;
  }

  class Future : public core::Future {
  public:
    typedef SharedRefPtr<Future> Ptr;

    Future()
        : core::Future(FUTURE_TYPE_GENERIC) {}

    ~Future() {
      if (processor_) {
        processor_->close();
      }
    }

    const RequestProcessor::Ptr& processor() const { return processor_; }

    void set_processor(const RequestProcessor::Ptr& processor) {
      ScopedMutex lock(&mutex_);
      processor_ = processor;
      internal_set(lock);
    }

  private:
    RequestProcessor::Ptr processor_;
  };

  class CloseListener
      : public RefCounted<CloseListener>
      , public RequestProcessorListener {
  public:
    typedef SharedRefPtr<CloseListener> Ptr;

    CloseListener(const Future::Ptr& close_future = Future::Ptr())
        : close_future_(close_future)
        , processor_(NULL) {
      inc_ref();
    }

    virtual ~CloseListener() {}

    virtual void on_connect(RequestProcessor* processor) { processor_ = processor; }

    virtual void on_pool_up(const Address& address) {
      // In the absence of a Cluster object the processor must notify itself
      // that a host is ready.
      if (processor_) {
        processor_->notify_host_ready(Host::Ptr(new Host(address)));
      }
    }
    virtual void on_pool_down(const Address& address) {}
    virtual void on_pool_critical_error(const Address& address, Connector::ConnectionError code,
                                        const String& message) {}
    virtual void on_keyspace_changed(const String& keyspace,
                                     const KeyspaceChangedHandler::Ptr& handler) {}
    virtual void on_prepared_metadata_changed(const String& id,
                                              const PreparedMetadata::Entry::Ptr& entry) {}

    virtual void on_close(RequestProcessor* processor) {
      if (close_future_) {
        close_future_->set();
      }
      dec_ref();
    }

  private:
    Future::Ptr close_future_;
    RequestProcessor* processor_;
  };

  class CriticalErrorListener : public RequestProcessorListener {
  public:
    size_t count(Connector::ConnectionError code) const {
      return std::count(error_codes_.begin(), error_codes_.end(), code);
    }

    virtual void on_pool_up(const Address& address) {}
    virtual void on_pool_down(const Address& address) {}
    virtual void on_pool_critical_error(const Address& address, Connector::ConnectionError code,
                                        const String& message) {
      error_codes_.push_back(code);
    }
    virtual void on_keyspace_changed(const String& keyspace,
                                     const KeyspaceChangedHandler::Ptr& handler) {}
    virtual void on_prepared_metadata_changed(const String& id,
                                              const PreparedMetadata::Entry::Ptr& entry) {}
    virtual void on_close(RequestProcessor* processor) {}

  private:
    Vector<Connector::ConnectionError> error_codes_;
  };

  class UpDownListener : public CloseListener {
  public:
    typedef SharedRefPtr<UpDownListener> Ptr;

    UpDownListener(const Future::Ptr& up_future, const Future::Ptr& down_future,
                   const Host::Ptr& target_host = Host::Ptr())
        : up_future_(up_future)
        , down_future_(down_future)
        , target_host_(target_host) {}

    virtual void on_pool_up(const Address& address) {
      if (target_host_ && target_host_->address() != address) return;
      up_future_->set();
    }

    virtual void on_pool_down(const Address& address) {
      if (target_host_ && target_host_->address() != address) return;
      down_future_->set();
    }

  private:
    Future::Ptr up_future_;
    Future::Ptr down_future_;
    Host::Ptr target_host_;
  };

  static void on_connected(RequestProcessorInitializer* initializer, Future* future) {
    if (initializer->is_ok()) {
      future->set_processor(initializer->release_processor());
    } else {
      switch (initializer->error_code()) {
        case RequestProcessorInitializer::REQUEST_PROCESSOR_ERROR_KEYSPACE:
          future->set_error(CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE, initializer->error_message());
          break;
        case RequestProcessorInitializer::REQUEST_PROCESSOR_ERROR_NO_HOSTS_AVAILABLE:
          future->set_error(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
                            "Unable to connect to any contact points");
          break;
        case RequestProcessorInitializer::REQUEST_PROCESSOR_ERROR_UNABLE_TO_INIT:
          future->set_error(CASS_ERROR_LIB_UNABLE_TO_INIT, initializer->error_message());
          break;
        default:
          future->set_error(CASS_ERROR_LIB_UNABLE_TO_CONNECT, initializer->error_message());
          break;
      }
    }
  }
};

TEST_F(RequestProcessorUnitTest, Simple) {
  mockssandra::SimpleCluster cluster(simple(), NUM_NODES);
  ASSERT_EQ(cluster.start_all(), 0);

  HostMap hosts(generate_hosts());

  Future::Ptr connect_future(new Future());
  RequestProcessorInitializer::Ptr initializer(new RequestProcessorInitializer(
      hosts.begin()->second, PROTOCOL_VERSION, hosts, TokenMap::Ptr(), "",
      bind_callback(on_connected, connect_future.get())));

  initializer->initialize(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_FALSE(connect_future->error());

  try_request(connect_future->processor());
}

TEST_F(RequestProcessorUnitTest, CloseWithRequestsPending) {
  mockssandra::SimpleCluster cluster(simple(), NUM_NODES);
  ASSERT_EQ(cluster.start_all(), 0);

  HostMap hosts(generate_hosts());

  Future::Ptr connect_future(new Future());
  RequestProcessorInitializer::Ptr initializer(new RequestProcessorInitializer(
      hosts.begin()->second, PROTOCOL_VERSION, hosts, TokenMap::Ptr(), "",
      bind_callback(on_connected, connect_future.get())));

  initializer->initialize(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_FALSE(connect_future->error());

  Vector<ResponseFuture::Ptr> futures;
  RequestProcessor::Ptr processor(connect_future->processor());

  for (int i = 0; i < 4096; ++i) {
    ResponseFuture::Ptr response_future(new ResponseFuture());
    Request::ConstPtr request(new QueryRequest("SELECT * FROM table"));
    RequestHandler::Ptr request_handler(new RequestHandler(request, response_future));

    processor->process_request(request_handler);
    futures.push_back(response_future);
  }

  processor->close();

  for (Vector<ResponseFuture::Ptr>::const_iterator it = futures.begin(), end = futures.end();
       it != end; ++it) {
    ResponseFuture::Ptr response_future(*it);
    ASSERT_TRUE(response_future->wait_for(WAIT_FOR_TIME));
    EXPECT_FALSE(response_future->error());
  }
}

TEST_F(RequestProcessorUnitTest, Auth) {
  mockssandra::SimpleCluster cluster(auth(), NUM_NODES);
  ASSERT_EQ(cluster.start_all(), 0);

  HostMap hosts(generate_hosts());

  Future::Ptr connect_future(new Future());
  RequestProcessorInitializer::Ptr initializer(new RequestProcessorInitializer(
      hosts.begin()->second, PROTOCOL_VERSION, hosts, TokenMap::Ptr(), "",
      bind_callback(on_connected, connect_future.get())));

  RequestProcessorSettings settings;
  settings.connection_pool_settings.connection_settings.auth_provider.reset(
      new PlainTextAuthProvider("cassandra", "cassandra"));

  initializer->with_settings(settings)->initialize(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_FALSE(connect_future->error());

  try_request(connect_future->processor());
}

TEST_F(RequestProcessorUnitTest, Ssl) {
  mockssandra::SimpleCluster cluster(simple(), NUM_NODES);
  RequestProcessorSettings settings;
  settings.connection_pool_settings.connection_settings = use_ssl(&cluster);
  ASSERT_EQ(cluster.start_all(), 0);

  HostMap hosts(generate_hosts());

  Future::Ptr connect_future(new Future());
  RequestProcessorInitializer::Ptr initializer(new RequestProcessorInitializer(
      hosts.begin()->second, PROTOCOL_VERSION, hosts, TokenMap::Ptr(), "",
      bind_callback(on_connected, connect_future.get())));

  initializer->with_settings(settings)->initialize(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_FALSE(connect_future->error());

  try_request(connect_future->processor());
}

TEST_F(RequestProcessorUnitTest, NotifyAddRemoveHost) {
  mockssandra::SimpleCluster cluster(simple(), NUM_NODES);
  ASSERT_EQ(cluster.start_all(), 0);

  HostMap hosts(generate_hosts());

  Host::Ptr to_add_remove(hosts.begin()->second);
  hosts.erase(to_add_remove->address()); // Remove to add/remove it back later

  Future::Ptr connect_future(new Future());
  Future::Ptr up_future(new Future());
  Future::Ptr down_future(new Future());
  RequestProcessorInitializer::Ptr initializer(new RequestProcessorInitializer(
      hosts.begin()->second, PROTOCOL_VERSION, hosts, TokenMap::Ptr(), "",
      bind_callback(on_connected, connect_future.get())));

  RequestProcessorSettings settings;
  settings.connection_pool_settings.reconnection_policy.reset(
      new ConstantReconnectionPolicy(1)); // Reconnect immediately

  UpDownListener::Ptr listener(new UpDownListener(up_future, down_future, to_add_remove));

  initializer->with_settings(settings)->with_listener(listener.get())->initialize(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_FALSE(connect_future->error());

  RequestProcessor::Ptr processor(connect_future->processor());

  processor->notify_host_added(to_add_remove);
  ASSERT_TRUE(up_future->wait_for(WAIT_FOR_TIME));

  processor->notify_host_removed(to_add_remove);
  ASSERT_TRUE(down_future->wait_for(WAIT_FOR_TIME));
}

TEST_F(RequestProcessorUnitTest, CloseDuringReconnect) {
  mockssandra::SimpleCluster cluster(simple(), NUM_NODES);
  ASSERT_EQ(cluster.start_all(), 0);

  HostMap hosts(generate_hosts());

  Future::Ptr close_future(new Future());
  Future::Ptr connect_future(new Future());
  RequestProcessorInitializer::Ptr initializer(new RequestProcessorInitializer(
      hosts.begin()->second, PROTOCOL_VERSION, hosts, TokenMap::Ptr(), "",
      bind_callback(on_connected, connect_future.get())));

  RequestProcessorSettings settings;
  settings.connection_pool_settings.reconnection_policy.reset(
      new ConstantReconnectionPolicy(100000)); // Make sure we're reconnecting when we close.

  CloseListener::Ptr listener(new CloseListener(close_future));

  initializer->with_settings(settings)->with_listener(listener.get())->initialize(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_FALSE(connect_future->error());

  RequestProcessor::Ptr processor(connect_future->processor());

  cluster.stop(1);
  test::Utils::msleep(200); // Give time for the reconnect to start
  processor->close();

  ASSERT_TRUE(close_future->wait_for(WAIT_FOR_TIME));
}

TEST_F(RequestProcessorUnitTest, CloseDuringAddNewHost) {
  mockssandra::SimpleCluster cluster(simple(), NUM_NODES);
  ASSERT_EQ(cluster.start_all(), 0);

  HostMap hosts(generate_hosts());

  Host::Ptr to_add(hosts.begin()->second);
  hosts.erase(to_add->address()); // Remove to add it back later

  Future::Ptr close_future(new Future());
  Future::Ptr connect_future(new Future());
  RequestProcessorInitializer::Ptr initializer(new RequestProcessorInitializer(
      hosts.begin()->second, PROTOCOL_VERSION, hosts, TokenMap::Ptr(), "",
      bind_callback(on_connected, connect_future.get())));

  CloseListener::Ptr listener(new CloseListener(close_future));

  initializer->with_listener(listener.get())->initialize(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_FALSE(connect_future->error());

  RequestProcessor::Ptr processor(connect_future->processor());

  processor->notify_host_added(to_add);
  processor->close();

  ASSERT_TRUE(close_future->wait_for(WAIT_FOR_TIME));
}

TEST_F(RequestProcessorUnitTest, PoolDown) {
  mockssandra::SimpleCluster cluster(simple(), NUM_NODES);
  ASSERT_EQ(cluster.start_all(), 0);

  HostMap hosts(generate_hosts());
  Host::Ptr target_host(hosts.find(Address("127.0.0.1", PORT))->second);
  ASSERT_TRUE(target_host);

  Future::Ptr connect_future(new Future());
  Future::Ptr up_future(new Future());
  Future::Ptr down_future(new Future());
  RequestProcessorInitializer::Ptr initializer(new RequestProcessorInitializer(
      hosts.begin()->second, PROTOCOL_VERSION, hosts, TokenMap::Ptr(), "",
      bind_callback(on_connected, connect_future.get())));

  UpDownListener::Ptr listener(new UpDownListener(up_future, down_future, target_host));

  initializer->with_listener(listener.get())->initialize(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_FALSE(connect_future->error());

  ASSERT_TRUE(up_future->wait_for(WAIT_FOR_TIME));

  cluster.stop(1);
  ASSERT_TRUE(down_future->wait_for(WAIT_FOR_TIME));
}

TEST_F(RequestProcessorUnitTest, PoolUp) {
  // Only start specific nodes
  mockssandra::SimpleCluster cluster(simple(), NUM_NODES);
  ASSERT_EQ(cluster.start(2), 0);
  ASSERT_EQ(cluster.start(3), 0);

  HostMap hosts(generate_hosts());
  Host::Ptr target_host(hosts.find(Address("127.0.0.1", PORT))->second);
  ASSERT_TRUE(target_host);

  Future::Ptr connect_future(new Future());
  Future::Ptr up_future(new Future());
  Future::Ptr down_future(new Future());
  RequestProcessorInitializer::Ptr initializer(new RequestProcessorInitializer(
      hosts.begin()->second, PROTOCOL_VERSION, hosts, TokenMap::Ptr(), "",
      bind_callback(on_connected, connect_future.get())));

  RequestProcessorSettings settings;
  settings.connection_pool_settings.reconnection_policy.reset(
      new ConstantReconnectionPolicy(1)); // Reconnect immediately

  UpDownListener::Ptr listener(new UpDownListener(up_future, down_future, target_host));

  initializer->with_settings(settings)->with_listener(listener.get())->initialize(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_FALSE(connect_future->error());

  ASSERT_TRUE(down_future->wait_for(WAIT_FOR_TIME));

  ASSERT_EQ(cluster.start(1), 0);
  ASSERT_TRUE(up_future->wait_for(WAIT_FOR_TIME));
}

TEST_F(RequestProcessorUnitTest, InvalidAuth) {
  mockssandra::SimpleCluster cluster(auth(), NUM_NODES);
  ASSERT_EQ(cluster.start_all(), 0);

  HostMap hosts(generate_hosts());

  Future::Ptr connect_future(new Future());
  RequestProcessorInitializer::Ptr initializer(new RequestProcessorInitializer(
      hosts.begin()->second, PROTOCOL_VERSION, hosts, TokenMap::Ptr(), "",
      bind_callback(on_connected, connect_future.get())));

  RequestProcessorSettings settings;
  settings.connection_pool_settings.connection_settings.auth_provider.reset(
      new PlainTextAuthProvider("invalid", "invalid"));

  CriticalErrorListener listener; // Use stack allocation because it's never closed

  initializer->with_settings(settings)->with_listener(&listener)->initialize(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  ASSERT_TRUE(connect_future->error());
  EXPECT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, connect_future->error()->code);
  EXPECT_EQ(3u, listener.count(Connector::CONNECTION_ERROR_AUTH));
}

TEST_F(RequestProcessorUnitTest, InvalidSsl) {
  mockssandra::SimpleCluster cluster(simple(), NUM_NODES);
  use_ssl(&cluster);
  ASSERT_EQ(cluster.start_all(), 0);

  HostMap hosts(generate_hosts());

  Future::Ptr connect_future(new Future());
  RequestProcessorInitializer::Ptr initializer(new RequestProcessorInitializer(
      hosts.begin()->second, PROTOCOL_VERSION, hosts, TokenMap::Ptr(), "",
      bind_callback(on_connected, connect_future.get())));

  SslContext::Ptr ssl_context(SslContextFactory::create()); // No trusted cert

  RequestProcessorSettings settings;
  settings.connection_pool_settings.connection_settings.socket_settings.ssl_context = ssl_context;

  CriticalErrorListener listener; // Use stack allocation because it's never closed

  initializer->with_settings(settings)->with_listener(&listener)->initialize(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  ASSERT_TRUE(connect_future->error());
  EXPECT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, connect_future->error()->code);
  EXPECT_EQ(3u, listener.count(Connector::CONNECTION_ERROR_SSL_VERIFY));
}

TEST_F(RequestProcessorUnitTest, RollingRestart) {
  mockssandra::SimpleCluster cluster(simple(), NUM_NODES);
  ASSERT_EQ(cluster.start_all(), 0);

  OutagePlan outage_plan(loop(), &cluster);
  // Multiple rolling restarts
  for (int i = 1; i <= NUM_NODES * 3; ++i) {
    int node = i % NUM_NODES;
    outage_plan.stop_node(node);
    outage_plan.start_node(node);
  }

  Future::Ptr close_future(new Future());
  CloseListener::Ptr listener(new CloseListener(close_future));

  HostMap hosts(generate_hosts());
  Future::Ptr connect_future(new Future());
  RequestProcessorInitializer::Ptr initializer(new RequestProcessorInitializer(
      hosts.begin()->second, PROTOCOL_VERSION, hosts, TokenMap::Ptr(), "",
      bind_callback(on_connected, connect_future.get())));

  RequestProcessorSettings settings;
  settings.connection_pool_settings.reconnection_policy.reset(
      new ConstantReconnectionPolicy(10)); // Reconnect immediately

  initializer->with_settings(settings)->with_listener(listener.get())->initialize(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_FALSE(connect_future->error());
  RequestProcessor::Ptr processor(connect_future->processor());
  Future::Ptr outage_future = execute_outage_plan(&outage_plan);

  while (!outage_future->wait_for(1000)) {     // 1 millisecond wait
    try_request(processor, WAIT_FOR_TIME * 3); // Increase wait time for chaotic tests
  }

  processor->close();
  ASSERT_TRUE(close_future->wait_for(WAIT_FOR_TIME));
}

TEST_F(RequestProcessorUnitTest, NoHostsAvailable) {
  mockssandra::SimpleCluster cluster(simple(), NUM_NODES);
  ASSERT_EQ(cluster.start_all(), 0);

  Future::Ptr close_future(new Future());
  CloseListener::Ptr listener(new CloseListener(close_future));

  HostMap hosts(generate_hosts());
  Future::Ptr connect_future(new Future());
  RequestProcessorInitializer::Ptr initializer(new RequestProcessorInitializer(
      hosts.begin()->second, PROTOCOL_VERSION, hosts, TokenMap::Ptr(), "",
      bind_callback(on_connected, connect_future.get())));

  initializer->with_listener(listener.get())->initialize(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_FALSE(connect_future->error());
  RequestProcessor::Ptr processor(connect_future->processor());

  ResponseFuture::Ptr response_future(new ResponseFuture());
  Request::ConstPtr request(new QueryRequest("SELECT * FROM table"));
  RequestHandler::Ptr request_handler(new RequestHandler(request, response_future));

  cluster.stop_all();

  processor->process_request(request_handler);
  ASSERT_TRUE(response_future->wait_for(WAIT_FOR_TIME));
  ASSERT_TRUE(response_future->error());
  ASSERT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, response_future->error()->code);

  processor->close();
  ASSERT_TRUE(close_future->wait_for(WAIT_FOR_TIME));
}

TEST_F(RequestProcessorUnitTest, RequestTimeout) {
  mockssandra::SimpleRequestHandlerBuilder builder;
  builder.on(mockssandra::OPCODE_QUERY).wait(100); // Create a delay for all queries
  mockssandra::SimpleCluster cluster(builder.build(), NUM_NODES);
  ASSERT_EQ(cluster.start_all(), 0);

  Future::Ptr close_future(new Future());
  CloseListener::Ptr listener(new CloseListener(close_future));

  HostMap hosts(generate_hosts());
  Future::Ptr connect_future(new Future());
  RequestProcessorInitializer::Ptr initializer(new RequestProcessorInitializer(
      hosts.begin()->second, PROTOCOL_VERSION, hosts, TokenMap::Ptr(), "",
      bind_callback(on_connected, connect_future.get())));

  initializer->with_listener(listener.get())->initialize(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_FALSE(connect_future->error());
  RequestProcessor::Ptr processor(connect_future->processor());

  ResponseFuture::Ptr response_future(new ResponseFuture());
  QueryRequest::Ptr query_request(new QueryRequest("SELECT * FROM table"));
  query_request->set_request_timeout_ms(50); // Small request timeout window (smaller than delay)
  Request::ConstPtr request(query_request);
  RequestHandler::Ptr request_handler(new RequestHandler(request, response_future));

  processor->process_request(request_handler);
  ASSERT_TRUE(response_future->wait_for(WAIT_FOR_TIME));
  ASSERT_TRUE(response_future->error());
  ASSERT_EQ(CASS_ERROR_LIB_REQUEST_TIMED_OUT, response_future->error()->code);

  processor->close();
  ASSERT_TRUE(close_future->wait_for(WAIT_FOR_TIME));
}

TEST_F(RequestProcessorUnitTest, LowNumberOfStreams) {
  mockssandra::SimpleRequestHandlerBuilder builder;
  builder.on(mockssandra::OPCODE_QUERY)
      .wait(1000) // Give  time for the streams to run out
      .system_local()
      .system_peers()
      .empty_rows_result(1);
  mockssandra::SimpleCluster cluster(builder.build(), 2); // Two node cluster
  ASSERT_EQ(cluster.start_all(), 0);

  Future::Ptr close_future(new Future());
  CloseListener::Ptr listener(new CloseListener(close_future));

  HostMap hosts(generate_hosts(2));
  Future::Ptr connect_future(new Future());

  ExecutionProfile profile;
  profile.set_load_balancing_policy(new InorderLoadBalancingPolicy());
  profile.set_speculative_execution_policy(new NoSpeculativeExecutionPolicy());
  profile.set_retry_policy(new DefaultRetryPolicy());

  RequestProcessorSettings settings;
  settings.default_profile = profile;
  settings.request_queue_size = 2 * CASS_MAX_STREAMS + 1; // Create a request queue with enough room

  RequestProcessorInitializer::Ptr initializer(new RequestProcessorInitializer(
      hosts.begin()->second, PROTOCOL_VERSION, hosts, TokenMap::Ptr(), "",
      bind_callback(on_connected, connect_future.get())));
  initializer->with_settings(settings)->with_listener(listener.get())->initialize(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_FALSE(connect_future->error());
  RequestProcessor::Ptr processor(connect_future->processor());

  // Saturate the hosts connections, but leave one stream.
  for (int i = 0; i < 2 * CASS_MAX_STREAMS - 1; ++i) {
    ResponseFuture::Ptr response_future(new ResponseFuture());
    Statement::Ptr request(new QueryRequest("SELECT * FROM table"));
    RequestHandler::Ptr request_handler(new RequestHandler(request, response_future));
    processor->process_request(request_handler);
  }

  { // Try two more requests. One should succeed on "127.0.0.2" and the other should fail (out of
    // streams).
    ResponseFuture::Ptr response_future(new ResponseFuture());

    Statement::Ptr request(new QueryRequest("SELECT * FROM table"));
    request->set_record_attempted_addresses(true);
    RequestHandler::Ptr request_handler(new RequestHandler(request, response_future));
    processor->process_request(request_handler);

    ResponseFuture::Ptr response_future_fail(new ResponseFuture());
    RequestHandler::Ptr request_handler_fail(new RequestHandler(
        Statement::Ptr(new QueryRequest("SELECT * FROM table")), response_future_fail));
    processor->process_request(request_handler_fail);
    ASSERT_TRUE(response_future_fail->wait_for(WAIT_FOR_TIME));
    ASSERT_TRUE(response_future_fail->error());
    EXPECT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, response_future_fail->error()->code);

    ASSERT_TRUE(response_future->wait_for(WAIT_FOR_TIME));
    EXPECT_FALSE(response_future->error());
    AddressVec attempted = response_future->attempted_addresses();
    ASSERT_GE(attempted.size(), 1u);
    EXPECT_EQ(attempted[0], Address("127.0.0.2", PORT));
  }

  processor->close();
  ASSERT_TRUE(close_future->wait_for(WAIT_FOR_TIME));
}
