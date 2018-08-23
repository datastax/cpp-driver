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

#include <gtest/gtest.h>

#include "mockssandra_test.hpp"
#include "test_utils.hpp"

#include "event_loop.hpp"
#include "query_request.hpp"
#include "request_handler.hpp"
#include "request_processor_initializer.hpp"
#include "ref_counted.hpp"

using namespace cass;

#define PROTOCOL_VERSION CASS_HIGHEST_SUPPORTED_PROTOCOL_VERSION
#define PORT 9042
#define WAIT_FOR_TIME 5 * 1000 * 1000 // 5 seconds

class RequestProcessorUnitTest : public mockssandra::SimpleClusterTest {
public:
  RequestProcessorUnitTest()
    : mockssandra::SimpleClusterTest(3,
                                     mockssandra::SimpleRequestHandlerBuilder().build()) { }

  EventLoop* event_loop() { return &event_loop_; }

  virtual void SetUp() {
    mockssandra::SimpleClusterTest::SetUp();
    ASSERT_EQ(0, event_loop_.init());
    ASSERT_EQ(0, event_loop_.run());
  }

  virtual void TearDown() {
    mockssandra::SimpleClusterTest::TearDown();
    event_loop_.close_handles();
    event_loop_.join();
  }

  HostMap generate_hosts() {
    HostMap hosts;
    Host::Ptr host1(Memory::allocate<Host>(Address("127.0.0.1", PORT)));
    Host::Ptr host2(Memory::allocate<Host>(Address("127.0.0.2", PORT)));
    Host::Ptr host3(Memory::allocate<Host>(Address("127.0.0.3", PORT)));
    hosts[host1->address()] = host1;
    hosts[host2->address()] = host2;
    hosts[host3->address()] = host3;
    return hosts;
  }

  void try_request(const RequestProcessor::Ptr& processor) {
    ResponseFuture::Ptr response_future(Memory::allocate<ResponseFuture>());
    Request::ConstPtr request(Memory::allocate<QueryRequest>("SELECT * FROM table"));
    RequestHandler::Ptr request_handler(Memory::allocate<RequestHandler>(request, response_future));

    processor->process_request(request_handler);

    ASSERT_TRUE(response_future->wait_for(WAIT_FOR_TIME));
    EXPECT_FALSE(response_future->error());
  }

  class Future : public cass::Future {
  public:
    typedef SharedRefPtr<Future> Ptr;

    Future()
      : cass::Future(FUTURE_TYPE_GENERIC) { }

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
      : close_future_(close_future) {
      inc_ref();
    }

    virtual ~CloseListener() { }

    virtual void on_pool_up(const Address& address)  { }
    virtual void on_pool_down(const Address& address) { }
    virtual void on_pool_critical_error(const Address& address,
                                        Connector::ConnectionError code,
                                        const String& message) { }
    virtual void on_keyspace_changed(const String& keyspace,
                                     const KeyspaceChangedHandler::Ptr& handler) { }
    virtual void on_prepared_metadata_changed(const String& id,
                                              const PreparedMetadata::Entry::Ptr& entry) { }

    virtual void on_close(RequestProcessor* processor) {
      if (close_future_) {
        close_future_->set();
      }
      dec_ref();
    }

  private:
    Future::Ptr close_future_;
  };

  class CriticalErrorListener : public RequestProcessorListener {
  public:
    size_t count(Connector::ConnectionError code) const {
      return std::count(error_codes_.begin(), error_codes_.end(), code);
    }

    virtual void on_pool_up(const Address& address)  { }
    virtual void on_pool_down(const Address& address) { }
    virtual void on_pool_critical_error(const Address& address,
                                        Connector::ConnectionError code,
                                        const String& message) {
      error_codes_.push_back(code);
    }
    virtual void on_keyspace_changed(const String& keyspace,
                                     const KeyspaceChangedHandler::Ptr& handler) { }
    virtual void on_prepared_metadata_changed(const String& id,
                                              const PreparedMetadata::Entry::Ptr& entry) { }
    virtual void on_close(RequestProcessor* processor) { }

  private:
    Vector<Connector::ConnectionError> error_codes_;
  };

  class UpDownListener : public CloseListener {
  public:
    typedef SharedRefPtr<UpDownListener> Ptr;

    UpDownListener(const Future::Ptr& up_future,
                   const Future::Ptr& down_future,
                   const Host::Ptr& target_host = Host::Ptr())
      : up_future_(up_future)
      , down_future_(down_future)
      , target_host_(target_host) { }

    virtual void on_pool_up(const Address& address)  {
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
        case RequestProcessorInitializer::RequestProcessorError::REQUEST_PROCESSOR_ERROR_KEYSPACE:
          future->set_error(CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE,
                            initializer->error_message());
          break;
        case RequestProcessorInitializer::RequestProcessorError::REQUEST_PROCESSOR_ERROR_NO_HOSTS_AVAILABLE:
          future->set_error(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
                            "Unable to connect to any contact points");
          break;
        case RequestProcessorInitializer::RequestProcessorError::REQUEST_PROCESSOR_ERROR_UNABLE_TO_INIT:
          future->set_error(CASS_ERROR_LIB_UNABLE_TO_INIT,
                            initializer->error_message());
          break;
        default:
          future->set_error(CASS_ERROR_LIB_UNABLE_TO_CONNECT,
                            initializer->error_message());
          break;
      }
    }
  }

private:
  EventLoop event_loop_;
};

TEST_F(RequestProcessorUnitTest, Simple) {
  start_all();

  HostMap hosts(generate_hosts());

  Future::Ptr connect_future(Memory::allocate<Future>());
  RequestProcessorInitializer::Ptr initializer(Memory::allocate<RequestProcessorInitializer>(hosts.begin()->second,
                                                                                             PROTOCOL_VERSION,
                                                                                             hosts,
                                                                                             TokenMap::Ptr(),
                                                                                             bind_callback(on_connected, connect_future.get())));

  initializer->initialize(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_FALSE(connect_future->error());

  try_request(connect_future->processor());
}

TEST_F(RequestProcessorUnitTest, CloseWithRequestsPending) {
  start_all();

  HostMap hosts(generate_hosts());

  Future::Ptr connect_future(Memory::allocate<Future>());
  RequestProcessorInitializer::Ptr initializer(Memory::allocate<RequestProcessorInitializer>(hosts.begin()->second,
                                                                                             PROTOCOL_VERSION,
                                                                                             hosts,
                                                                                             TokenMap::Ptr(),
                                                                                             bind_callback(on_connected, connect_future.get())));

  initializer->initialize(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_FALSE(connect_future->error());

  Vector<ResponseFuture::Ptr> futures;
  RequestProcessor::Ptr processor(connect_future->processor());

  for (int i = 0; i < 4096; ++i) {
    ResponseFuture::Ptr response_future(Memory::allocate<ResponseFuture>());
    Request::ConstPtr request(Memory::allocate<QueryRequest>("SELECT * FROM table"));
    RequestHandler::Ptr request_handler(Memory::allocate<RequestHandler>(request, response_future));

    processor->process_request(request_handler);
    futures.push_back(response_future);
  }

  processor->close();

  for (Vector<ResponseFuture::Ptr>::const_iterator it = futures.begin(),
       end = futures.end(); it != end; ++it) {
    ResponseFuture::Ptr response_future(*it);
    ASSERT_TRUE(response_future->wait_for(WAIT_FOR_TIME));
    EXPECT_FALSE(response_future->error());
  }
}

TEST_F(RequestProcessorUnitTest, Auth) {
  mockssandra::SimpleCluster cluster(
        mockssandra::AuthRequestHandlerBuilder().build(), 3);
  cluster.start_all();

  HostMap hosts(generate_hosts());

  Future::Ptr connect_future(Memory::allocate<Future>());
  RequestProcessorInitializer::Ptr initializer(Memory::allocate<RequestProcessorInitializer>(hosts.begin()->second,
                                                                                             PROTOCOL_VERSION,
                                                                                             hosts,
                                                                                             TokenMap::Ptr(),
                                                                                             bind_callback(on_connected, connect_future.get())));

  RequestProcessorSettings settings;
  settings.connection_pool_settings.connection_settings.auth_provider.reset(
        Memory::allocate<PlainTextAuthProvider>("cassandra", "cassandra"));

  initializer
      ->with_settings(settings)
      ->initialize(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_FALSE(connect_future->error());

  try_request(connect_future->processor());
}

TEST_F(RequestProcessorUnitTest, Ssl) {
  RequestProcessorSettings settings;
  settings.connection_pool_settings.connection_settings =  use_ssl();

  start_all();

  HostMap hosts(generate_hosts());

  Future::Ptr connect_future(Memory::allocate<Future>());
  RequestProcessorInitializer::Ptr initializer(Memory::allocate<RequestProcessorInitializer>(hosts.begin()->second,
                                                                                             PROTOCOL_VERSION,
                                                                                             hosts,
                                                                                             TokenMap::Ptr(),
                                                                                             bind_callback(on_connected, connect_future.get())));

  initializer
      ->with_settings(settings)
      ->initialize(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_FALSE(connect_future->error());

  try_request(connect_future->processor());
}

TEST_F(RequestProcessorUnitTest, NotifyAddRemoveHost) {
  start_all();

  HostMap hosts(generate_hosts());

  Host::Ptr to_add_remove(hosts.begin()->second);
  hosts.erase(to_add_remove->address()); // Remove to add/remove it back later

  Future::Ptr connect_future(Memory::allocate<Future>());
  Future::Ptr up_future(Memory::allocate<Future>());
  Future::Ptr down_future(Memory::allocate<Future>());
  RequestProcessorInitializer::Ptr initializer(Memory::allocate<RequestProcessorInitializer>(hosts.begin()->second,
                                                                                             PROTOCOL_VERSION,
                                                                                             hosts,
                                                                                             TokenMap::Ptr(),
                                                                                             bind_callback(on_connected, connect_future.get())));

  RequestProcessorSettings settings;
  settings.connection_pool_settings.reconnect_wait_time_ms = 1; // Reconnect immediately

  UpDownListener::Ptr listener(Memory::allocate<UpDownListener>(up_future, down_future, to_add_remove));

  initializer
      ->with_settings(settings)
      ->with_listener(listener.get())
      ->initialize(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_FALSE(connect_future->error());

  RequestProcessor::Ptr processor(connect_future->processor());

  processor->notify_host_add(to_add_remove);
  ASSERT_TRUE(up_future->wait_for(WAIT_FOR_TIME));

  processor->notify_host_remove(to_add_remove);
  ASSERT_TRUE(down_future->wait_for(WAIT_FOR_TIME));
}

TEST_F(RequestProcessorUnitTest, CloseDuringReconnect) {
  start_all();

  HostMap hosts(generate_hosts());

  Future::Ptr close_future(Memory::allocate<Future>());
  Future::Ptr connect_future(Memory::allocate<Future>());
  RequestProcessorInitializer::Ptr initializer(Memory::allocate<RequestProcessorInitializer>(hosts.begin()->second,
                                                                                             PROTOCOL_VERSION,
                                                                                             hosts,
                                                                                             TokenMap::Ptr(),
                                                                                             bind_callback(on_connected, connect_future.get())));

  RequestProcessorSettings settings;
  settings.connection_pool_settings.reconnect_wait_time_ms = 100000; // Make sure we're reconnecting when we close.

  CloseListener::Ptr listener(Memory::allocate<CloseListener>(close_future));

  initializer
      ->with_settings(settings)
      ->with_listener(listener.get())
      ->initialize(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_FALSE(connect_future->error());

  RequestProcessor::Ptr processor(connect_future->processor());

  stop(1);
  test::Utils::msleep(200); // Give time for the reconnect to start
  processor->close();

  ASSERT_TRUE(close_future->wait_for(WAIT_FOR_TIME));
}

TEST_F(RequestProcessorUnitTest, CloseDuringAddNewHost) {
  start_all();

  HostMap hosts(generate_hosts());

  Host::Ptr to_add(hosts.begin()->second);
  hosts.erase(to_add->address()); // Remove to add it back later

  Future::Ptr close_future(Memory::allocate<Future>());
  Future::Ptr connect_future(Memory::allocate<Future>());
  RequestProcessorInitializer::Ptr initializer(Memory::allocate<RequestProcessorInitializer>(hosts.begin()->second,
                                                                                             PROTOCOL_VERSION,
                                                                                             hosts,
                                                                                             TokenMap::Ptr(),
                                                                                             bind_callback(on_connected, connect_future.get())));

  CloseListener::Ptr listener(Memory::allocate<CloseListener>(close_future));

  initializer
      ->with_listener(listener.get())
      ->initialize(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_FALSE(connect_future->error());

  RequestProcessor::Ptr processor(connect_future->processor());

  processor->notify_host_add(to_add);
  processor->close();

  ASSERT_TRUE(close_future->wait_for(WAIT_FOR_TIME));
}

TEST_F(RequestProcessorUnitTest, PoolDown) {
  start_all();

  HostMap hosts(generate_hosts());
  Host::Ptr target_host(hosts.find(Address("127.0.0.1", PORT))->second);
  ASSERT_TRUE(target_host);

  Future::Ptr connect_future(Memory::allocate<Future>());
  Future::Ptr up_future(Memory::allocate<Future>());
  Future::Ptr down_future(Memory::allocate<Future>());
  RequestProcessorInitializer::Ptr initializer(Memory::allocate<RequestProcessorInitializer>(hosts.begin()->second,
                                                                                             PROTOCOL_VERSION,
                                                                                             hosts,
                                                                                             TokenMap::Ptr(),
                                                                                             bind_callback(on_connected, connect_future.get())));

  UpDownListener::Ptr listener(Memory::allocate<UpDownListener>(up_future, down_future, target_host));

  initializer
      ->with_listener(listener.get())
      ->initialize(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_FALSE(connect_future->error());

  ASSERT_TRUE(up_future->wait_for(WAIT_FOR_TIME));

  stop(1);
  ASSERT_TRUE(down_future->wait_for(WAIT_FOR_TIME));
}

TEST_F(RequestProcessorUnitTest, PoolUp) {
  // Only start specific nodes
  start(2);
  start(3);

  HostMap hosts(generate_hosts());
  Host::Ptr target_host(hosts.find(Address("127.0.0.1", PORT))->second);
  ASSERT_TRUE(target_host);

  Future::Ptr connect_future(Memory::allocate<Future>());
  Future::Ptr up_future(Memory::allocate<Future>());
  Future::Ptr down_future(Memory::allocate<Future>());
  RequestProcessorInitializer::Ptr initializer(Memory::allocate<RequestProcessorInitializer>(hosts.begin()->second,
                                                                                             PROTOCOL_VERSION,
                                                                                             hosts,
                                                                                             TokenMap::Ptr(),
                                                                                             bind_callback(on_connected, connect_future.get())));

  RequestProcessorSettings settings;
  settings.connection_pool_settings.reconnect_wait_time_ms = 1; // Reconnect immediately

  UpDownListener::Ptr listener(Memory::allocate<UpDownListener>(up_future, down_future, target_host));

  initializer
      ->with_settings(settings)
      ->with_listener(listener.get())
      ->initialize(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_FALSE(connect_future->error());

  ASSERT_TRUE(down_future->wait_for(WAIT_FOR_TIME));

  start(1);
  ASSERT_TRUE(up_future->wait_for(WAIT_FOR_TIME));
}

TEST_F(RequestProcessorUnitTest, InvalidAuth) {
  mockssandra::SimpleCluster cluster(
        mockssandra::AuthRequestHandlerBuilder().build(), 3);
  cluster.start_all();

  HostMap hosts(generate_hosts());

  Future::Ptr connect_future(Memory::allocate<Future>());
  RequestProcessorInitializer::Ptr initializer(Memory::allocate<RequestProcessorInitializer>(hosts.begin()->second,
                                                                                             PROTOCOL_VERSION,
                                                                                             hosts,
                                                                                             TokenMap::Ptr(),
                                                                                             bind_callback(on_connected, connect_future.get())));

  RequestProcessorSettings settings;
  settings.connection_pool_settings.connection_settings.auth_provider.reset(
        Memory::allocate<PlainTextAuthProvider>("invalid", "invalid"));

  CriticalErrorListener listener; // Use stack allocation because it's never closed

  initializer
      ->with_settings(settings)
      ->with_listener(&listener)
      ->initialize(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  ASSERT_TRUE(connect_future->error());
  EXPECT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, connect_future->error()->code);
  EXPECT_EQ(3u, listener.count(Connector::CONNECTION_ERROR_AUTH));
}

TEST_F(RequestProcessorUnitTest, InvalidSsl) {
  use_ssl();
  start_all();

  HostMap hosts(generate_hosts());

  Future::Ptr connect_future(Memory::allocate<Future>());
  RequestProcessorInitializer::Ptr initializer(Memory::allocate<RequestProcessorInitializer>(hosts.begin()->second,
                                                                                             PROTOCOL_VERSION,
                                                                                             hosts,
                                                                                             TokenMap::Ptr(),
                                                                                             bind_callback(on_connected, connect_future.get())));

  SslContext::Ptr ssl_context(SslContextFactory::create()); // No trusted cert

  RequestProcessorSettings settings;
  settings.connection_pool_settings.connection_settings.socket_settings.ssl_context = ssl_context;

  CriticalErrorListener listener; // Use stack allocation because it's never closed

  initializer
      ->with_settings(settings)
      ->with_listener(&listener)
      ->initialize(event_loop());

  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  ASSERT_TRUE(connect_future->error());
  EXPECT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, connect_future->error()->code);
  EXPECT_EQ(3u, listener.count(Connector::CONNECTION_ERROR_SSL_VERIFY));
}
