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

#include "mockssandra.hpp"

#include "connection_pool_manager_initializer.hpp"
#include "event_loop.hpp"
#include "future.hpp"
#include "request_queue.hpp"

#define NUM_NODES 3
#define PROTOCOL_VERSION 4

using namespace cass;

class PoolUnitTest : public testing::Test {
public:

  template <class State>
  class Future : public cass::Future {
  public:
    Future(size_t count)
      : cass::Future(FUTURE_TYPE_GENERIC)
      , count_(count) { }

    int count(State state) {
      wait();
      int count = 0;
      for (typename std::vector<State>::const_iterator it = results_.begin(),
           end = results_.end(); it != end; ++it) {
        if (*it == state) count++;
      }
      return count;
    }

  protected:
    void set(State state) {
      cass::ScopedMutex lock(&mutex_);
      results_.push_back(state);
      if (results_.size() == count_) {
        Future::internal_set(lock);
      }
    }

  private:
    std::vector<State> results_;
    const size_t count_;
  };

  struct RequestState {
    enum Enum {
      SUCCESS,
      ERROR_NO_CONNECTION,
      ERROR_FAILED_WRITE,
      ERROR,
      ERROR_RESPONSE,
      TIMEOUT,
    };
  };

  class RequestFuture
      : public RequestState
      , public Future<RequestState::Enum> {
  public:
    typedef SharedRefPtr<RequestFuture> Ptr;

    RequestFuture(int num_nodes = NUM_NODES)
      : Future(num_nodes) { }

    void success() { set(SUCCESS); }
    void error_failed_write() { set(ERROR_FAILED_WRITE); }
    void error_no_connection() { set(ERROR_NO_CONNECTION); }
    void error() { set(ERROR); }
    void error_response() { set(ERROR_RESPONSE); }
    void timeout() { set(TIMEOUT); }
  };

  struct ListenerState {
    enum Enum {
      UP,
      DOWN,
      CRITICAL_ERROR,
      CRITICAL_ERROR_INVALID_PROTOCOL,
      CRITICAL_ERROR_KEYSPACE,
      CRITICAL_ERROR_AUTH,
      CRITICAL_ERROR_SSL_HANDSHAKE,
      CRITICAL_ERROR_SSL_VERIFY
    };
  };

  class ListenerFuture
      : public ListenerState
      , public Future<ListenerState::Enum> {
  public:
    typedef SharedRefPtr<ListenerFuture> Ptr;

    ListenerFuture(int num_nodes = NUM_NODES)
      : Future(num_nodes) { }

    void up() { set(UP); }
    void down() { set(DOWN); }
    void critical_error() { set(CRITICAL_ERROR); }
    void critical_error_invalid_protocol() { set(CRITICAL_ERROR_INVALID_PROTOCOL); }
    void critical_error_keyspace() { set(CRITICAL_ERROR_KEYSPACE); }
    void critical_error_auth() { set(CRITICAL_ERROR_AUTH); }
    void critical_error_ssl_handshake() { set(CRITICAL_ERROR_SSL_HANDSHAKE); }
    void critical_error_ssl_verify() { set(CRITICAL_ERROR_SSL_VERIFY); }
  };

  class Listener : public ConnectionPoolManagerListener {
  public:
    Listener(const ListenerFuture::Ptr& future)
      : future_(future) {
      uv_mutex_init(&mutex_);
    }

    ~Listener() {
      uv_mutex_destroy(&mutex_);
    }

    void reset(const ListenerFuture::Ptr& future) {
      ScopedMutex l(&mutex_);
      future_ = future;
    }

    virtual void on_up(const Address& address)  {
      ScopedMutex l(&mutex_);
      future_->up();
    }

    virtual void on_down(const Address& address) {
      ScopedMutex l(&mutex_);
      future_->down();
    }

    virtual void on_critical_error(const Address& address,
                                   Connector::ConnectionError code,
                                   const String& message)  {
      ScopedMutex l(&mutex_);
      switch (code) {
        case Connector::CONNECTION_ERROR_INVALID_PROTOCOL:
          future_->critical_error_invalid_protocol();
          break;
        case Connector::CONNECTION_ERROR_KEYSPACE:
          future_->critical_error_keyspace();
          break;
        case Connector::CONNECTION_ERROR_AUTH:
          future_->critical_error_auth();
          break;
        case Connector::CONNECTION_ERROR_SSL_HANDSHAKE:
          future_->critical_error_ssl_handshake();
          break;
        case Connector::CONNECTION_ERROR_SSL_VERIFY:
          future_->critical_error_ssl_verify();
          break;
        default:
          future_->critical_error();
          break;
      }

    }

    virtual void on_close() {
      Memory::deallocate(this);
    }

  private:
    uv_mutex_t mutex_;
    ListenerFuture::Ptr future_;
  };


  class RequestCallback : public SimpleRequestCallback {
  public:
    RequestCallback(RequestFuture* future)
      : SimpleRequestCallback("SELECT * FROM blah")
      , future_(future) {
      future->inc_ref();
    }

    ~RequestCallback() { future_->dec_ref(); }

    virtual void on_internal_set(ResponseMessage* response) {
      if (response->response_body()->opcode() == CQL_OPCODE_RESULT) {
        future_->success();
      } else {
        future_->error_response();
      }
    }

    virtual void on_internal_error(CassError code, const String& message) {
      future_->error();
    }

    virtual void on_internal_timeout() {
      future_->timeout();
    }

  private:
    RequestFuture* future_;
  };

  PoolUnitTest()
    : cluster_(mockssandra::SimpleRequestHandlerBuilder().build(), NUM_NODES)
    , event_loop_group_(1)
    , request_queue_manager_(&event_loop_group_) { }

  AddressVec addresses() const {
    mockssandra::Ipv4AddressGenerator generator;
    AddressVec addresses;
    for (int i = 0; i < NUM_NODES; ++i) {
      addresses.push_back(generator.next());
    }
    return addresses;
  }

  ConnectionPoolManagerSettings use_ssl() {
    SslContext::Ptr ssl_context(SslContextFactory::create());

    String cert = cluster_.use_ssl();
    EXPECT_FALSE(cert.empty()) << "Unable to enable SSL";
    EXPECT_EQ(ssl_context->add_trusted_cert(cert.data(), cert.size()), CASS_OK);

    ConnectionPoolManagerSettings settings;
    settings.connection_settings.socket_settings.ssl_context = ssl_context;
    settings.connection_settings.socket_settings.hostname_resolution_enabled = true;

    return settings;
  }

  void stop_all() {
    cluster_.stop_all();
  }

  void start_all() {
    if (cluster_.start_all() != 0) {
      cluster_.stop_all();
      ASSERT_TRUE(false) << "Unable to start cluster";
    }
  }

  void start(size_t node) {
    if (cluster_.start(node) != 0) {
      cluster_.stop_all();
      ASSERT_TRUE(false) << "Unable to start node " << node;
    }
  }

  void stop(size_t node) {
    cluster_.stop(node);
  }

  virtual void SetUp() {
    SslContextFactory::init();
    ASSERT_EQ(event_loop_group_.init(), 0);
    event_loop_group_.run();
    ASSERT_EQ(request_queue_manager_.init(1024), 0);
  }

  virtual void TearDown() {
    request_queue_manager_.close_handles();
    event_loop_group_.close_handles();
    event_loop_group_.join();
    stop_all();
  }

  RequestQueueManager* request_queue_manager() {
    return &request_queue_manager_;
  }

  void run_request(const ConnectionPoolManager::Ptr& manager, const Address& address) {
    PooledConnection::Ptr connection = manager->find_least_busy(address);
    if (connection) {
      RequestFuture::Ptr future(Memory::allocate<RequestFuture>(1));
      RequestCallback::Ptr callback(Memory::allocate<RequestCallback>(future.get()));
      EXPECT_TRUE(connection->write(callback.get())) << "Unable to write request to connection " << address.to_string();
      EXPECT_EQ(future->count(RequestFuture::SUCCESS), 1);
    } else {
      EXPECT_TRUE(false) << "No connection available for " << address.to_string();
    }
  }

  static void on_pool_connected(ConnectionPoolManagerInitializer* initializer) {
    RequestFuture* future = static_cast<RequestFuture*>(initializer->data());

    mockssandra::Ipv4AddressGenerator generator;
    for (int i = 0; i < NUM_NODES; ++i) {
      PooledConnection::Ptr connection = initializer->manager()->find_least_busy(generator.next());
      if (connection) {
        RequestCallback::Ptr callback(Memory::allocate<RequestCallback>(future));
        if(!connection->write(callback.get())) {
          future->error_failed_write();
        }
      } else {
        future->error_no_connection();
      }
    }
  }

  static void on_pool_cancelled(ConnectionPoolManagerInitializer* initializer) {
  }

  static void on_pool_nop(ConnectionPoolManagerInitializer* initializer) { }

private:
  mockssandra::SimpleCluster cluster_;
  RoundRobinEventLoopGroup event_loop_group_;
  RequestQueueManager request_queue_manager_;
};

TEST_F(PoolUnitTest, Simple) {
  start_all();

  RequestFuture::Ptr future(Memory::allocate<RequestFuture>());

  ConnectionPoolManagerInitializer::Ptr initializer(
        Memory::allocate<ConnectionPoolManagerInitializer>(
          request_queue_manager(),
          PROTOCOL_VERSION,
          static_cast<void*>(future.get()), on_pool_connected));

  initializer
      ->initialize(addresses());

  EXPECT_EQ(future->count(RequestFuture::SUCCESS), 3);

  initializer->manager()->close();
}

TEST_F(PoolUnitTest, Keyspace) {
  mockssandra::SimpleRequestHandlerBuilder builder;

  builder
      .on(mockssandra::OPCODE_QUERY)
      .use_keyspace("foo")
      .validate_query().void_result();
  mockssandra::SimpleCluster cluster(builder.build(), NUM_NODES);

  cluster.start_all();

  RequestFuture::Ptr future(Memory::allocate<RequestFuture>());

  ConnectionPoolManagerInitializer::Ptr initializer(
        Memory::allocate<ConnectionPoolManagerInitializer>(
          request_queue_manager(),
          PROTOCOL_VERSION,
          static_cast<void*>(future.get()), on_pool_connected));

  AddressVec addresses(this->addresses());

  initializer
      ->with_keyspace("foo")
      ->initialize(addresses);

  EXPECT_EQ(future->count(RequestFuture::SUCCESS), 3);

  ConnectionPoolManager::Ptr manager(initializer->manager());

  for (size_t i = 0; i < NUM_NODES; ++i) {
    PooledConnection::Ptr connection = manager->find_least_busy(addresses[i]);
    if (connection) {
      EXPECT_EQ(connection->keyspace(), "foo");
    } else {
      EXPECT_TRUE(false) << "Unable to get connection for " << addresses[i].to_string();
    }
  }

  manager->close();
}

TEST_F(PoolUnitTest, Auth) {
  mockssandra::SimpleRequestHandlerBuilder builder;
  builder
      .on(mockssandra::OPCODE_STARTUP)
      .authenticate("com.datastax.SomeAuthenticator");
  builder
      .on(mockssandra::OPCODE_AUTH_RESPONSE)
      .plaintext_auth("cassandra", "cassandra");
  mockssandra::SimpleCluster cluster(builder.build(), NUM_NODES);
  cluster.start_all();

  RequestFuture::Ptr future(Memory::allocate<RequestFuture>());

  ConnectionPoolManagerInitializer::Ptr initializer(
        Memory::allocate<ConnectionPoolManagerInitializer>(
          request_queue_manager(),
          PROTOCOL_VERSION,
          static_cast<void*>(future.get()), on_pool_connected));

  ConnectionPoolManagerSettings settings;
  settings.connection_settings.auth_provider.reset(Memory::allocate<PlainTextAuthProvider>("cassandra", "cassandra"));

  initializer
      ->with_settings(settings)
      ->initialize(addresses());

  EXPECT_EQ(future->count(RequestFuture::SUCCESS), 3);

  initializer->manager()->close();
}

TEST_F(PoolUnitTest, Ssl) {
  ConnectionPoolManagerSettings settings(use_ssl());

  start_all();

  RequestFuture::Ptr future(Memory::allocate<RequestFuture>());

  ConnectionPoolManagerInitializer::Ptr initializer(
        Memory::allocate<ConnectionPoolManagerInitializer>(
          request_queue_manager(),
          PROTOCOL_VERSION,
          static_cast<void*>(future.get()), on_pool_connected));

  initializer
      ->with_settings(settings)
      ->initialize(addresses());

  EXPECT_EQ(future->count(RequestFuture::SUCCESS), 3);

  initializer->manager()->close();
}

TEST_F(PoolUnitTest, Listener) {
  start_all();

  ListenerFuture::Ptr future(Memory::allocate<ListenerFuture>());

  ConnectionPoolManagerInitializer::Ptr initializer(
        Memory::allocate<ConnectionPoolManagerInitializer>(
          request_queue_manager(),
          PROTOCOL_VERSION,
          static_cast<void*>(NULL), on_pool_nop));

  initializer
      ->with_listener(Memory::allocate<Listener>(future))
      ->initialize(addresses());

  EXPECT_EQ(future->count(ListenerFuture::UP), 3);
  EXPECT_EQ(initializer->failures().size(), 0u);

  initializer->manager()->close();
}

TEST_F(PoolUnitTest, ListenerDown) {
  start(1);

  ListenerFuture::Ptr future(Memory::allocate<ListenerFuture>());

  ConnectionPoolManagerInitializer::Ptr initializer(
        Memory::allocate<ConnectionPoolManagerInitializer>(
          request_queue_manager(),
          PROTOCOL_VERSION,
          static_cast<void*>(NULL), on_pool_nop));

  initializer
      ->with_listener(Memory::allocate<Listener>(future))
      ->initialize(addresses());

  EXPECT_EQ(future->count(ListenerFuture::UP), 1);
  EXPECT_EQ(future->count(ListenerFuture::DOWN), 2);
  EXPECT_EQ(initializer->failures().size(), 0u);

  initializer->manager()->close();
}

TEST_F(PoolUnitTest, AddRemove) {
  start_all();

  ListenerFuture::Ptr future(Memory::allocate<ListenerFuture>());

  ConnectionPoolManagerInitializer::Ptr initializer(
        Memory::allocate<ConnectionPoolManagerInitializer>(
          request_queue_manager(),
          PROTOCOL_VERSION,
          static_cast<void*>(NULL), on_pool_nop));

  AddressVec addresses(this->addresses());

  initializer
      ->with_listener(Memory::allocate<Listener>(future))
      ->initialize(addresses);

  EXPECT_EQ(future->count(ListenerFuture::UP), 3);

  ConnectionPoolManager::Ptr manager(initializer->manager());

  for (size_t i = 0; i < NUM_NODES; ++i) {
    future.reset(Memory::allocate<ListenerFuture>(1));
    static_cast<Listener*>(manager->listener())->reset(future);

    manager->remove(addresses[i]); // Remove node
    EXPECT_EQ(future->count(ListenerFuture::DOWN), 1);
    EXPECT_FALSE(manager->find_least_busy(addresses[i]));

    future.reset(Memory::allocate<ListenerFuture>(1));
    static_cast<Listener*>(manager->listener())->reset(future);

    manager->add(addresses[i]); // Add node
    EXPECT_EQ(future->count(ListenerFuture::UP), 1);
    run_request(manager, addresses[i]);
  }

  manager->close();
}

TEST_F(PoolUnitTest, Reconnect) {
  start_all();

  ListenerFuture::Ptr future(Memory::allocate<ListenerFuture>());

  ConnectionPoolManagerInitializer::Ptr initializer(
        Memory::allocate<ConnectionPoolManagerInitializer>(
          request_queue_manager(),
          PROTOCOL_VERSION,
          static_cast<void*>(NULL), on_pool_nop));

  AddressVec addresses(this->addresses());

  initializer
      ->with_listener(Memory::allocate<Listener>(future))
      ->initialize(addresses);

  EXPECT_EQ(future->count(ListenerFuture::UP), 3);

  ConnectionPoolManager::Ptr manager(initializer->manager());

  for (size_t i = 0; i < NUM_NODES; ++i) {
    future.reset(Memory::allocate<ListenerFuture>(1));
    static_cast<Listener*>(manager->listener())->reset(future);

    stop(i + 1); // Stop node
    EXPECT_EQ(future->count(ListenerFuture::DOWN), 1);
    EXPECT_FALSE(manager->find_least_busy(addresses[i]));

    future.reset(Memory::allocate<ListenerFuture>(1));
    static_cast<Listener*>(manager->listener())->reset(future);

    start(i + 1); // Start node
    EXPECT_EQ(future->count(ListenerFuture::UP), 1);
    run_request(manager, addresses[i]);
  }

  manager->close();
}

TEST_F(PoolUnitTest, Timeout) {
  mockssandra::RequestHandler::Builder builder;
  builder.on(mockssandra::OPCODE_STARTUP).no_result(); // Don't return a response

  mockssandra::SimpleCluster cluster(builder.build(), NUM_NODES);
  cluster.start_all();

  ListenerFuture::Ptr future(Memory::allocate<ListenerFuture>());

  ConnectionPoolManagerInitializer::Ptr initializer(
        Memory::allocate<ConnectionPoolManagerInitializer>(
          request_queue_manager(),
          PROTOCOL_VERSION,
          static_cast<void*>(NULL), on_pool_nop));

  ConnectionPoolManagerSettings settings;
  settings.connection_settings.connect_timeout_ms = 1000;

  initializer
      ->with_settings(settings)
      ->with_listener(Memory::allocate<Listener>(future))
      ->initialize(addresses());

  EXPECT_EQ(future->count(ListenerFuture::DOWN), 3);

  initializer->manager()->close();
}

TEST_F(PoolUnitTest, InvalidProtocol) {
  start_all();

  ListenerFuture::Ptr future(Memory::allocate<ListenerFuture>());

  ConnectionPoolManagerInitializer::Ptr initializer(
        Memory::allocate<ConnectionPoolManagerInitializer>(
          request_queue_manager(),
          0x7F,  // Invalid protocol version
          static_cast<void*>(NULL), on_pool_nop));

  initializer
      ->with_listener(Memory::allocate<Listener>(future))
      ->initialize(addresses());

  EXPECT_EQ(future->count(ListenerFuture::CRITICAL_ERROR_INVALID_PROTOCOL), 3);

  ConnectionPoolConnector::Vec failures = initializer->failures();
  EXPECT_EQ(failures.size(), 3u);

  for (ConnectionPoolConnector::Vec::const_iterator it = failures.begin(),
       end = failures.end(); it != end; ++it) {
    EXPECT_EQ((*it)->error_code(), Connector::CONNECTION_ERROR_INVALID_PROTOCOL);
  }

  initializer->manager()->close();
}

TEST_F(PoolUnitTest, InvalidKeyspace) {
  mockssandra::SimpleRequestHandlerBuilder builder;
  builder
      .on(mockssandra::OPCODE_QUERY)
      .use_keyspace("foo")
      .validate_query().void_result();
  mockssandra::SimpleCluster cluster(builder.build(), NUM_NODES);

  cluster.start_all();

  ListenerFuture::Ptr future(Memory::allocate<ListenerFuture>());

  ConnectionPoolManagerInitializer::Ptr initializer(
        Memory::allocate<ConnectionPoolManagerInitializer>(
          request_queue_manager(),
          PROTOCOL_VERSION,
          static_cast<void*>(future.get()), on_pool_connected));

  initializer
      ->with_keyspace("invalid")
      ->with_listener(Memory::allocate<Listener>(future))
      ->initialize(addresses());

  EXPECT_EQ(future->count(ListenerFuture::CRITICAL_ERROR_KEYSPACE), 3);

  initializer->manager()->close();
}

TEST_F(PoolUnitTest, InvalidAuth) {
  mockssandra::SimpleRequestHandlerBuilder builder;
  builder
      .on(mockssandra::OPCODE_STARTUP)
      .authenticate("com.datastax.SomeAuthenticator");
  builder
      .on(mockssandra::OPCODE_AUTH_RESPONSE)
      .plaintext_auth("cassandra", "cassandra");
  mockssandra::SimpleCluster cluster(builder.build(), NUM_NODES);
  cluster.start_all();

  ListenerFuture::Ptr future(Memory::allocate<ListenerFuture>());

  ConnectionPoolManagerInitializer::Ptr initializer(
        Memory::allocate<ConnectionPoolManagerInitializer>(
          request_queue_manager(),
          PROTOCOL_VERSION,
          static_cast<void*>(future.get()), on_pool_connected));

  ConnectionPoolManagerSettings settings;
  settings.connection_settings.auth_provider.reset(Memory::allocate<PlainTextAuthProvider>("invalid", "invalid"));

  initializer
      ->with_settings(settings)
      ->with_listener(Memory::allocate<Listener>(future))
      ->initialize(addresses());

  EXPECT_EQ(future->count(ListenerFuture::CRITICAL_ERROR_AUTH), 3);

  initializer->manager()->close();
}

TEST_F(PoolUnitTest, InvalidNoSsl) {
  start_all(); // Start without ssl

  ListenerFuture::Ptr future(Memory::allocate<ListenerFuture>());

  ConnectionPoolManagerInitializer::Ptr initializer(
        Memory::allocate<ConnectionPoolManagerInitializer>(
          request_queue_manager(),
          PROTOCOL_VERSION,
          static_cast<void*>(future.get()), on_pool_connected));

  SslContext::Ptr ssl_context(SslContextFactory::create());

  ConnectionPoolManagerSettings settings;
  settings.connection_settings.socket_settings.ssl_context = ssl_context;
  settings.connection_settings.socket_settings.hostname_resolution_enabled = true;

  initializer
      ->with_settings(settings)
      ->with_listener(Memory::allocate<Listener>(future))
      ->initialize(addresses());

  EXPECT_EQ(future->count(ListenerFuture::CRITICAL_ERROR_SSL_HANDSHAKE), 3);

  initializer->manager()->close();
}

TEST_F(PoolUnitTest, InvalidSsl) {
  use_ssl();
  start_all();

  ListenerFuture::Ptr future(Memory::allocate<ListenerFuture>());

  ConnectionPoolManagerInitializer::Ptr initializer(
        Memory::allocate<ConnectionPoolManagerInitializer>(
          request_queue_manager(),
          PROTOCOL_VERSION,
          static_cast<void*>(future.get()), on_pool_connected));

  SslContext::Ptr ssl_context(SslContextFactory::create()); // No trusted cert

  ConnectionPoolManagerSettings settings;
  settings.connection_settings.socket_settings.ssl_context = ssl_context;
  settings.connection_settings.socket_settings.hostname_resolution_enabled = true;

  initializer
      ->with_settings(settings)
      ->with_listener(Memory::allocate<Listener>(future))
      ->initialize(addresses());

  EXPECT_EQ(future->count(ListenerFuture::CRITICAL_ERROR_SSL_VERIFY), 3);

  initializer->manager()->close();
}

TEST_F(PoolUnitTest, PartialReconnect) {
  // TODO:
}

TEST_F(PoolUnitTest, LowNumberOfStreams) {
  // TODO:
}
