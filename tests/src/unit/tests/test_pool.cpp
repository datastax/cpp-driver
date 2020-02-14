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

#include "loop_test.hpp"

#include "connection_pool_manager_initializer.hpp"
#include "constants.hpp"
#include "ssl.hpp"

#define NUM_NODES 3u

using namespace datastax::internal::core;

class PoolUnitTest : public LoopTest {
public:
  template <class State>
  class Status {
  public:
    size_t count(State state) {
      size_t count = 0;
      for (typename Vector<State>::const_iterator it = results_.begin(), end = results_.end();
           it != end; ++it) {
        if (*it == state) count++;
      }
      return count;
    }

    const Vector<State>& results() { return results_; }

  protected:
    void set(State state) { results_.push_back(state); }

  private:
    Vector<State> results_;
  };

  struct RequestState {
    enum Enum { SUCCESS, ERROR_NO_CONNECTION, ERROR_FAILED_WRITE, ERROR, ERROR_RESPONSE, TIMEOUT };

    static const char* to_string(Enum state) {
      switch (state) {
        case SUCCESS:
          return "SUCCESS";
        case ERROR_NO_CONNECTION:
          return "ERROR_NO_CONNECTION";
        case ERROR_FAILED_WRITE:
          return "ERROR_FAILED_WRITE";
        case ERROR:
          return "ERROR";
        case ERROR_RESPONSE:
          return "ERROR_RESPONSE";
        case TIMEOUT:
          return "TIMEOUT";
      }
      return "";
    }
  };

  class RequestStatus
      : public RequestState
      , public Status<RequestState::Enum> {
  public:
    RequestStatus(uv_loop_t* loop, int num_requests = NUM_NODES)
        : loop_(loop)
        , remaining_(num_requests) {}

    virtual ~RequestStatus() {}

    virtual void set(RequestState::Enum state) {
      Status<RequestStatus::Enum>::set(state);
      if (--remaining_ == 0) uv_stop(loop_);
    }

    void success() { set(SUCCESS); }
    void error_failed_write() { set(ERROR_FAILED_WRITE); }
    void error_no_connection() { set(ERROR_NO_CONNECTION); }
    void error() { set(ERROR); }
    void error_response() { set(ERROR_RESPONSE); }
    void timeout() { set(TIMEOUT); }

  protected:
    uv_loop_t* loop_;
    int remaining_;
  };

  class RequestStatusWithManager : public RequestStatus {
  public:
    RequestStatusWithManager(uv_loop_t* loop, int num_requests = NUM_NODES)
        : RequestStatus(loop, num_requests) {}

    ~RequestStatusWithManager() {
      ConnectionPoolManager::Ptr temp(manager());
      if (temp) temp->close();
      uv_run(loop_, UV_RUN_DEFAULT); // Allow the loop to cleanup
    }

    void set_manager(const ConnectionPoolManager::Ptr& manager) { manager_ = manager; }

    ConnectionPoolManager::Ptr manager() { return manager_; }

    virtual void set(RequestState::Enum state) { RequestStatus::set(state); }

  private:
    ConnectionPoolManager::Ptr manager_;
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

    static const char* to_string(Enum state) {
      switch (state) {
        case UP:
          return "UP";
        case DOWN:
          return "DOWN";
        case CRITICAL_ERROR:
          return "CRITICAL_ERROR";
        case CRITICAL_ERROR_INVALID_PROTOCOL:
          return "CRITICAL_ERROR_INVALID_PROTOCOL";
        case CRITICAL_ERROR_KEYSPACE:
          return "CRITICAL_ERROR_KEYSPACE";
        case CRITICAL_ERROR_AUTH:
          return "CRITICAL_ERROR_AUTH";
        case CRITICAL_ERROR_SSL_HANDSHAKE:
          return "CRITICAL_ERROR_SSL_HANDSHAKE";
        case CRITICAL_ERROR_SSL_VERIFY:
          return "CRITICAL_ERROR_SSL_VERIFY";
      }
      return "";
    }
  };

  class ListenerStatus
      : public ListenerState
      , public Status<ListenerState::Enum> {
  public:
    ListenerStatus(uv_loop_t* loop, int num_nodes = NUM_NODES)
        : loop_(loop)
        , count_(num_nodes)
        , remaining_(num_nodes) {}

    ~ListenerStatus() {}

    void reset() { remaining_ = count_; }

    virtual void up() { set(UP); }
    virtual void down() { set(DOWN); }
    virtual void critical_error() { set(CRITICAL_ERROR); }
    virtual void critical_error_invalid_protocol() { set(CRITICAL_ERROR_INVALID_PROTOCOL); }
    virtual void critical_error_keyspace() { set(CRITICAL_ERROR_KEYSPACE); }
    virtual void critical_error_auth() { set(CRITICAL_ERROR_AUTH); }
    virtual void critical_error_ssl_handshake() { set(CRITICAL_ERROR_SSL_HANDSHAKE); }
    virtual void critical_error_ssl_verify() { set(CRITICAL_ERROR_SSL_VERIFY); }

  private:
    virtual void set(ListenerState::Enum state) {
      Status<ListenerState::Enum>::set(state);
      if (--remaining_ == 0) uv_stop(loop_);
    }

  private:
    uv_loop_t* loop_;
    size_t count_;
    size_t remaining_;
  };

  class ListenerUpStatus : public ListenerStatus {
  public:
    ListenerUpStatus(uv_loop_t* loop, int num_nodes = NUM_NODES)
        : ListenerStatus(loop, num_nodes) {}

    void down() {}
    void critical_error() {}
    void critical_error_invalid_protocol() {}
    void critical_error_keyspace() {}
    void critical_error_auth() {}
    void critical_error_ssl_handshake() {}
    void critical_error_ssl_verify() {}
  };

  class Listener : public ConnectionPoolManagerListener {
  public:
    Listener(ListenerStatus* status)
        : status_(status) {}

    void reset(ListenerStatus* status) { status_ = status; }

    ListenerStatus* status() const { return status_; }

    virtual void on_pool_up(const Address& address) { status_->up(); }

    virtual void on_pool_down(const Address& address) { status_->down(); }

    virtual void on_pool_critical_error(const Address& address, Connector::ConnectionError code,
                                        const String& message) {
      switch (code) {
        case Connector::CONNECTION_ERROR_INVALID_PROTOCOL:
          status_->critical_error_invalid_protocol();
          break;
        case Connector::CONNECTION_ERROR_KEYSPACE:
          status_->critical_error_keyspace();
          break;
        case Connector::CONNECTION_ERROR_AUTH:
          status_->critical_error_auth();
          break;
        case Connector::CONNECTION_ERROR_SSL_HANDSHAKE:
          status_->critical_error_ssl_handshake();
          break;
        case Connector::CONNECTION_ERROR_SSL_VERIFY:
          status_->critical_error_ssl_verify();
          break;
        default:
          status_->critical_error();
          break;
      }
    }

    virtual void on_close(ConnectionPoolManager* manager) {}

  private:
    ListenerStatus* status_;
  };

  class RequestCallback : public SimpleRequestCallback {
  public:
    RequestCallback(RequestStatus* status)
        : SimpleRequestCallback("SELECT * FROM blah")
        , status_(status) {}

    virtual void on_internal_set(ResponseMessage* response) {
      if (response->response_body()->opcode() == CQL_OPCODE_RESULT) {
        status_->success();
      } else {
        status_->error_response();
      }
    }

    virtual void on_internal_error(CassError code, const String& message) { status_->error(); }

    virtual void on_internal_timeout() { status_->timeout(); }

  private:
    RequestStatus* status_;
  };

  class PoolUnitTestReconnectionPolicy : public ReconnectionPolicy {
  public:
    typedef SharedRefPtr<PoolUnitTestReconnectionPolicy> Ptr;

    PoolUnitTestReconnectionPolicy()
        : ReconnectionPolicy(ReconnectionPolicy::CONSTANT)
        , reconnection_schedule_count_(0)
        , destroyed_reconnection_schedule_count_(0)
        , scheduled_delay_count_(0) {}

    virtual const char* name() const { return "blah"; }
    virtual ReconnectionSchedule* new_reconnection_schedule() {
      ++reconnection_schedule_count_;
      return new ClusterUnitTestReconnectionSchedule(&scheduled_delay_count_,
                                                     &destroyed_reconnection_schedule_count_);
    }

    unsigned reconnection_schedule_count() const { return reconnection_schedule_count_; }
    unsigned destroyed_reconnection_schedule_count() const {
      return destroyed_reconnection_schedule_count_;
    }
    unsigned scheduled_delay_count() const { return scheduled_delay_count_; }

  private:
    unsigned reconnection_schedule_count_;
    unsigned destroyed_reconnection_schedule_count_;
    unsigned scheduled_delay_count_;

    class ClusterUnitTestReconnectionSchedule : public ReconnectionSchedule {
    public:
      ClusterUnitTestReconnectionSchedule(unsigned* delay_count, unsigned* destroyed_count)
          : delay_count_(delay_count)
          , destroyed_count_(destroyed_count) {}

      ~ClusterUnitTestReconnectionSchedule() { ++*destroyed_count_; }

      virtual uint64_t next_delay_ms() {
        ++*delay_count_;
        return 1;
      }

    private:
      unsigned* delay_count_;
      unsigned* destroyed_count_;
    };
  };

  PoolUnitTest() { loop()->data = NULL; }

  HostMap hosts(size_t num_nodes = NUM_NODES) const {
    mockssandra::Ipv4AddressGenerator generator;
    HostMap hosts;
    for (size_t i = 0; i < num_nodes; ++i) {
      Host::Ptr host(new Host(generator.next()));
      hosts[host->address()] = host;
    }
    return hosts;
  }

  void run_request(const ConnectionPoolManager::Ptr& manager, const Address& address) {
    PooledConnection::Ptr connection = manager->find_least_busy(address);
    if (connection) {
      RequestStatus status(manager->loop(), 1);
      RequestCallback::Ptr callback(new RequestCallback(&status));
      EXPECT_TRUE(connection->write(callback.get()) > 0)
          << "Unable to write request to connection " << address.to_string();
      connection->flush(); // Flush requests to avoid unnecessary timeouts
      uv_run(loop(), UV_RUN_DEFAULT);
      EXPECT_EQ(status.count(RequestState::SUCCESS), 1u) << status.results();
    } else {
      EXPECT_TRUE(false) << "No connection available for " << address.to_string();
    }
  }

  static void on_pool_connected(ConnectionPoolManagerInitializer* initializer,
                                RequestStatusWithManager* status) {
    mockssandra::Ipv4AddressGenerator generator;
    ConnectionPoolManager::Ptr manager = initializer->release_manager();
    status->set_manager(manager);

    for (size_t i = 0; i < NUM_NODES; ++i) {
      PooledConnection::Ptr connection = manager->find_least_busy(generator.next());
      if (connection) {
        RequestCallback::Ptr callback(new RequestCallback(status));
        if (connection->write(callback.get()) < 0) {
          status->error_failed_write();
        }
      } else {
        status->error_no_connection();
      }
      manager->flush(); // Flush requests to avoid unnecessary timeouts
    }
  }

  static void on_pool_connected_exhaust_streams(ConnectionPoolManagerInitializer* initializer,
                                                RequestStatusWithManager* status) {
    const Address address("127.0.0.1", 9042);
    ConnectionPoolManager::Ptr manager = initializer->release_manager();
    status->set_manager(manager);

    for (size_t i = 0; i < CASS_MAX_STREAMS; ++i) {
      PooledConnection::Ptr connection = manager->find_least_busy(address);

      if (connection) {
        RequestCallback::Ptr callback(new RequestCallback(status));
        if (connection->write(callback.get()) < 0) {
          status->error_failed_write();
        }
      } else {
        status->error_no_connection();
      }
    }

    PooledConnection::Ptr connection = manager->find_least_busy(address);
    ASSERT_TRUE(connection);
    RequestCallback::Ptr callback(new RequestCallback(status));
    EXPECT_EQ(connection->write(callback.get()), Request::REQUEST_ERROR_NO_AVAILABLE_STREAM_IDS);

    manager->flush();
  }

  static void on_pool_nop(ConnectionPoolManagerInitializer* initializer,
                          RequestStatusWithManager* status) {
    ConnectionPoolManager::Ptr manager = initializer->release_manager();
    status->set_manager(manager);
  }
};

std::ostream& operator<<(std::ostream& os, const Vector<PoolUnitTest::RequestState::Enum>& states) {
  os << "[";
  bool first = true;
  ;
  for (Vector<PoolUnitTest::RequestState::Enum>::const_iterator it = states.begin(),
                                                                end = states.end();
       it != end; ++it) {
    if (!first) {
      os << ", ";
    } else {
      first = false;
    }
    os << PoolUnitTest::RequestState::to_string(*it);
  }
  os << "]";
  return os;
}

std::ostream& operator<<(std::ostream& os,
                         const Vector<PoolUnitTest::ListenerState::Enum>& states) {
  os << "[";
  bool first = true;
  ;
  for (Vector<PoolUnitTest::ListenerState::Enum>::const_iterator it = states.begin(),
                                                                 end = states.end();
       it != end; ++it) {
    if (!first) {
      os << ", ";
    } else {
      first = false;
    }
    os << PoolUnitTest::ListenerState::to_string(*it);
  }
  os << "]";
  return os;
}

TEST_F(PoolUnitTest, Simple) {
  mockssandra::SimpleCluster cluster(simple(), NUM_NODES);
  ASSERT_EQ(cluster.start_all(), 0);

  RequestStatusWithManager status(loop());

  ConnectionPoolManagerInitializer::Ptr initializer(new ConnectionPoolManagerInitializer(
      PROTOCOL_VERSION, bind_callback(on_pool_connected, &status)));

  initializer->initialize(loop(), hosts());
  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(status.count(RequestStatus::SUCCESS), NUM_NODES) << status.results();
}

TEST_F(PoolUnitTest, Keyspace) {
  mockssandra::SimpleRequestHandlerBuilder builder;

  builder.on(mockssandra::OPCODE_QUERY).use_keyspace("foo").validate_query().void_result();
  mockssandra::SimpleCluster cluster(builder.build(), NUM_NODES);
  ASSERT_EQ(cluster.start_all(), 0);

  RequestStatusWithManager status(loop());

  ConnectionPoolManagerInitializer::Ptr initializer(new ConnectionPoolManagerInitializer(
      PROTOCOL_VERSION, bind_callback(on_pool_connected, &status)));

  HostMap hosts = this->hosts();
  ASSERT_EQ(hosts.size(), NUM_NODES);

  initializer->with_keyspace("foo")->initialize(loop(), hosts);
  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(status.count(RequestStatus::SUCCESS), NUM_NODES) << status.results();

  ConnectionPoolManager::Ptr manager = status.manager();
  ASSERT_TRUE(manager);

  for (HostMap::const_iterator it = hosts.begin(), end = hosts.end(); it != end; ++it) {
    const Address& address = it->first;
    PooledConnection::Ptr connection = manager->find_least_busy(address);
    if (connection) {
      EXPECT_EQ(connection->keyspace(), "foo");
    } else {
      EXPECT_TRUE(false) << "Unable to get connection for " << address.to_string();
    }
  }
}

TEST_F(PoolUnitTest, Auth) {
  mockssandra::SimpleCluster cluster(auth(), NUM_NODES);
  ASSERT_EQ(cluster.start_all(), 0);

  RequestStatusWithManager status(loop());

  ConnectionPoolManagerInitializer::Ptr initializer(new ConnectionPoolManagerInitializer(
      PROTOCOL_VERSION, bind_callback(on_pool_connected, &status)));

  ConnectionPoolSettings settings;
  settings.connection_settings.auth_provider.reset(
      new PlainTextAuthProvider("cassandra", "cassandra"));

  initializer->with_settings(settings)->initialize(loop(), hosts());
  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(status.count(RequestStatus::SUCCESS), NUM_NODES) << status.results();
}

TEST_F(PoolUnitTest, Ssl) {
  mockssandra::SimpleCluster cluster(simple(), NUM_NODES);
  ConnectionPoolSettings settings;
  settings.connection_settings = use_ssl(&cluster);
  ASSERT_EQ(cluster.start_all(), 0);

  RequestStatusWithManager status(loop());

  ConnectionPoolManagerInitializer::Ptr initializer(new ConnectionPoolManagerInitializer(
      PROTOCOL_VERSION, bind_callback(on_pool_connected, &status)));

  initializer->with_settings(settings)->initialize(loop(), hosts());
  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(status.count(RequestStatus::SUCCESS), NUM_NODES) << status.results();
}

TEST_F(PoolUnitTest, Listener) {
  mockssandra::SimpleCluster cluster(simple(), NUM_NODES);
  ASSERT_EQ(cluster.start_all(), 0);

  ListenerStatus listener_status(loop());
  ScopedPtr<Listener> listener(new Listener(&listener_status));
  RequestStatusWithManager request_status(loop(), 0);

  ConnectionPoolManagerInitializer::Ptr initializer(new ConnectionPoolManagerInitializer(
      PROTOCOL_VERSION, bind_callback(on_pool_nop, &request_status)));

  initializer->with_listener(listener.get())->initialize(loop(), hosts());
  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(listener_status.count(ListenerStatus::UP), NUM_NODES) << listener_status.results();
  EXPECT_EQ(initializer->failures().size(), 0u);
}

TEST_F(PoolUnitTest, ListenerDown) {
  mockssandra::SimpleCluster cluster(simple(), NUM_NODES);
  ASSERT_EQ(cluster.start(1), 0);

  ListenerStatus listener_status(loop());
  ScopedPtr<Listener> listener(new Listener(&listener_status));
  RequestStatusWithManager request_status(loop(), 0);

  ConnectionPoolManagerInitializer::Ptr initializer(new ConnectionPoolManagerInitializer(
      PROTOCOL_VERSION, bind_callback(on_pool_nop, &request_status)));

  initializer->with_listener(listener.get())->initialize(loop(), hosts());
  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(listener_status.count(ListenerStatus::UP), 1u) << listener_status.results();
  EXPECT_EQ(listener_status.count(ListenerStatus::DOWN), NUM_NODES - 1)
      << listener_status.results();
  EXPECT_EQ(initializer->failures().size(), 0u);
}

TEST_F(PoolUnitTest, AddRemove) {
  mockssandra::SimpleCluster cluster(simple(), NUM_NODES);
  ASSERT_EQ(cluster.start_all(), 0);

  ListenerStatus listener_status(loop());
  ListenerStatus add_remove_listener_status(loop(), 1);
  ScopedPtr<Listener> listener(new Listener(&listener_status));

  RequestStatusWithManager request_status(loop(), 0);

  ConnectionPoolManagerInitializer::Ptr initializer(new ConnectionPoolManagerInitializer(
      PROTOCOL_VERSION, bind_callback(on_pool_nop, &request_status)));

  HostMap hosts = this->hosts();
  ASSERT_EQ(hosts.size(), NUM_NODES);

  initializer->with_listener(listener.get())->initialize(loop(), hosts);
  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(listener_status.count(ListenerStatus::UP), NUM_NODES) << listener_status.results();

  ConnectionPoolManager::Ptr manager = request_status.manager();
  ASSERT_TRUE(manager);

  listener->reset(&add_remove_listener_status);
  for (HostMap::const_iterator it = hosts.begin(), end = hosts.end(); it != end; ++it) {
    const Address& address = it->first;
    const Host::Ptr& host = it->second;
    add_remove_listener_status.reset();
    manager->remove(address); // Remove node
    uv_run(loop(), UV_RUN_DEFAULT);
    EXPECT_FALSE(manager->find_least_busy(address));

    add_remove_listener_status.reset();
    manager->add(host); // Add node
    uv_run(loop(), UV_RUN_DEFAULT);
    run_request(manager, address);
  }

  EXPECT_EQ(add_remove_listener_status.count(ListenerStatus::DOWN), 3u)
      << add_remove_listener_status.results();
  EXPECT_EQ(add_remove_listener_status.count(ListenerStatus::UP), 3u)
      << add_remove_listener_status.results();
}

TEST_F(PoolUnitTest, Reconnect) {
  mockssandra::SimpleCluster cluster(simple(), NUM_NODES);
  ASSERT_EQ(cluster.start_all(), 0);

  ListenerStatus listener_status(loop());
  ListenerStatus reconnect_listener_status(loop(), 1);
  ScopedPtr<Listener> listener(new Listener(&listener_status));
  RequestStatusWithManager request_status(loop(), 0);

  ConnectionPoolManagerInitializer::Ptr initializer(new ConnectionPoolManagerInitializer(
      PROTOCOL_VERSION, bind_callback(on_pool_nop, &request_status)));

  HostMap hosts = this->hosts();
  ASSERT_EQ(hosts.size(), NUM_NODES);

  ConnectionPoolSettings settings;
  settings.reconnection_policy.reset(new ConstantReconnectionPolicy(0)); // Reconnect immediately

  initializer->with_settings(settings)->with_listener(listener.get())->initialize(loop(), hosts);
  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(listener_status.count(ListenerStatus::UP), NUM_NODES) << listener_status.results();

  ConnectionPoolManager::Ptr manager = request_status.manager();
  ASSERT_TRUE(manager);

  listener->reset(&reconnect_listener_status);
  size_t node = 1;
  for (HostMap::const_iterator it = hosts.begin(), end = hosts.end(); it != end; ++it) {
    const Address& address = it->first;
    reconnect_listener_status.reset();

    cluster.stop(node); // Stop node
    uv_run(loop(), UV_RUN_DEFAULT);
    EXPECT_FALSE(manager->find_least_busy(address));

    reconnect_listener_status.reset();

    ASSERT_EQ(cluster.start(node), 0); // Start node
    uv_run(loop(), UV_RUN_DEFAULT);
    run_request(manager, address);
    ++node;
  }

  EXPECT_EQ(reconnect_listener_status.count(ListenerStatus::DOWN), 3u)
      << reconnect_listener_status.results();
  EXPECT_EQ(reconnect_listener_status.count(ListenerStatus::UP), 3u)
      << reconnect_listener_status.results();
}

TEST_F(PoolUnitTest, Timeout) {
  mockssandra::RequestHandler::Builder builder;
  builder.on(mockssandra::OPCODE_STARTUP).no_result(); // Don't return a response
  mockssandra::SimpleCluster cluster(builder.build(), NUM_NODES);
  ASSERT_EQ(cluster.start_all(), 0);

  ListenerStatus listener_status(loop());
  ScopedPtr<Listener> listener(new Listener(&listener_status));
  RequestStatusWithManager request_status(loop(), 0);

  ConnectionPoolManagerInitializer::Ptr initializer(new ConnectionPoolManagerInitializer(
      PROTOCOL_VERSION, bind_callback(on_pool_nop, &request_status)));

  ConnectionPoolSettings settings;
  settings.connection_settings.connect_timeout_ms = 200;

  initializer->with_settings(settings)->with_listener(listener.get())->initialize(loop(), hosts());
  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(listener_status.count(ListenerStatus::DOWN), NUM_NODES) << listener_status.results();
}

TEST_F(PoolUnitTest, InvalidProtocol) {
  mockssandra::SimpleCluster cluster(simple(), NUM_NODES);
  ASSERT_EQ(cluster.start_all(), 0);

  ListenerStatus listener_status(loop());
  ScopedPtr<Listener> listener(new Listener(&listener_status));
  RequestStatusWithManager request_status(loop(), 0);

  ConnectionPoolManagerInitializer::Ptr initializer(
      new ConnectionPoolManagerInitializer(0x7F, // Invalid protocol version
                                           bind_callback(on_pool_nop, &request_status)));

  initializer->with_listener(listener.get())->initialize(loop(), hosts());
  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_GT(listener_status.count(ListenerStatus::CRITICAL_ERROR_INVALID_PROTOCOL), 0u)
      << listener_status.results();

  ConnectionPoolConnector::Vec failures = initializer->failures();
  EXPECT_EQ(failures.size(), NUM_NODES);

  for (ConnectionPoolConnector::Vec::const_iterator it = failures.begin(), end = failures.end();
       it != end; ++it) {
    EXPECT_EQ((*it)->error_code(), Connector::CONNECTION_ERROR_INVALID_PROTOCOL);
  }
}

TEST_F(PoolUnitTest, InvalidKeyspace) {
  mockssandra::SimpleRequestHandlerBuilder builder;
  builder.on(mockssandra::OPCODE_QUERY).use_keyspace("foo").validate_query().void_result();
  mockssandra::SimpleCluster cluster(builder.build(), NUM_NODES);
  ASSERT_EQ(cluster.start_all(), 0);

  ListenerStatus listener_status(loop());
  ScopedPtr<Listener> listener(new Listener(&listener_status));
  RequestStatusWithManager request_status(loop(), 0);

  ConnectionPoolManagerInitializer::Ptr initializer(new ConnectionPoolManagerInitializer(
      PROTOCOL_VERSION, bind_callback(on_pool_nop, &request_status)));

  initializer->with_keyspace("invalid")->with_listener(listener.get())->initialize(loop(), hosts());
  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(listener_status.count(ListenerStatus::CRITICAL_ERROR_KEYSPACE), NUM_NODES)
      << listener_status.results();
}

TEST_F(PoolUnitTest, InvalidAuth) {
  mockssandra::SimpleCluster cluster(auth(), NUM_NODES);
  ASSERT_EQ(cluster.start_all(), 0);

  ListenerStatus listener_status(loop());
  ScopedPtr<Listener> listener(new Listener(&listener_status));
  RequestStatusWithManager request_status(loop(), 0);

  ConnectionPoolManagerInitializer::Ptr initializer(new ConnectionPoolManagerInitializer(
      PROTOCOL_VERSION, bind_callback(on_pool_nop, &request_status)));

  ConnectionPoolSettings settings;
  settings.connection_settings.auth_provider.reset(new PlainTextAuthProvider("invalid", "invalid"));

  initializer->with_settings(settings)->with_listener(listener.get())->initialize(loop(), hosts());
  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_GT(listener_status.count(ListenerStatus::CRITICAL_ERROR_AUTH), 0u)
      << listener_status.results();
}

TEST_F(PoolUnitTest, InvalidNoSsl) {
  mockssandra::SimpleCluster cluster(simple(), NUM_NODES);
  ASSERT_EQ(cluster.start_all(), 0); // Start without ssl

  ListenerStatus listener_status(loop());
  ScopedPtr<Listener> listener(new Listener(&listener_status));
  RequestStatusWithManager request_status(loop(), 0);

  ConnectionPoolManagerInitializer::Ptr initializer(new ConnectionPoolManagerInitializer(
      PROTOCOL_VERSION, bind_callback(on_pool_nop, &request_status)));

  SslContext::Ptr ssl_context(SslContextFactory::create());

  ConnectionPoolSettings settings;
  settings.connection_settings.socket_settings.ssl_context = ssl_context;
  settings.connection_settings.socket_settings.hostname_resolution_enabled = true;

  initializer->with_settings(settings)->with_listener(listener.get())->initialize(loop(), hosts());
  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_GT(listener_status.count(ListenerStatus::CRITICAL_ERROR_SSL_HANDSHAKE), 0u)
      << listener_status.results();
}

TEST_F(PoolUnitTest, InvalidSsl) {
  mockssandra::SimpleCluster cluster(simple(), NUM_NODES);
  use_ssl(&cluster);
  ASSERT_EQ(cluster.start_all(), 0);

  ListenerStatus listener_status(loop());
  ScopedPtr<Listener> listener(new Listener(&listener_status));
  RequestStatusWithManager request_status(loop(), 0);

  ConnectionPoolManagerInitializer::Ptr initializer(new ConnectionPoolManagerInitializer(
      PROTOCOL_VERSION, bind_callback(on_pool_nop, &request_status)));

  SslContext::Ptr ssl_context(SslContextFactory::create()); // No trusted cert

  ConnectionPoolSettings settings;
  settings.connection_settings.socket_settings.ssl_context = ssl_context;
  settings.connection_settings.socket_settings.hostname_resolution_enabled = true;

  initializer->with_settings(settings)->with_listener(listener.get())->initialize(loop(), hosts());
  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_GT(listener_status.count(ListenerStatus::CRITICAL_ERROR_SSL_VERIFY), 0u)
      << listener_status.results();
}

TEST_F(PoolUnitTest, ReconnectionPolicy) {
  mockssandra::SimpleCluster cluster(simple(), 2);
  ASSERT_EQ(cluster.start_all(), 0);

  ListenerStatus listener_status(loop(), 2);
  ListenerStatus reconnect_listener_status(loop(), 1);
  ScopedPtr<Listener> listener(new Listener(&listener_status));
  RequestStatusWithManager request_status(loop(), 0);

  ConnectionPoolManagerInitializer::Ptr initializer(new ConnectionPoolManagerInitializer(
      PROTOCOL_VERSION, bind_callback(on_pool_nop, &request_status)));

  HostMap hosts = this->hosts(2);
  ConnectionPoolSettings settings;
  settings.reconnection_policy.reset(new PoolUnitTestReconnectionPolicy());
  initializer->with_settings(settings)->with_listener(listener.get())->initialize(loop(), hosts);
  uv_run(loop(), UV_RUN_DEFAULT);
  EXPECT_EQ(listener_status.count(ListenerStatus::UP), 2u) << listener_status.results();

  // Stop and start node 1 twice engaging the reconnection policy
  listener->reset(&reconnect_listener_status);
  for (int i = 0; i < 2; ++i) {
    reconnect_listener_status.reset();
    cluster.stop(1);
    uv_run(loop(), UV_RUN_DEFAULT);
    reconnect_listener_status.reset();
    ASSERT_EQ(cluster.start(1), 0);
    uv_run(loop(), UV_RUN_DEFAULT);
  }

  PoolUnitTestReconnectionPolicy::Ptr policy(
      static_cast<PoolUnitTestReconnectionPolicy::Ptr>(settings.reconnection_policy));
  EXPECT_EQ(2u, policy->reconnection_schedule_count());
  EXPECT_EQ(2u, policy->destroyed_reconnection_schedule_count());
  EXPECT_EQ(2u, policy->scheduled_delay_count());
  EXPECT_EQ(3u, cluster.connection_attempts(1)); // Includes initial connection attempt
  EXPECT_EQ(1u, cluster.connection_attempts(2));
}

TEST_F(PoolUnitTest, PartialReconnect) {
  // TODO:
}

TEST_F(PoolUnitTest, NoAvailableStreams) {
  mockssandra::SimpleCluster cluster(simple(), 1);
  ASSERT_EQ(cluster.start_all(), 0);

  RequestStatusWithManager status(loop(), CASS_MAX_STREAMS);

  ConnectionPoolManagerInitializer::Ptr initializer(new ConnectionPoolManagerInitializer(
      PROTOCOL_VERSION, bind_callback(on_pool_connected_exhaust_streams, &status)));

  initializer->initialize(loop(), hosts());
  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(status.count(RequestStatus::SUCCESS), static_cast<size_t>(CASS_MAX_STREAMS))
      << status.results();
}

/**
 * Verify that connections start up correctly with a case-sensitive keyspace.
 */
TEST_F(PoolUnitTest, CaseSenstiveKeyspace) {
  mockssandra::SimpleRequestHandlerBuilder builder;

  builder.on(mockssandra::OPCODE_QUERY)
      .use_keyspace("CaseSensitive") // Not quoted
      .validate_query()
      .void_result();

  mockssandra::SimpleCluster cluster(builder.build(), NUM_NODES);
  ASSERT_EQ(cluster.start_all(), 0);

  RequestStatusWithManager status(loop());

  ConnectionPoolManagerInitializer::Ptr initializer(new ConnectionPoolManagerInitializer(
      PROTOCOL_VERSION, bind_callback(on_pool_connected, &status)));

  HostMap hosts(this->hosts());
  Address address(hosts.begin()->first);

  ConnectionPoolSettings settings;

  settings.reconnection_policy =
      ReconnectionPolicy::Ptr(new ConstantReconnectionPolicy(10)); // Reconnect quickly

  initializer->with_keyspace("\"CaseSensitive\"")
      ->with_settings(settings)
      ->initialize(loop(), hosts);
  uv_run(loop(), UV_RUN_DEFAULT);

  ASSERT_EQ(status.count(RequestStatus::SUCCESS), NUM_NODES) << status.results();

  ConnectionPoolManager::Ptr manager(status.manager());
  EXPECT_EQ(manager->find_least_busy(address)->keyspace(),
            "\"CaseSensitive\""); // Verify new keyspace was set properly by running a request
}

/**
 * Verify that connections properly switch to a case-sensitive keyspace when triggered by a request.
 */
TEST_F(PoolUnitTest, ChangeToCaseSensitiveKeyspaceWithRequest) {
  mockssandra::SimpleRequestHandlerBuilder builder;

  Vector<String> keyspaces;
  keyspaces.push_back("case_insensitive");
  keyspaces.push_back("CaseSensitive"); // Not quoted
  builder.on(mockssandra::OPCODE_QUERY).use_keyspace(keyspaces).validate_query().void_result();

  mockssandra::SimpleCluster cluster(builder.build(), NUM_NODES);
  ASSERT_EQ(cluster.start_all(), 0);

  RequestStatusWithManager status(loop());

  ConnectionPoolManagerInitializer::Ptr initializer(new ConnectionPoolManagerInitializer(
      PROTOCOL_VERSION, bind_callback(on_pool_connected, &status)));

  HostMap hosts(this->hosts());
  Address address(hosts.begin()->first);

  ConnectionPoolSettings settings;

  settings.reconnection_policy =
      ReconnectionPolicy::Ptr(new ConstantReconnectionPolicy(10)); // Reconnect quickly

  initializer->with_keyspace("case_insensitive")
      ->with_settings(settings)
      ->initialize(loop(), hosts);
  uv_run(loop(), UV_RUN_DEFAULT);

  ASSERT_EQ(status.count(RequestStatus::SUCCESS), NUM_NODES) << status.results();

  ConnectionPoolManager::Ptr manager(status.manager());

  manager->set_keyspace("\"CaseSensitive\"");
  EXPECT_EQ(manager->find_least_busy(address)->keyspace(),
            "case_insensitive"); // Verify existing keyspace is the one set during initialization

  run_request(manager, address);
  EXPECT_EQ(manager->find_least_busy(address)->keyspace(),
            "\"CaseSensitive\""); // Verify new keyspace was set properly by running a request
}
