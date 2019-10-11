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

#include "auth.hpp"
#include "connector.hpp"
#include "constants.hpp"
#include "delayed_connector.hpp"
#include "request_callback.hpp"
#include "ssl.hpp"

#ifdef WIN32
#undef STATUS_TIMEOUT
#endif

using namespace datastax::internal;
using namespace datastax::internal::core;

class ConnectionUnitTest : public LoopTest {
public:
  enum Status {
    STATUS_NEW,
    STATUS_CONNECTED,
    STATUS_ERROR,
    STATUS_ERROR_RESPONSE,
    STATUS_TIMEOUT,
    STATUS_SUCCESS
  };

  struct State {
    State()
        : status(STATUS_NEW) {}

    Connection::Ptr connection;
    Status status;
  };

  class RequestCallback : public SimpleRequestCallback {
  public:
    RequestCallback(Connection* connection, ConnectionUnitTest::State* state)
        : SimpleRequestCallback("SELECT * FROM blah")
        , state_(state)
        , connection_(connection) {}

    virtual void on_internal_set(ResponseMessage* response) {
      connection_->close();
      if (response->response_body()->opcode() == CQL_OPCODE_RESULT) {
        state_->status = ConnectionUnitTest::STATUS_SUCCESS;
      } else {
        state_->status = ConnectionUnitTest::STATUS_ERROR_RESPONSE;
      }
    }

    virtual void on_internal_error(CassError code, const String& message) {
      connection_->close();
      state_->status = ConnectionUnitTest::STATUS_ERROR;
    }

    virtual void on_internal_timeout() {
      state_->status = ConnectionUnitTest::STATUS_TIMEOUT;
      connection_->close();
    }

  private:
    ConnectionUnitTest::State* state_;
    Connection* connection_;
  };

  static void on_connection_connected(Connector* connector, State* state) {
    ASSERT_TRUE(connector->is_ok());
    state->status = STATUS_CONNECTED;
    state->connection = connector->release_connection();
    state->connection->start_heartbeats();
    state->connection->write_and_flush(
        RequestCallback::Ptr(new RequestCallback(state->connection.get(), state)));
  }

  static void on_connection_error_code(Connector* connector,
                                       Connector::ConnectionError* error_code) {
    if (!connector->is_ok()) {
      *error_code = connector->error_code();
    }
  }

  static void on_connection_close(Connector* connector, bool* is_closed) {
    if (connector->error_code() == Connector::CONNECTION_ERROR_CLOSE) {
      *is_closed = true;
    }
  }

  static void on_delayed_connected(DelayedConnector* connector, State* state) {
    ASSERT_TRUE(connector->is_ok());
    state->status = STATUS_CONNECTED;
    state->connection = connector->release_connection();
    state->connection->start_heartbeats();
    state->connection->write_and_flush(
        RequestCallback::Ptr(new RequestCallback(state->connection.get(), state)));
  }

  static void on_delayed_error_code(DelayedConnector* connector,
                                    Connector::ConnectionError* error_code) {
    if (!connector->is_ok()) {
      *error_code = connector->error_code();
    }
  }
};

TEST_F(ConnectionUnitTest, Simple) {
  mockssandra::SimpleCluster cluster(simple());
  ASSERT_EQ(cluster.start_all(), 0);

  State state;
  Connector::Ptr connector(new Connector(Host::Ptr(new Host(Address("127.0.0.1", PORT))),
                                         PROTOCOL_VERSION,
                                         bind_callback(on_connection_connected, &state)));

  connector->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(state.status, STATUS_SUCCESS);
}

TEST_F(ConnectionUnitTest, Keyspace) {
  mockssandra::SimpleRequestHandlerBuilder builder;
  builder.on(mockssandra::OPCODE_QUERY).use_keyspace("foo").validate_query().void_result();
  mockssandra::SimpleCluster cluster(builder.build());
  ASSERT_EQ(cluster.start_all(), 0);

  State state;
  Connector::Ptr connector(new Connector(Host::Ptr(new Host(Address("127.0.0.1", PORT))),
                                         PROTOCOL_VERSION,
                                         bind_callback(on_connection_connected, &state)));

  connector->with_keyspace("foo")->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(state.status, STATUS_SUCCESS);
  ASSERT_TRUE(static_cast<bool>(state.connection));
  EXPECT_EQ(state.connection->keyspace(), "foo");
}

TEST_F(ConnectionUnitTest, Auth) {
  mockssandra::SimpleCluster cluster(auth());
  ASSERT_EQ(cluster.start_all(), 0);

  State state;
  Connector::Ptr connector(new Connector(Host::Ptr(new Host(Address("127.0.0.1", PORT))),
                                         PROTOCOL_VERSION,
                                         bind_callback(on_connection_connected, &state)));

  ConnectionSettings settings;
  settings.auth_provider.reset(new PlainTextAuthProvider("cassandra", "cassandra"));

  connector->with_settings(settings)->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(state.status, STATUS_SUCCESS);
}

TEST_F(ConnectionUnitTest, Ssl) {
  mockssandra::SimpleCluster cluster(simple());
  ConnectionSettings settings(use_ssl(&cluster));
  ASSERT_EQ(cluster.start_all(), 0);

  State state;
  Connector::Ptr connector(new Connector(Host::Ptr(new Host(Address("127.0.0.1", PORT))),
                                         PROTOCOL_VERSION,
                                         bind_callback(on_connection_connected, &state)));
  connector->with_settings(settings)->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(state.status, STATUS_SUCCESS);
}

TEST_F(ConnectionUnitTest, Refused) {
  // Don't start cluster

  Connector::ConnectionError error_code(Connector::CONNECTION_OK);
  Connector::Ptr connector(new Connector(Host::Ptr(new Host(Address("127.0.0.1", PORT))),
                                         PROTOCOL_VERSION,
                                         bind_callback(on_connection_error_code, &error_code)));
  connector->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(Connector::CONNECTION_ERROR_CONNECT, error_code);
}

TEST_F(ConnectionUnitTest, Close) {
  mockssandra::SimpleCluster cluster(simple());
  cluster.use_close_immediately();
  ASSERT_EQ(cluster.start_all(), 0);

  Vector<Connector::Ptr> connectors;

  bool is_closed(false);
  for (size_t i = 0; i < 10; ++i) {
    Connector::Ptr connector(new Connector(Host::Ptr(new Host(Address("127.0.0.1", PORT))),
                                           PROTOCOL_VERSION,
                                           bind_callback(on_connection_close, &is_closed)));
    connector->connect(loop());
    connectors.push_back(connector);
  }

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_TRUE(is_closed);
}

TEST_F(ConnectionUnitTest, SslClose) {
  mockssandra::SimpleCluster cluster(simple());
  ConnectionSettings settings(use_ssl(&cluster));
  cluster.use_close_immediately();
  ASSERT_EQ(cluster.start_all(), 0);

  Vector<Connector::Ptr> connectors;

  bool is_closed(false);
  for (size_t i = 0; i < 10; ++i) {
    Connector::Ptr connector(new Connector(Host::Ptr(new Host(Address("127.0.0.1", PORT))),
                                           PROTOCOL_VERSION,
                                           bind_callback(on_connection_close, &is_closed)));
    connector->with_settings(settings)->connect(loop());
    connectors.push_back(connector);
  }

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_TRUE(is_closed);
}

TEST_F(ConnectionUnitTest, Cancel) {
  mockssandra::SimpleCluster cluster(simple());
  ASSERT_EQ(cluster.start_all(), 0);

  Vector<Connector::Ptr> connectors;

  Connector::ConnectionError error_code(Connector::CONNECTION_OK);
  for (size_t i = 0; i < 10; ++i) {
    Connector::Ptr connector(new Connector(Host::Ptr(new Host(Address("127.0.0.1", PORT))),
                                           PROTOCOL_VERSION,
                                           bind_callback(on_connection_error_code, &error_code)));
    connector->connect(loop());
    connectors.push_back(connector);
  }

  Vector<Connector::Ptr>::iterator it = connectors.begin();
  while (it != connectors.end()) {
    (*it)->cancel();
    uv_run(loop(), UV_RUN_NOWAIT);
    it++;
  }

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(Connector::CONNECTION_CANCELED, error_code);
}

TEST_F(ConnectionUnitTest, SslCancel) {
  mockssandra::SimpleCluster cluster(simple());
  ConnectionSettings settings(use_ssl(&cluster));
  ASSERT_EQ(cluster.start_all(), 0);

  Vector<Connector::Ptr> connectors;

  Connector::ConnectionError error_code(Connector::CONNECTION_OK);
  for (size_t i = 0; i < 10; ++i) {
    Connector::Ptr connector(new Connector(Host::Ptr(new Host(Address("127.0.0.1", PORT))),
                                           PROTOCOL_VERSION,
                                           bind_callback(on_connection_error_code, &error_code)));
    connector->with_settings(settings)->connect(loop());
    connectors.push_back(connector);
  }

  Vector<Connector::Ptr>::iterator it = connectors.begin();
  while (it != connectors.end()) {
    (*it)->cancel();
    uv_run(loop(), UV_RUN_NOWAIT);
    it++;
  }

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(Connector::CONNECTION_CANCELED, error_code);
}

TEST_F(ConnectionUnitTest, Timeout) {
  mockssandra::SimpleRequestHandlerBuilder builder;
  builder.on(mockssandra::OPCODE_STARTUP).no_result(); // Don't return a response
  mockssandra::SimpleCluster cluster(builder.build());
  ASSERT_EQ(cluster.start_all(), 0);

  Connector::ConnectionError error_code(Connector::CONNECTION_OK);
  Connector::Ptr connector(new Connector(Host::Ptr(new Host(Address("127.0.0.1", PORT))),
                                         PROTOCOL_VERSION,
                                         bind_callback(on_connection_error_code, &error_code)));

  ConnectionSettings settings;
  settings.connect_timeout_ms = 200;

  connector->with_settings(settings)->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(Connector::CONNECTION_ERROR_TIMEOUT, error_code);
}

TEST_F(ConnectionUnitTest, InvalidKeyspace) {
  mockssandra::SimpleRequestHandlerBuilder builder;
  builder.on(mockssandra::OPCODE_QUERY).use_keyspace("foo").validate_query().void_result();
  mockssandra::SimpleCluster cluster(builder.build());
  ASSERT_EQ(cluster.start_all(), 0);

  Connector::ConnectionError error_code(Connector::CONNECTION_OK);
  Connector::Ptr connector(new Connector(Host::Ptr(new Host(Address("127.0.0.1", PORT))),
                                         PROTOCOL_VERSION,
                                         bind_callback(on_connection_error_code, &error_code)));
  connector->with_keyspace("invalid")->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(Connector::CONNECTION_ERROR_KEYSPACE, error_code);
}

TEST_F(ConnectionUnitTest, InvalidProtocol) {
  mockssandra::SimpleCluster cluster(simple());
  ASSERT_EQ(cluster.start_all(), 0);

  Connector::ConnectionError error_code(Connector::CONNECTION_OK);
  Connector::Ptr connector(new Connector(Host::Ptr(new Host(Address("127.0.0.1", PORT))),
                                         0x7F, // Invalid protocol version
                                         bind_callback(on_connection_error_code, &error_code)));
  connector->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(Connector::CONNECTION_ERROR_INVALID_PROTOCOL, error_code);
}

TEST_F(ConnectionUnitTest, DeprecatedProtocol) {
  mockssandra::SimpleCluster cluster(
      mockssandra::SimpleRequestHandlerBuilder().with_supported_protocol_versions(1, 2).build());
  cluster.start_all();

  Connector::ConnectionError error_code(Connector::CONNECTION_OK);
  Connector::Ptr connector(new Connector(Host::Ptr(new Host(Address("127.0.0.1", PORT))),
                                         PROTOCOL_VERSION,
                                         bind_callback(on_connection_error_code, &error_code)));
  connector->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(Connector::CONNECTION_ERROR_INVALID_PROTOCOL, error_code);
}

TEST_F(ConnectionUnitTest, InvalidAuth) {
  mockssandra::SimpleCluster cluster(auth());
  ASSERT_EQ(cluster.start_all(), 0);

  Connector::ConnectionError error_code(Connector::CONNECTION_OK);
  Connector::Ptr connector(new Connector(Host::Ptr(new Host(Address("127.0.0.1", PORT))),
                                         PROTOCOL_VERSION,
                                         bind_callback(on_connection_error_code, &error_code)));

  ConnectionSettings settings;
  settings.auth_provider.reset(new PlainTextAuthProvider("invalid", "invalid"));

  connector->with_settings(settings)->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(Connector::CONNECTION_ERROR_AUTH, error_code);
}

TEST_F(ConnectionUnitTest, InvalidNoSsl) {
  mockssandra::SimpleCluster cluster(simple());
  ASSERT_EQ(cluster.start_all(), 0); // Start without ssl

  Connector::ConnectionError error_code(Connector::CONNECTION_OK);
  Connector::Ptr connector(new Connector(Host::Ptr(new Host(Address("127.0.0.1", PORT))),
                                         PROTOCOL_VERSION,
                                         bind_callback(on_connection_error_code, &error_code)));

  SslContext::Ptr ssl_context(SslContextFactory::create());

  ConnectionSettings settings;
  settings.socket_settings.ssl_context = ssl_context;
  settings.socket_settings.hostname_resolution_enabled = true;

  connector->with_settings(settings)->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(Connector::CONNECTION_ERROR_SSL_HANDSHAKE, error_code);
}

TEST_F(ConnectionUnitTest, InvalidSsl) {
  mockssandra::SimpleCluster cluster(simple());
  use_ssl(&cluster);
  ASSERT_EQ(cluster.start_all(), 0);

  Connector::ConnectionError error_code(Connector::CONNECTION_OK);
  Connector::Ptr connector(new Connector(Host::Ptr(new Host(Address("127.0.0.1", PORT))),
                                         PROTOCOL_VERSION,
                                         bind_callback(on_connection_error_code, &error_code)));

  SslContext::Ptr ssl_context(SslContextFactory::create()); // No trusted cert

  ConnectionSettings settings;
  settings.socket_settings.ssl_context = ssl_context;
  settings.socket_settings.hostname_resolution_enabled = true;

  connector->with_settings(settings)->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(Connector::CONNECTION_ERROR_SSL_VERIFY, error_code);
}

TEST_F(ConnectionUnitTest, Delayed) {
  mockssandra::SimpleCluster cluster(simple());
  ASSERT_EQ(cluster.start_all(), 0);

  State state;
  DelayedConnector::Ptr connector(
      new DelayedConnector(Host::Ptr(new Host(Address("127.0.0.1", PORT))), PROTOCOL_VERSION,
                           bind_callback(on_delayed_connected, &state)));

  connector->delayed_connect(loop(), 500);

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(state.status, STATUS_SUCCESS);
}

TEST_F(ConnectionUnitTest, DelayedCancel) {
  mockssandra::SimpleCluster cluster(simple());
  ASSERT_EQ(cluster.start_all(), 0);

  State state;
  DelayedConnector::Ptr connector(
      new DelayedConnector(Host::Ptr(new Host(Address("127.0.0.1", PORT))), PROTOCOL_VERSION,
                           bind_callback(on_delayed_connected, &state)));

  connector->delayed_connect(loop(), 500);

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(state.status, STATUS_SUCCESS);

  ASSERT_EQ(cluster.start_all(), 0);

  Vector<DelayedConnector::Ptr> connectors;

  Connector::ConnectionError error_code(Connector::CONNECTION_OK);
  for (size_t i = 0; i < 10; ++i) {
    DelayedConnector::Ptr connector(
        new DelayedConnector(Host::Ptr(new Host(Address("127.0.0.1", PORT))), PROTOCOL_VERSION,
                             bind_callback(on_delayed_error_code, &error_code)));
    connector->delayed_connect(loop(), 500);
    connectors.push_back(connector);
  }

  Vector<DelayedConnector::Ptr>::iterator it = connectors.begin();
  while (it != connectors.end()) {
    (*it)->cancel();
    uv_run(loop(), UV_RUN_NOWAIT);
    it++;
  }

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(Connector::CONNECTION_CANCELED, error_code);
}
