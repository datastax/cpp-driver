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

#include "auth.hpp"
#include "connector.hpp"
#include "request_callback.hpp"
#include "ssl.hpp"

using namespace cass;

#define PROTOCOL_VERSION 4
#define PORT 9042

class ConnectionUnitTest : public testing::Test {
public:
  enum State {
    STATE_NEW,
    STATE_CONNECTED,
    STATE_ERROR,
    STATE_ERROR_RESPONSE,
    STATE_TIMEOUT,
    STATE_SUCCESS,
  };

  class RequestCallback : public SimpleRequestCallback {
  public:
    RequestCallback(Connection* connection, ConnectionUnitTest::State* state)
      : SimpleRequestCallback("SELECT * FROM blah")
      , state_(state)
      , connection_(connection) { }

    virtual void on_internal_set(ResponseMessage* response) {
      connection_->close();
      if (response->response_body()->opcode() == CQL_OPCODE_RESULT) {
        *state_ = ConnectionUnitTest::STATE_SUCCESS;
      } else {
        *state_ = ConnectionUnitTest::STATE_ERROR_RESPONSE;
      }
    }

    virtual void on_internal_error(CassError code, const String& message) {
      connection_->close();
      *state_ = ConnectionUnitTest::STATE_ERROR;
    }

    virtual void on_internal_timeout() {
      *state_ = ConnectionUnitTest::STATE_TIMEOUT;
      connection_->close();
    }

  private:
    ConnectionUnitTest::State* state_;
    Connection* connection_;
  };

  ConnectionUnitTest()
    : cluster_(mockssandra::SimpleRequestHandlerBuilder().build()) { }

  uv_loop_t* loop() { return &loop_; }

  ConnectionSettings use_ssl() {
    SslContext::Ptr ssl_context(SslContextFactory::create());

    String cert = cluster_.use_ssl();
    EXPECT_FALSE(cert.empty()) << "Unable to enable SSL";
    EXPECT_EQ(ssl_context->add_trusted_cert(cert.data(), cert.size()), CASS_OK);

    ConnectionSettings settings;
    settings.socket_settings.ssl_context = ssl_context;
    settings.socket_settings.hostname_resolution_enabled = true;

    return settings;
  }

  void start_all() {
    ASSERT_EQ(cluster_.start_all(), 0);
  }

  void stop_all() {
    cluster_.stop_all();
  }

  void use_close_immediately() {
    cluster_.use_close_immediately();
  }

  virtual void SetUp() {
    uv_loop_init(loop());
    saved_log_level_ = Logger::log_level();
    Logger::set_log_level(CASS_LOG_DISABLED);
  }

  virtual void TearDown() {
    uv_loop_close(loop());
    stop_all();
    Logger::set_log_level(saved_log_level_);
  }

  static void on_connection_connected(Connector* connector) {
    ASSERT_TRUE(connector->is_ok());
    State* state = static_cast<State*>(connector->data());
    *state = STATE_CONNECTED;
    connector->connection()->start_heartbeats();
    connector->connection()->write_and_flush(RequestCallback::Ptr(Memory::allocate<RequestCallback>(connector->connection().get(), state)));
  }

  static void on_connection_error_code(Connector* connector) {
    Connector::ConnectionError *error_code =
        static_cast<Connector::ConnectionError*>(connector->data());
    if (!connector->is_ok()) {
      *error_code = connector->error_code();
    }
  }

  static void on_connection_close(Connector* connector) {
    if (connector->error_code() == Connector::CONNECTION_ERROR_CLOSE) {
      bool* is_closed = static_cast<bool*>(connector->data());
      *is_closed = true;
    }
  }

private:
  uv_loop_t loop_;
  mockssandra::SimpleCluster cluster_;
  CassLogLevel saved_log_level_;
};


TEST_F(ConnectionUnitTest, Simple) {
  start_all();

  State state(STATE_NEW);
  Connector::Ptr connector(Memory::allocate<Connector>(Address("127.0.0.1", PORT),
                                                       PROTOCOL_VERSION,
                                                       static_cast<void*>(&state),
                                                       on_connection_connected));

  connector->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(state, STATE_SUCCESS);
}

TEST_F(ConnectionUnitTest, Keyspace) {
  mockssandra::SimpleRequestHandlerBuilder builder;
  builder
      .on(mockssandra::OPCODE_QUERY)
      .use_keyspace("foo")
      .validate_query().void_result();
  mockssandra::SimpleCluster cluster(builder.build());
  cluster.start_all();

  State state(STATE_NEW);
  Connector::Ptr connector(Memory::allocate<Connector>(Address("127.0.0.1", PORT),
                                                       PROTOCOL_VERSION,
                                                       static_cast<void*>(&state),
                                                       on_connection_connected));

  connector
      ->with_keyspace("foo")
      ->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(state, STATE_SUCCESS);
  EXPECT_EQ(connector->connection()->keyspace(), "foo");
}

TEST_F(ConnectionUnitTest, Auth) {
  mockssandra::SimpleRequestHandlerBuilder builder;
  builder
      .on(mockssandra::OPCODE_STARTUP)
      .authenticate("com.datastax.SomeAuthenticator");
  builder
      .on(mockssandra::OPCODE_AUTH_RESPONSE)
      .plaintext_auth("cassandra", "cassandra");

  mockssandra::SimpleCluster cluster(builder.build());
  cluster.start_all();

  State state(STATE_NEW);
  Connector::Ptr connector(Memory::allocate<Connector>(Address("127.0.0.1", PORT),
                                                       PROTOCOL_VERSION,
                                                       static_cast<void*>(&state),
                                                       on_connection_connected));

  ConnectionSettings settings;
  settings.auth_provider.reset(Memory::allocate<PlainTextAuthProvider>("cassandra", "cassandra"));

  connector
      ->with_settings(settings)
      ->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(state, STATE_SUCCESS);
}

TEST_F(ConnectionUnitTest, Ssl) {
  ConnectionSettings settings(use_ssl());

  start_all();

  State state(STATE_NEW);
  Connector::Ptr connector(Memory::allocate<Connector>(Address("127.0.0.1", PORT),
                                                       PROTOCOL_VERSION,
                                                       static_cast<void*>(&state),
                                                       on_connection_connected));
  connector
      ->with_settings(settings)
      ->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(state, STATE_SUCCESS);
}

TEST_F(ConnectionUnitTest, Refused) {
  // Don't start cluster

  Connector::ConnectionError error_code(Connector::CONNECTION_OK);
  Connector::Ptr connector(Memory::allocate<Connector>(Address("127.0.0.1", PORT),
                                                       PROTOCOL_VERSION,
                                                       static_cast<void*>(&error_code),
                                                       on_connection_error_code));
  connector->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(Connector::CONNECTION_ERROR_CONNECT, error_code);
}

TEST_F(ConnectionUnitTest, Close) {
  use_close_immediately();
  start_all();

  Vector<Connector::Ptr> connectors;

  bool is_closed(false);
  for (size_t i = 0; i < 100; ++i) {
    Connector::Ptr connector(Memory::allocate<Connector>(Address("127.0.0.1", PORT),
                                                         PROTOCOL_VERSION,
                                                         static_cast<void*>(&is_closed),
                                                         on_connection_close));
    connector->connect(loop());
    connectors.push_back(connector);
  }

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_TRUE(is_closed);
}

TEST_F(ConnectionUnitTest, SslClose) {
  ConnectionSettings settings(use_ssl());

  use_close_immediately();
  start_all();

  Vector<Connector::Ptr> connectors;

  bool is_closed(false);
  for (size_t i = 0; i < 100; ++i) {
    Connector::Ptr connector(Memory::allocate<Connector>(Address("127.0.0.1", PORT),
                                                         PROTOCOL_VERSION,
                                                         static_cast<void*>(&is_closed),
                                                         on_connection_close));
    connector
        ->with_settings(settings)
        ->connect(loop());
    connectors.push_back(connector);
  }

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_TRUE(is_closed);
}

TEST_F(ConnectionUnitTest, Cancel) {
  start_all();

  Vector<Connector::Ptr> connectors;

  Connector::ConnectionError error_code(Connector::CONNECTION_OK);
  for (size_t i = 0; i < 100; ++i) {
    Connector::Ptr connector(Memory::allocate<Connector>(Address("127.0.0.1", PORT),
                                                         PROTOCOL_VERSION,
                                                         static_cast<void*>(&error_code),
                                                         on_connection_error_code));
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

  EXPECT_EQ(Connector::CONNECTION_CANCELLED, error_code);
}

TEST_F(ConnectionUnitTest, SslCancel) {
  ConnectionSettings settings(use_ssl());

  start_all();

  Vector<Connector::Ptr> connectors;

  Connector::ConnectionError error_code(Connector::CONNECTION_OK);
  for (size_t i = 0; i < 100; ++i) {
    Connector::Ptr connector(Memory::allocate<Connector>(Address("127.0.0.1", PORT),
                                                         PROTOCOL_VERSION,
                                                         static_cast<void*>(&error_code),
                                                         on_connection_error_code));
    connector
        ->with_settings(settings)
        ->connect(loop());
    connectors.push_back(connector);
  }

  Vector<Connector::Ptr>::iterator it = connectors.begin();
  while (it != connectors.end()) {
    (*it)->cancel();
    uv_run(loop(), UV_RUN_NOWAIT);
    it++;
  }

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(Connector::CONNECTION_CANCELLED, error_code);
}

TEST_F(ConnectionUnitTest, Timeout) {
  mockssandra::RequestHandler::Builder builder;
  builder.on(mockssandra::OPCODE_STARTUP).no_result(); // Don't return a response

  mockssandra::SimpleCluster cluster(builder.build());
  cluster.start_all();

  Connector::ConnectionError error_code(Connector::CONNECTION_OK);
  Connector::Ptr connector(Memory::allocate<Connector>(Address("127.0.0.1", PORT),
                                                       PROTOCOL_VERSION,
                                                       static_cast<void*>(&error_code),
                                                       on_connection_error_code));

  ConnectionSettings settings;
  settings.connect_timeout_ms = 1000;

  connector
      ->with_settings(settings)
      ->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(Connector::CONNECTION_ERROR_TIMEOUT, error_code);
}

TEST_F(ConnectionUnitTest, InvalidKeyspace) {
  mockssandra::SimpleRequestHandlerBuilder builder;
  builder
      .on(mockssandra::OPCODE_QUERY)
      .use_keyspace("foo")
      .validate_query().void_result();
  mockssandra::SimpleCluster cluster(builder.build());
  cluster.start_all();

  Connector::ConnectionError error_code(Connector::CONNECTION_OK);
  Connector::Ptr connector(Memory::allocate<Connector>(Address("127.0.0.1", PORT),
                                                       PROTOCOL_VERSION,
                                                       static_cast<void*>(&error_code),
                                                       on_connection_error_code));
  connector
      ->with_keyspace("invalid")
      ->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(Connector::CONNECTION_ERROR_KEYSPACE, error_code);
}

TEST_F(ConnectionUnitTest, InvalidProtocol) {
  start_all();

  Connector::ConnectionError error_code(Connector::CONNECTION_OK);
  Connector::Ptr connector(Memory::allocate<Connector>(Address("127.0.0.1", PORT),
                                                       0x7F, // Invalid protocol version
                                                       static_cast<void*>(&error_code),
                                                       on_connection_error_code));
  connector
      ->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(Connector::CONNECTION_ERROR_INVALID_PROTOCOL, error_code);
}

TEST_F(ConnectionUnitTest, InvalidAuth) {
  mockssandra::SimpleRequestHandlerBuilder builder;
  builder
      .on(mockssandra::OPCODE_STARTUP)
      .authenticate("com.datastax.SomeAuthenticator");
  builder
      .on(mockssandra::OPCODE_AUTH_RESPONSE)
      .plaintext_auth("cassandra", "cassandra");

  mockssandra::SimpleCluster cluster(builder.build());
  cluster.start_all();

  Connector::ConnectionError error_code(Connector::CONNECTION_OK);
  Connector::Ptr connector(Memory::allocate<Connector>(Address("127.0.0.1", PORT),
                                                       PROTOCOL_VERSION,
                                                       static_cast<void*>(&error_code),
                                                       on_connection_error_code));

  ConnectionSettings settings;
  settings.auth_provider.reset(Memory::allocate<PlainTextAuthProvider>("invalid", "invalid"));

  connector
      ->with_settings(settings)
      ->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(Connector::CONNECTION_ERROR_AUTH, error_code);
}

TEST_F(ConnectionUnitTest, InvalidNoSsl) {
  start_all(); // Start without ssl

  Connector::ConnectionError error_code(Connector::CONNECTION_OK);
  Connector::Ptr connector(Memory::allocate<Connector>(Address("127.0.0.1", PORT),
                                                       PROTOCOL_VERSION,
                                                       static_cast<void*>(&error_code),
                                                       on_connection_error_code));

  SslContext::Ptr ssl_context(SslContextFactory::create());

  ConnectionSettings settings;
  settings.socket_settings.ssl_context = ssl_context;
  settings.socket_settings.hostname_resolution_enabled = true;

  connector
      ->with_settings(settings)
      ->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(Connector::CONNECTION_ERROR_SSL_HANDSHAKE, error_code);
}

TEST_F(ConnectionUnitTest, InvalidSsl) {
  use_ssl();
  start_all();

  Connector::ConnectionError error_code(Connector::CONNECTION_OK);
  Connector::Ptr connector(Memory::allocate<Connector>(Address("127.0.0.1", PORT),
                                                       PROTOCOL_VERSION,
                                                       static_cast<void*>(&error_code),
                                                       on_connection_error_code));

  SslContext::Ptr ssl_context(SslContextFactory::create()); // No trusted cert

  ConnectionSettings settings;
  settings.socket_settings.ssl_context = ssl_context;
  settings.socket_settings.hostname_resolution_enabled = true;

  connector
      ->with_settings(settings)
      ->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(Connector::CONNECTION_ERROR_SSL_VERIFY, error_code);
}
