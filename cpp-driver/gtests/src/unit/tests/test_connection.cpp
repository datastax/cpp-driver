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

#include "auth.hpp"
#include "connector.hpp"
#include "constants.hpp"
#include "request_callback.hpp"
#include "ssl.hpp"

#ifdef WIN32
#undef STATUS_TIMEOUT
#endif

using namespace cass;

#define PROTOCOL_VERSION CASS_HIGHEST_SUPPORTED_PROTOCOL_VERSION
#define PORT 9042

class ConnectionUnitTest : public mockssandra::SimpleClusterTest {
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
      : status(STATUS_NEW) { }

    Connection::Ptr connection;
    Status status;
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

  uv_loop_t* loop() { return &loop_; }

  virtual void SetUp() {
    mockssandra::SimpleClusterTest::SetUp();
    uv_loop_init(loop());
  }

  virtual void TearDown() {
    uv_loop_close(loop());
    mockssandra::SimpleClusterTest::TearDown();
  }

  static void on_connection_connected(Connector* connector) {
    ASSERT_TRUE(connector->is_ok());
    State* state = static_cast<State*>(connector->data());
    state->status = STATUS_CONNECTED;
    state->connection = connector->release_connection();
    state->connection->start_heartbeats();
    state->connection->write_and_flush(RequestCallback::Ptr(Memory::allocate<RequestCallback>(state->connection.get(), state)));
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
};


TEST_F(ConnectionUnitTest, Simple) {
  start_all();

  State state;
  Connector::Ptr connector(Memory::allocate<Connector>(Address("127.0.0.1", PORT),
                                                       PROTOCOL_VERSION,
                                                       static_cast<void*>(&state),
                                                       on_connection_connected));

  connector->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(state.status, STATUS_SUCCESS);
}

TEST_F(ConnectionUnitTest, Keyspace) {
  mockssandra::SimpleRequestHandlerBuilder builder;
  builder
      .on(mockssandra::OPCODE_QUERY)
      .use_keyspace("foo")
      .validate_query().void_result();
  mockssandra::SimpleCluster cluster(builder.build());
  cluster.start_all();

  State state;
  Connector::Ptr connector(Memory::allocate<Connector>(Address("127.0.0.1", PORT),
                                                       PROTOCOL_VERSION,
                                                       static_cast<void*>(&state),
                                                       on_connection_connected));

  connector
      ->with_keyspace("foo")
      ->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(state.status, STATUS_SUCCESS);
  ASSERT_TRUE(static_cast<bool>(state.connection));
  EXPECT_EQ(state.connection->keyspace(), "foo");
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

  State state;
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

  EXPECT_EQ(state.status, STATUS_SUCCESS);
}

TEST_F(ConnectionUnitTest, Ssl) {
  ConnectionSettings settings(use_ssl());

  start_all();

  State state;
  Connector::Ptr connector(Memory::allocate<Connector>(Address("127.0.0.1", PORT),
                                                       PROTOCOL_VERSION,
                                                       static_cast<void*>(&state),
                                                       on_connection_connected));
  connector
      ->with_settings(settings)
      ->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(state.status, STATUS_SUCCESS);
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
  for (size_t i = 0; i < 10; ++i) {
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
  for (size_t i = 0; i < 10; ++i) {
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
  for (size_t i = 0; i < 10; ++i) {
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

  EXPECT_EQ(Connector::CONNECTION_CANCELED, error_code);
}

TEST_F(ConnectionUnitTest, SslCancel) {
  ConnectionSettings settings(use_ssl());

  start_all();

  Vector<Connector::Ptr> connectors;

  Connector::ConnectionError error_code(Connector::CONNECTION_OK);
  for (size_t i = 0; i < 10; ++i) {
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

  EXPECT_EQ(Connector::CONNECTION_CANCELED, error_code);
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
  settings.connect_timeout_ms = 200;

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
