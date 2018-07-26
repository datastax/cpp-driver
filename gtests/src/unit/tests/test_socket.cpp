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
#include "loop_test.hpp"

#include "connector.hpp"
#include "socket_connector.hpp"
#include "ssl.hpp"

using namespace cass;

class TestSocketHandler : public SocketHandler {
public:
  TestSocketHandler(String* result)
    : result_(result) { }

  virtual void on_read(Socket* socket, ssize_t nread, const uv_buf_t* buf) {
    if (nread > 0) {
      result_->append(buf->base, nread);
    }
    free_buffer(buf);
    if (result_->find("Closed") != std::string::npos) {
      socket->close();
    }
  }

  virtual void on_write(Socket* socket, int status, SocketRequest* request) {
    Memory::deallocate(request);
  }

  virtual void on_close() { }

private:
  String* result_;
};

class SslTestSocketHandler : public SslSocketHandler {
public:
  SslTestSocketHandler(SslSession* ssl_session, String* result)
    : SslSocketHandler(ssl_session)
    , result_(result) { }

  virtual void on_ssl_read(Socket* socket, char* buf, size_t size) {
    result_->append(buf, size);
    if (result_->find("Closed") != std::string::npos) {
      socket->close();
    }
  }

  virtual void on_write(Socket* socket, int status, SocketRequest* request) {
    Memory::deallocate(request);
  }

  virtual void on_close() { }

private:
  String* result_;
};

class SocketUnitTest : public LoopTest {
public:
  SocketSettings use_ssl() {
    SslContext::Ptr ssl_context(SslContextFactory::create());

    String cert = server_.use_ssl();
    EXPECT_FALSE(cert.empty()) << "Unable to enable SSL";
    EXPECT_EQ(ssl_context->add_trusted_cert(cert.data(), cert.size()), CASS_OK);

    SocketSettings settings;
    settings.ssl_context = ssl_context;
    settings.hostname_resolution_enabled = true;

    return settings;
  }

  void listen() {
    ASSERT_EQ(server_.listen(), 0);
  }

  void close() {
    server_.close();
  }

  void use_close_immediately() {
    server_.use_close_immediately();
  }

  virtual void SetUp() {
    LoopTest::SetUp();
    saved_log_level_ = Logger::log_level();
    Logger::set_log_level(CASS_LOG_DISABLED);
  }

  virtual void TearDown() {
    LoopTest::TearDown();
    close();
    Logger::set_log_level(saved_log_level_);
  }

  static void on_socket_connected(SocketConnector* connector, String* result) {
    Socket::Ptr socket = connector->release_socket();
    if (connector->error_code() == SocketConnector::SOCKET_OK) {
      if (connector->ssl_session()) {
        socket->set_handler(
              Memory::allocate<SslTestSocketHandler>(
                connector->ssl_session().release(), result));
      } else {
        socket->set_handler(
              Memory::allocate<TestSocketHandler>(result));
      }
      const char* data = "The socket is sucessfully connected and wrote data - ";
      socket->write(Memory::allocate<BufferSocketRequest>(Buffer(data, strlen(data))));
      socket->write(Memory::allocate<BufferSocketRequest>(Buffer("Closed", sizeof("Closed") - 1)));
      socket->flush();
    } else {
      ASSERT_TRUE(false) << "Failed to connect: " << connector->error_message();
    }
  }

  static void on_socket_refused(SocketConnector* connector, bool* is_refused) {
    if (connector->error_code() == SocketConnector::SOCKET_ERROR_CONNECT) {
      *is_refused = true;
    }
  }

  static void on_socket_closed(SocketConnector* connector, bool* is_closed) {
    if (connector->error_code() == SocketConnector::SOCKET_ERROR_CLOSE) {
      *is_closed = true;
    }
  }

  static void on_socket_canceled(SocketConnector* connector, bool* is_canceled) {
    if (connector->is_canceled()) {
      *is_canceled = true;
    }
  }

private:
  mockssandra::SimpleEchoServer server_;
  CassLogLevel saved_log_level_;
};

TEST_F(SocketUnitTest, Simple) {
  listen();

  String result;
  SocketConnector::Ptr connector(Memory::allocate<SocketConnector>(Address("127.0.0.1", 8888),
                                                                   cass::bind_callback(on_socket_connected, &result)));

  connector->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(result, "The socket is sucessfully connected and wrote data - Closed");
}

TEST_F(SocketUnitTest, Ssl) {
  listen();

  SocketSettings settings(use_ssl());

  String result;
  SocketConnector::Ptr connector(Memory::allocate<SocketConnector>(Address("127.0.0.1", 8888),
                                                                   cass::bind_callback(on_socket_connected, &result)));


  connector->with_settings(settings)
           ->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(result, "The socket is sucessfully connected and wrote data - Closed");
}

TEST_F(SocketUnitTest, Refused) {
  bool is_refused = false;
  SocketConnector::Ptr connector(Memory::allocate<SocketConnector>(Address("127.0.0.1", 8888),
                                                                   cass::bind_callback(on_socket_refused, &is_refused)));
  connector->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_TRUE(is_refused);
}

TEST_F(SocketUnitTest, SslClose) {
  use_close_immediately();
  listen();

  SocketSettings settings(use_ssl());

  Vector<SocketConnector::Ptr> connectors;

  bool is_closed = false;
  for (size_t i = 0; i < 10; ++i) {
    SocketConnector::Ptr connector(Memory::allocate<SocketConnector>(Address("127.0.0.1", 8888),
                                                                     cass::bind_callback(on_socket_closed, &is_closed)));

    connector
        ->with_settings(settings)
        ->connect(loop());
    connectors.push_back(connector);
  }

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_TRUE(is_closed);
}

TEST_F(SocketUnitTest, Cancel) {
  listen();

  Vector<SocketConnector::Ptr> connectors;

  bool is_canceled = false;
  for (size_t i = 0; i < 10; ++i) {
    SocketConnector::Ptr connector(Memory::allocate<SocketConnector>(Address("127.0.0.1", 8888),
                                                                     cass::bind_callback(on_socket_canceled, &is_canceled)));
    connector->connect(loop());
    connectors.push_back(connector);
  }

  Vector<SocketConnector::Ptr>::iterator it = connectors.begin();
  while (it != connectors.end()) {
    (*it)->cancel();
    uv_run(loop(), UV_RUN_NOWAIT);
    it++;
  }

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_TRUE(is_canceled);
}

TEST_F(SocketUnitTest, SslCancel) {
  listen();

  SocketSettings settings(use_ssl());

  Vector<SocketConnector::Ptr> connectors;

  bool is_canceled = false;
  for (size_t i = 0; i < 10; ++i) {
    SocketConnector::Ptr connector(Memory::allocate<SocketConnector>(Address("127.0.0.1", 8888),
                                                                     cass::bind_callback(on_socket_canceled, &is_canceled)));
    connector->with_settings(settings)
             ->connect(loop());
    connectors.push_back(connector);
  }

  Vector<SocketConnector::Ptr>::iterator it = connectors.begin();
  while (it != connectors.end()) {
    (*it)->cancel();
    uv_run(loop(), UV_RUN_NOWAIT);
    it++;
  }

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_TRUE(is_canceled);
}
