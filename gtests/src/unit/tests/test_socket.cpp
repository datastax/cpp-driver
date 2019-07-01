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
#
#include "connector.hpp"
#include "socket_connector.hpp"
#include "ssl.hpp"

#define SSL_VERIFY_PEER_DNS_RELATIVE_HOSTNAME "cpp-driver.hostname"
#define SSL_VERIFY_PEER_DNS_ABSOLUTE_HOSTNAME SSL_VERIFY_PEER_DNS_RELATIVE_HOSTNAME "."
#define SSL_VERIFY_PEER_DNS_IP_ADDRESS "127.254.254.254"

using namespace datastax;
using namespace datastax::internal;
using namespace datastax::internal::core;

class TestSocketHandler : public SocketHandler {
public:
  TestSocketHandler(String* result)
      : result_(result) {}

  virtual void on_read(Socket* socket, ssize_t nread, const uv_buf_t* buf) {
    if (nread > 0) {
      result_->append(buf->base, nread);
    }
    free_buffer(buf);
    if (result_->find("Closed") != std::string::npos) {
      socket->close();
    }
  }

  virtual void on_write(Socket* socket, int status, SocketRequest* request) { delete request; }

  virtual void on_close() {}

private:
  String* result_;
};

class SslTestSocketHandler : public SslSocketHandler {
public:
  SslTestSocketHandler(SslSession* ssl_session, String* result)
      : SslSocketHandler(ssl_session)
      , result_(result) {}

  virtual void on_ssl_read(Socket* socket, char* buf, size_t size) {
    result_->append(buf, size);
    if (result_->find("Closed") != std::string::npos) {
      socket->close();
    }
  }

  virtual void on_write(Socket* socket, int status, SocketRequest* request) { delete request; }

  virtual void on_close() {}

private:
  String* result_;
};

class SocketUnitTest : public LoopTest {
public:
  SocketSettings use_ssl(const String& cn = "") {
    SslContext::Ptr ssl_context(SslContextFactory::create());

    String cert = server_.use_ssl(cn);
    EXPECT_FALSE(cert.empty()) << "Unable to enable SSL";
    EXPECT_EQ(ssl_context->add_trusted_cert(cert.data(), cert.size()), CASS_OK);

    SocketSettings settings;
    settings.ssl_context = ssl_context;
    settings.hostname_resolution_enabled = true;

    return settings;
  }

  void listen() { ASSERT_EQ(server_.listen(), 0); }

  void reset(const Address& address) { server_.reset(address); }

  void close() { server_.close(); }

  void use_close_immediately() { server_.use_close_immediately(); }

  virtual void TearDown() {
    LoopTest::TearDown();
    close();
  }

  static void on_socket_connected(SocketConnector* connector, String* result) {
    Socket::Ptr socket = connector->release_socket();
    if (connector->error_code() == SocketConnector::SOCKET_OK) {
      if (connector->ssl_session()) {
        socket->set_handler(new SslTestSocketHandler(connector->ssl_session().release(), result));
      } else {
        socket->set_handler(new TestSocketHandler(result));
      }
      const char* data = "The socket is successfully connected and wrote data - ";
      socket->write(new BufferSocketRequest(Buffer(data, strlen(data))));
      socket->write(new BufferSocketRequest(Buffer("Closed", sizeof("Closed") - 1)));
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

  static void on_request(uv_getnameinfo_t* handle, int status, const char* hostname,
                         const char* service) {
    if (status) {
      FAIL() << "Unable to Execute Test SocketUnitTest.SslVerifyIdentityDns: "
             << "Add /etc/hosts entry " << SSL_VERIFY_PEER_DNS_IP_ADDRESS << "\t"
             << SSL_VERIFY_PEER_DNS_ABSOLUTE_HOSTNAME;
    } else if (String(hostname) != String(SSL_VERIFY_PEER_DNS_ABSOLUTE_HOSTNAME)) {
      FAIL() << "Invalid /etc/hosts entry for: '" << hostname << "' != '"
             << SSL_VERIFY_PEER_DNS_ABSOLUTE_HOSTNAME << "'";
    }
  }

private:
  mockssandra::SimpleEchoServer server_;
};

TEST_F(SocketUnitTest, Simple) {
  listen();

  String result;
  SocketConnector::Ptr connector(
      new SocketConnector(Address("127.0.0.1", 8888), bind_callback(on_socket_connected, &result)));

  connector->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(result, "The socket is successfully connected and wrote data - Closed");
}

TEST_F(SocketUnitTest, Ssl) {
  listen();

  SocketSettings settings(use_ssl());

  String result;
  SocketConnector::Ptr connector(
      new SocketConnector(Address("127.0.0.1", 8888), bind_callback(on_socket_connected, &result)));

  connector->with_settings(settings)->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(result, "The socket is successfully connected and wrote data - Closed");
}

TEST_F(SocketUnitTest, Refused) {
  bool is_refused = false;
  SocketConnector::Ptr connector(new SocketConnector(
      Address("127.0.0.1", 8888), bind_callback(on_socket_refused, &is_refused)));
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
    SocketConnector::Ptr connector(new SocketConnector(
        Address("127.0.0.1", 8888), bind_callback(on_socket_closed, &is_closed)));

    connector->with_settings(settings)->connect(loop());
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
    SocketConnector::Ptr connector(new SocketConnector(
        Address("127.0.0.1", 8888), bind_callback(on_socket_canceled, &is_canceled)));
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
    SocketConnector::Ptr connector(new SocketConnector(
        Address("127.0.0.1", 8888), bind_callback(on_socket_canceled, &is_canceled)));
    connector->with_settings(settings)->connect(loop());
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

TEST_F(SocketUnitTest, SslVerifyIdentity) {
  listen();

  SocketSettings settings(use_ssl("127.0.0.1"));
  settings.ssl_context->set_verify_flags(CASS_SSL_VERIFY_PEER_IDENTITY);

  String result;
  SocketConnector::Ptr connector(
      new SocketConnector(Address("127.0.0.1", 8888), bind_callback(on_socket_connected, &result)));

  connector->with_settings(settings)->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(result, "The socket is successfully connected and wrote data - Closed");
}

TEST_F(SocketUnitTest, SslVerifyIdentityDns) {
  // Verify address can be resolved
  Address verify_entry;
  Address::from_string(SSL_VERIFY_PEER_DNS_IP_ADDRESS, 8888, &verify_entry);
  uv_getnameinfo_t request;
  ASSERT_EQ(0, uv_getnameinfo(loop(), &request, on_request,
                              static_cast<const Address>(verify_entry).addr(), 0));
  uv_run(loop(), UV_RUN_DEFAULT);
  if (this->HasFailure()) { // Make test fail due to DNS not configured
    return;
  }

  reset(Address(SSL_VERIFY_PEER_DNS_IP_ADDRESS,
                8888)); // Ensure the echo server is listening on the correct address
  listen();

  SocketSettings settings(use_ssl(SSL_VERIFY_PEER_DNS_RELATIVE_HOSTNAME));
  settings.ssl_context->set_verify_flags(CASS_SSL_VERIFY_PEER_IDENTITY_DNS);

  String result;
  SocketConnector::Ptr connector(new SocketConnector(Address(SSL_VERIFY_PEER_DNS_IP_ADDRESS, 8888),
                                                     bind_callback(on_socket_connected, &result)));

  connector->with_settings(settings)->connect(loop());

  uv_run(loop(), UV_RUN_DEFAULT);

  EXPECT_EQ(result, "The socket is successfully connected and wrote data - Closed");
}
