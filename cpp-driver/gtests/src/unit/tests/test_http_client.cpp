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

#include "http_test.hpp"

#include "driver_info.hpp"
#include "http_client.hpp"

using namespace datastax::internal;
using datastax::internal::core::HttpClient;
using datastax::internal::core::SocketSettings;
using datastax::internal::core::SslContext;
using datastax::internal::core::SslContextFactory;
using mockssandra::Ssl;

class HttpClientUnitTest : public HttpTest {
public:
  static void on_success_response(HttpClient* client, bool* flag) {
    *flag = true;
    EXPECT_TRUE(client->is_ok());
    EXPECT_EQ("text/plain", client->content_type());
    EXPECT_EQ(echo_response(), client->response_body());
  }

  static void on_failed_response(HttpClient* client, bool* flag) {
    *flag = true;
    EXPECT_FALSE(client->is_ok());
  }

private:
  static String echo_response() {
    OStringStream ss;

    ss << "GET / HTTP/1.0\r\n"
       << "Host: " HTTP_MOCK_SERVER_IP << ":" << HTTP_MOCK_SERVER_PORT << "\r\n"
       << "User-Agent: cpp-driver/" << driver_version() << "\r\nAccept: */*\r\n\r\n";

    return ss.str();
  }
};

TEST_F(HttpClientUnitTest, Simple) {
  start_http_server();

  bool is_success = false;
  HttpClient::Ptr client(new HttpClient(Address(HTTP_MOCK_SERVER_IP, HTTP_MOCK_SERVER_PORT), "/",
                                        bind_callback(on_success_response, &is_success)));
  client->request(loop());
  uv_run(loop(), UV_RUN_DEFAULT);
  ASSERT_TRUE(is_success);

  stop_http_server();
}

TEST_F(HttpClientUnitTest, InvalidHttpServer) {
  bool is_failed = false;
  HttpClient::Ptr client(new HttpClient(Address(HTTP_MOCK_SERVER_IP, HTTP_MOCK_SERVER_PORT), "/",
                                        bind_callback(on_failed_response, &is_failed)));
  client->request(loop());
  uv_run(loop(), UV_RUN_DEFAULT);
  ASSERT_TRUE(is_failed);
}

TEST_F(HttpClientUnitTest, InvalidHttpServerResponse) {
  enable_valid_response(false);
  start_http_server();

  bool is_failed = false;
  HttpClient::Ptr client(new HttpClient(Address(HTTP_MOCK_SERVER_IP, HTTP_MOCK_SERVER_PORT), "/",
                                        bind_callback(on_failed_response, &is_failed)));
  client->request(loop());
  uv_run(loop(), UV_RUN_DEFAULT);
  ASSERT_TRUE(is_failed);

  stop_http_server();
}

TEST_F(HttpClientUnitTest, InvalidEndpoint) {
  start_http_server();

  bool is_failed = false;
  HttpClient::Ptr client(new HttpClient(Address(HTTP_MOCK_SERVER_IP, HTTP_MOCK_SERVER_PORT),
                                        "/invalid", bind_callback(on_failed_response, &is_failed)));
  client->request(loop());
  uv_run(loop(), UV_RUN_DEFAULT);
  ASSERT_TRUE(is_failed);

  stop_http_server();
}

#ifdef HAVE_OPENSSL
TEST_F(HttpClientUnitTest, SimpleSsl) {
  SocketSettings settings = use_ssl();
  start_http_server();

  bool is_success = false;
  HttpClient::Ptr client(new HttpClient(Address(HTTP_MOCK_SERVER_IP, HTTP_MOCK_SERVER_PORT), "/",
                                        bind_callback(on_success_response, &is_success)));
  client->with_settings(settings)->request(loop());
  uv_run(loop(), UV_RUN_DEFAULT);
  ASSERT_TRUE(is_success);

  stop_http_server();
}

TEST_F(HttpClientUnitTest, InvalidEndpointSsl) {
  SocketSettings settings = use_ssl();
  start_http_server();

  bool is_failed = false;
  HttpClient::Ptr client(new HttpClient(Address(HTTP_MOCK_SERVER_IP, HTTP_MOCK_SERVER_PORT),
                                        "/invalid", bind_callback(on_failed_response, &is_failed)));
  client->with_settings(settings)->request(loop());
  uv_run(loop(), UV_RUN_DEFAULT);
  ASSERT_TRUE(is_failed);

  stop_http_server();
}

TEST_F(HttpClientUnitTest, InvalidClientSslNotConfigured) {
  use_ssl();
  start_http_server();

  bool is_failed = false;
  HttpClient::Ptr client(new HttpClient(Address(HTTP_MOCK_SERVER_IP, HTTP_MOCK_SERVER_PORT), "/",
                                        bind_callback(on_failed_response, &is_failed)));
  client->request(loop());
  uv_run(loop(), UV_RUN_DEFAULT);
  ASSERT_TRUE(is_failed);

  stop_http_server();
}

TEST_F(HttpClientUnitTest, InvalidServerSslNotConfigured) {
  SocketSettings settings = use_ssl("127.0.0.1", false);
  start_http_server();

  bool is_failed = false;
  HttpClient::Ptr client(new HttpClient(Address(HTTP_MOCK_SERVER_IP, HTTP_MOCK_SERVER_PORT), "/",
                                        bind_callback(on_failed_response, &is_failed)));
  client->with_settings(settings)->request(loop());
  uv_run(loop(), UV_RUN_DEFAULT);
  ASSERT_TRUE(is_failed);

  stop_http_server();
}
#endif
