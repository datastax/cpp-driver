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

#ifndef HTTP_SERVER_TEST_HPP
#define HTTP_SERVER_TEST_HPP

#include "http_server.hpp"
#include "loop_test.hpp"
#include "socket_connector.hpp"

class HttpTest : public LoopTest {
public:
  ~HttpTest() { server_.close(); }

  const datastax::String& ca_cert() const { return ca_cert_; }
  const datastax::String& cert() const { return cert_; }
  const datastax::String& key() const { return key_; }

  void set_path(const datastax::String& path) { server_.set_path(path); }

  void set_content_type(const datastax::String& content_type) {
    server_.set_content_type(content_type);
  }

  void set_response_body(const datastax::String& response_body) {
    server_.set_response_body(response_body);
  }

  void set_response_status_code(int status_code) { server_.set_response_status_code(status_code); }

  void enable_valid_response(bool enable) { server_.enable_valid_response(enable); }

  void set_close_connnection_after_request(bool enable) {
    server_.set_close_connnection_after_request(enable);
  }

  void start_http_server() { server_.listen(); }
  void stop_http_server() { server_.close(); }

  datastax::internal::core::SocketSettings use_ssl(String cn = "127.0.0.1",
                                                   bool is_server_using_ssl = true) {
    datastax::internal::core::SocketSettings settings;

#ifdef HAVE_OPENSSL
    datastax::String ca_key = mockssandra::Ssl::generate_key();
    ca_cert_ = mockssandra::Ssl::generate_cert(ca_key, cn);
    key_ = mockssandra::Ssl::generate_key();
    cert_ = mockssandra::Ssl::generate_cert(key_, cn, ca_cert_, ca_key);

    datastax::internal::core::SslContext::Ptr ssl_context(
        datastax::internal::core::SslContextFactory::create());
    ssl_context->set_cert(cert().c_str(), cert().size());
    ssl_context->set_private_key(key().c_str(), key().size(), "",
                                 0); // No password expected for the private key
    ssl_context->add_trusted_cert(ca_cert().c_str(), ca_cert().size());

    settings.ssl_context = ssl_context;

    if (is_server_using_ssl) {
      server_.use_ssl(ca_key, ca_cert_, "", cert_);
    }
#endif

    return settings;
  }

  void use_ssl(const String& key, const String& cert, const String& ca_key, const String& ca_cert) {
#ifdef HAVE_OPENSSL
    key_ = key;
    cert_ = cert;
    ca_cert_ = ca_cert;

    server_.use_ssl(ca_key, ca_cert_, "", cert_);
#endif
  }

private:
  datastax::String ca_cert_;
  datastax::String cert_;
  datastax::String key_;
  mockssandra::http::Server server_;
};

#endif
