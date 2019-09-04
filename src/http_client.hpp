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

#ifndef DATASTAX_INTERNAL_HTTP_CLIENT_HPP
#define DATASTAX_INTERNAL_HTTP_CLIENT_HPP

#include "address.hpp"
#include "callback.hpp"
#include "cloud_secure_connection_config.hpp"
#include "event_loop.hpp"
#include "http_parser.h"
#include "ref_counted.hpp"
#include "socket_connector.hpp"
#include "string.hpp"

namespace datastax { namespace internal { namespace core {

class HttpClient : public RefCounted<HttpClient> {
public:
  typedef SharedRefPtr<HttpClient> Ptr;
  typedef internal::Callback<void, HttpClient*> Callback;

  enum HttpClientError {
    HTTP_CLIENT_OK,
    HTTP_CLIENT_CANCELED,
    HTTP_CLIENT_ERROR_SOCKET,
    HTTP_CLIENT_ERROR_PARSING,
    HTTP_CLIENT_ERROR_HTTP_STATUS,
    HTTP_CLIENT_ERROR_TIMEOUT,
    HTTP_CLIENT_ERROR_CLOSED
  };

  HttpClient(const Address& address, const String& path, const Callback& callback);

  HttpClient* with_settings(const SocketSettings& settings);

  HttpClient* with_request_timeout_ms(uint64_t request_timeout_ms);

  bool is_ok() const { return error_code_ == HTTP_CLIENT_OK; }
  bool is_error_status_code() const { return error_code_ == HTTP_CLIENT_ERROR_HTTP_STATUS; }
  bool is_canceled() const { return error_code_ == HTTP_CLIENT_CANCELED; }
  HttpClientError error_code() const { return error_code_; }
  String error_message() const { return error_message_; }
  unsigned status_code() const { return status_code_; }
  const String& content_type() const { return content_type_; }
  const String& response_body() const { return response_body_; }

  void request(uv_loop_t* loop);
  void cancel();

private:
  friend class HttpClientSocketHandler;
  friend class HttpClientSslSocketHandler;

private:
  void on_socket_connect(SocketConnector* connector);
  void on_read(char* buf, ssize_t nread);
  void on_timeout(Timer* timer);

  static int on_status(http_parser* parser, const char* buf, size_t len);
  int handle_status(unsigned status_code);
  static int on_header_field(http_parser* parser, const char* buf, size_t len);
  int handle_header_field(const char* buf, size_t len);
  static int on_header_value(http_parser* parser, const char* buf, size_t len);
  int handle_header_value(const char* buf, size_t len);
  static int on_body(http_parser* parser, const char* buf, size_t len);
  int handle_body(const char* buf, size_t len);
  static int on_message_complete(http_parser* parser);
  int handle_message_complete();

  void finish();

private:
  HttpClientError error_code_;
  String error_message_;
  Address address_;
  String path_;
  Callback callback_;
  SocketConnector::Ptr socket_connector_;
  Socket::Ptr socket_;
  Timer request_timer_;
  uint64_t request_timeout_ms_;
  http_parser parser_;
  http_parser_settings parser_settings_;
  String current_header_;
  unsigned status_code_;
  String content_type_;
  String response_body_;
};

}}} // namespace datastax::internal::core

#endif
