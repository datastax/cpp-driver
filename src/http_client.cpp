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

#include "http_client.hpp"

#include "constants.hpp"
#include "driver_info.hpp"

using namespace datastax;
using namespace datastax::internal::core;

namespace datastax { namespace internal { namespace core {

class HttpClientSocketHandler : public SocketHandler {
public:
  HttpClientSocketHandler(HttpClient* client)
      : client_(client) {}

  virtual void on_read(Socket* socket, ssize_t nread, const uv_buf_t* buf) {
    client_->on_read(buf->base, nread);
    free_buffer(buf);
  }

  virtual void on_write(Socket* socket, int status, SocketRequest* request) { delete request; }

  virtual void on_close() { client_->finish(); }

private:
  HttpClient* client_;
};

class HttpClientSslSocketHandler : public SslSocketHandler {
public:
  HttpClientSslSocketHandler(SslSession* ssl_session, HttpClient* client)
      : SslSocketHandler(ssl_session)
      , client_(client) {}

  virtual void on_ssl_read(Socket* socket, char* buf, size_t size) { client_->on_read(buf, size); }

  virtual void on_write(Socket* socket, int status, SocketRequest* request) { delete request; }

  virtual void on_close() { client_->finish(); }

private:
  HttpClient* client_;
};

}}} // namespace datastax::internal::core

HttpClient::HttpClient(const Address& address, const String& path, const Callback& callback)
    : error_code_(HTTP_CLIENT_OK)
    , address_(address)
    , path_(path)
    , callback_(callback)
    , socket_connector_(
          new SocketConnector(address, bind_callback(&HttpClient::on_socket_connect, this)))
    , request_timeout_ms_(CASS_DEFAULT_CONNECT_TIMEOUT_MS)
    , status_code_(0) {
  http_parser_init(&parser_, HTTP_RESPONSE);
  http_parser_settings_init(&parser_settings_);

  parser_.data = this;
  parser_settings_.on_status = on_status;
  parser_settings_.on_header_field = on_header_field;
  parser_settings_.on_header_value = on_header_value;
  parser_settings_.on_body = on_body;
  parser_settings_.on_message_complete = on_message_complete;
}

HttpClient* HttpClient::with_settings(const SocketSettings& settings) {
  socket_connector_->with_settings(settings);
  return this;
}

HttpClient* HttpClient::with_request_timeout_ms(uint64_t request_timeout_ms) {
  request_timeout_ms_ = request_timeout_ms;
  return this;
}

void HttpClient::request(uv_loop_t* loop) {
  inc_ref();
  socket_connector_->connect(loop);
  if (request_timeout_ms_ > 0) {
    request_timer_.start(loop, request_timeout_ms_, bind_callback(&HttpClient::on_timeout, this));
  }
}

void HttpClient::cancel() {
  error_code_ = HTTP_CLIENT_CANCELED;
  socket_connector_->cancel();
  if (socket_) socket_->close();
  request_timer_.stop();
}

void HttpClient::on_socket_connect(SocketConnector* connector) {
  if (connector->is_ok()) {
    socket_ = connector->release_socket();
    if (connector->ssl_session()) {
      socket_->set_handler(
          new HttpClientSslSocketHandler(connector->ssl_session().release(), this));
    } else {
      socket_->set_handler(new HttpClientSocketHandler(this));
    }

    OStringStream ss;
    ss << "GET " << path_ << " HTTP/1.0\r\n" // HTTP/1.0 ensures chunked responses are not sent
       << "Host: " << address_.to_string(false) << "\r\n"
       << "User-Agent: cpp-driver/" << driver_version() << "\r\nAccept: */*\r\n\r\n";

    String request = ss.str();
    socket_->write_and_flush(new BufferSocketRequest(Buffer(request.c_str(), request.size())));
  } else {
    if (!connector->is_canceled()) {
      error_code_ = HTTP_CLIENT_ERROR_SOCKET;
      error_message_ = "Failed to establish HTTP connection: " + connector->error_message();
    }
    finish();
  }
}

void HttpClient::on_read(char* buf, ssize_t nread) {
  if (is_canceled()) return;

  if (nread > 0) {
    size_t parsed = http_parser_execute(&parser_, &parser_settings_, buf, nread);
    if (parsed < static_cast<size_t>(nread)) {
      error_code_ = HTTP_CLIENT_ERROR_PARSING;
      OStringStream ss;
      enum http_errno err = HTTP_PARSER_ERRNO(&parser_);
      ss << "HTTP parsing error (" << http_errno_name(err) << "):" << http_errno_description(err);
      error_message_ = ss.str();
      socket_->close();
    }
  } else if (is_ok() && status_code_ == 0) { // Make sure there wasn't an existing error
    error_code_ = HTTP_CLIENT_ERROR_CLOSED;
    error_message_ = "HTTP connection prematurely closed";
  }
}

void HttpClient::on_timeout(Timer* timer) {
  error_code_ = HTTP_CLIENT_ERROR_TIMEOUT;
  OStringStream ss;
  ss << "HTTP request timed out after " << request_timeout_ms_ << " ms";
  error_message_ = ss.str();
  socket_connector_->cancel();
  if (socket_) socket_->close();
}

int HttpClient::on_status(http_parser* parser, const char* buf, size_t len) {
  HttpClient* self = static_cast<HttpClient*>(parser->data);
  return self->handle_status(parser->status_code);
}

int HttpClient::handle_status(unsigned status_code) {
  if (status_code < 200 || status_code > 299) {
    error_code_ = HTTP_CLIENT_ERROR_HTTP_STATUS;
  }
  status_code_ = status_code;
  return 0;
}

int HttpClient::on_header_field(http_parser* parser, const char* buf, size_t len) {
  HttpClient* self = static_cast<HttpClient*>(parser->data);
  return self->handle_header_field(buf, len);
}

int HttpClient::handle_header_field(const char* buf, size_t len) {
  current_header_.assign(buf, len);
  return 0;
}

int HttpClient::on_header_value(http_parser* parser, const char* buf, size_t len) {
  HttpClient* self = static_cast<HttpClient*>(parser->data);
  return self->handle_header_value(buf, len);
}

int HttpClient::handle_header_value(const char* buf, size_t len) {
  if (StringRef(current_header_).iequals("content-type")) {
    content_type_.assign(buf, len);
  }
  return 0;
}

int HttpClient::on_body(http_parser* parser, const char* buf, size_t len) {
  HttpClient* self = static_cast<HttpClient*>(parser->data);
  return self->handle_body(buf, len);
}

int HttpClient::handle_body(const char* buf, size_t len) {
  response_body_.assign(buf, len);
  return 0;
}

int HttpClient::on_message_complete(http_parser* parser) {
  HttpClient* self = static_cast<HttpClient*>(parser->data);
  return self->handle_message_complete();
}

int HttpClient::handle_message_complete() {
  socket_->close();
  return 0;
}

void HttpClient::finish() {
  request_timer_.stop();
  if (callback_) callback_(this);
  dec_ref();
}
