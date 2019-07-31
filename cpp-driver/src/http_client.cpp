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

#include "driver_info.hpp"
#include "logger.hpp"

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

  virtual void on_close() { client_->handle_socket_close(); }

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

  virtual void on_close() { client_->handle_socket_close(); }

private:
  HttpClient* client_;
};

}}} // namespace datastax::internal::core

HttpClient::HttpClient(const Address& address, const String& path, const Callback& callback)
    : address_(address)
    , path_(path)
    , callback_(callback)
    , socket_connector_(
          new SocketConnector(address, bind_callback(&HttpClient::on_socket_connect, this)))
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

void HttpClient::request(uv_loop_t* loop) {
  inc_ref();
  socket_connector_->connect(loop);
}

void HttpClient::cancel() { socket_connector_->cancel(); }

void HttpClient::on_socket_connect(SocketConnector* connector) {
  if (connector->error_code() == SocketConnector::SOCKET_OK) {
    socket_ = connector->release_socket();
    if (connector->ssl_session()) {
      socket_->set_handler(
          new HttpClientSslSocketHandler(connector->ssl_session().release(), this));
    } else {
      socket_->set_handler(new HttpClientSocketHandler(this));
    }

    OStringStream ss;
    ss << "GET " << path_ << " HTTP/1.0\r\n" // HTTP/1.0 ensures chunked responses are not sent
       << "Host: " << socket_->address().to_string(true) << "\r\n"
       << "User-Agent: cpp-driver/" << driver_version() << "\r\nAccept: */*\r\n\r\n";

    String request = ss.str();
    socket_->write_and_flush(new BufferSocketRequest(Buffer(request.c_str(), request.size())));
  } else {
    LOG_ERROR("Failed to connect to address %s: %s", address_.to_string(true).c_str(),
              connector->error_message().c_str());
    finish();
  }
}

void HttpClient::handle_socket_close() { finish(); }

void HttpClient::on_read(char* buf, ssize_t nread) {
  if (nread > 0) {
    size_t parsed = http_parser_execute(&parser_, &parser_settings_, buf, nread);
    if (parsed < static_cast<size_t>(nread)) {
      enum http_errno err = HTTP_PARSER_ERRNO(&parser_);
      LOG_ERROR("%s: %s", http_errno_name(err), http_errno_description(err));
      socket_->close();
    }

    if (!is_ok()) { // No more data is available
      socket_->close();
    }
  } else if (nread != UV_EOF) {
    LOG_ERROR("Read error: %s", uv_strerror(nread));
    socket_->close();
  }
}

int HttpClient::on_status(http_parser* parser, const char* buf, size_t len) {
  HttpClient* self = static_cast<HttpClient*>(parser->data);
  return self->handle_status(parser->status_code);
}

int HttpClient::handle_status(unsigned status_code) {
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
  if (callback_) callback_(this);
  dec_ref();
}
