/*
  Copyright (c) 2014-2017 DataStax

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

#include "socket_connector.hpp"

#include "config.hpp"
#include "logger.hpp"

#define SSL_HANDSHAKE_MAX_BUFFER_SIZE (16 * 1024 + 5)

namespace cass {

/**
 * A socket handler that handles the SSL handshake process.
 */
class SslHandshakeHandler : public SocketHandler {
public:
  SslHandshakeHandler(SocketConnector* connector)
    : connector_(connector) { }

  virtual void alloc_buffer(size_t suggested_size, uv_buf_t* buf) {
    buf->base = connector_->ssl_session_->incoming().peek_writable(&suggested_size);
    buf->len = suggested_size;
  }

  virtual void on_read(Socket* socket, ssize_t nread, const uv_buf_t* buf) {
    if (nread > 0) {
      connector_->ssl_session_->incoming().commit(nread);
      connector_->ssl_handshake();
    }
  }

  virtual void on_write(Socket* socket, int status, SocketRequest* request) {
    delete request;
    if (status != 0) {
      connector_->on_error(SocketConnector::SOCKET_ERROR_WRITE, "Write error");
    }
  }

  virtual void on_close() {
    if (connector_->is_cancelled()) {
      connector_->finish();
    } else {
      connector_->on_error(SocketConnector::SOCKET_ERROR_CLOSE, "Socket closed prematurely");
    }
  }

private:
  SocketConnector* connector_;
};

SocketSettings::SocketSettings(const Config& config)
  : hostname_resolution_enabled(config.use_hostname_resolution())
  , resolve_timeout_ms(config.resolve_timeout_ms())
  , ssl_context(config.ssl_context())
  , tcp_nodelay_enabled(config.tcp_nodelay_enable())
  , tcp_keepalive_enabled(config.tcp_keepalive_enable())
  , tcp_keepalive_delay_secs(config.tcp_keepalive_delay_secs()) { }

SocketConnector::SocketConnector(const Address& address, void* data, Callback callback)
  : address_(address)
  , data_(data)
  , callback_(callback)
  , socket_(NULL)
  , error_code_(SOCKET_OK)
  , ssl_error_code_(CASS_OK) { }

SocketConnector* SocketConnector::with_settings(const SocketSettings& settings) {
  settings_ = settings;
  return this;
}

void SocketConnector::connect(uv_loop_t* loop) {
  inc_ref(); // For the event loop

  if (settings_.hostname_resolution_enabled) {
    // Run hostname resolution then connect.
    resolver_.reset(Memory::allocate<NameResolver>(address_));
    resolver_->resolve(loop, this, on_resolve, settings_.resolve_timeout_ms);
  } else {
    internal_connect(loop);
  }
}

void SocketConnector::cancel() {
  error_code_ = SOCKET_CANCELLED;
  if (resolver_) resolver_->cancel();
  if (connector_) connector_->cancel();
  if (socket_) socket_->close();
}

void SocketConnector::internal_connect(uv_loop_t* loop) {
  Socket::Ptr socket(Memory::allocate<Socket>(address_));

  if (uv_tcp_init(loop, socket->handle()) != 0) {
    on_error(SOCKET_ERROR_INIT, "Unable to initalize TCP object");
    return;
  }

  socket_ = socket;
  socket_->inc_ref(); // For the event loop

  if (uv_tcp_nodelay(socket_->handle(),
                     settings_.tcp_nodelay_enabled ? 1 : 0) != 0) {
    LOG_WARN("Unable to set tcp nodelay");
  }

  if (uv_tcp_keepalive(socket_->handle(),
                       settings_.tcp_keepalive_enabled ? 1 : 0,
                       settings_.tcp_keepalive_delay_secs) != 0) {
    LOG_WARN("Unable to set tcp keepalive");
  }

  if (settings_.ssl_context) {
    ssl_session_.reset(settings_.ssl_context->create_session(address_, hostname_));
  }

  connector_.reset(Memory::allocate<TcpConnector>(address_));
  connector_->connect(socket_->handle(), this, on_connect);
}

void SocketConnector::ssl_handshake() {
  // Run the handshake process if not done which might create outgoing data
  // which is handled below.
  if (!ssl_session_->is_handshake_done()) {
    ssl_session_->do_handshake();
    if (ssl_session_->has_error()) {
      on_error(SOCKET_ERROR_SSL_HANDSHAKE,
               "Error during SSL handshake: " + ssl_session_->error_message());
      return;
    }
  }

  // Write any outgoing data created by the handshake process.
  char buf[SSL_HANDSHAKE_MAX_BUFFER_SIZE];
  size_t size = ssl_session_->outgoing().read(buf, SSL_HANDSHAKE_MAX_BUFFER_SIZE);
  if (size > 0) {
    socket_->write_and_flush(Memory::allocate<BufferSocketRequest>(Buffer(buf, size)));
  }

  // If the handshake process is done then verify the certificate and finish.
  if (ssl_session_->is_handshake_done()) {
    ssl_session_->verify();
    if (ssl_session_->has_error()) {
      on_error(SOCKET_ERROR_SSL_VERIFY,
               "Error verifying peer certificate: " + ssl_session_->error_message());
      return;
    }
    finish();
  }
}

void SocketConnector::finish() {
  if (socket_) socket_->set_handler(NULL);
  callback_(this);
  dec_ref();
}

void SocketConnector::on_error(SocketError code, const String& message) {
  assert(code != SOCKET_OK && "Notified error without an error");
  if (error_code_ == SOCKET_OK) { // Only call this once
    LOG_DEBUG("Lost connection to host %s with the following error: %s",
              address_.to_string().c_str(),
              message.c_str());
    error_message_ = message;
    error_code_ = code;
    if (is_ssl_error()) {
      ssl_error_code_ = ssl_session_->error_code();
    }
    if (socket_) socket_->defunct();
    finish();
  }
}

void SocketConnector::on_connect(TcpConnector* tcp_connector) {
  SocketConnector* connector = static_cast<SocketConnector*>(tcp_connector->data());
  connector->handle_connect(tcp_connector);
}

void SocketConnector::handle_connect(TcpConnector* tcp_connector) {
  if (tcp_connector->is_success()) {
    LOG_DEBUG("Connected to host %s on socket(%p)",
              address_.to_string().c_str(),
              static_cast<void*>(this));

#if defined(HAVE_NOSIGPIPE) && UV_VERSION_MAJOR >= 1
    uv_tcp_t* tcp = &socket_->tcp_;

    // This must be done after socket for the socket file descriptor to be
    // valid.
    uv_os_fd_t fd = 0;
    int enabled = 1;
    if (uv_fileno(reinterpret_cast<uv_handle_t*>(tcp), &fd) != 0 ||
        setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, (void *)&enabled, sizeof(int)) != 0) {
      LOG_WARN("Unable to set socket option SO_NOSIGPIPE for host %s",
               address_.to_string().c_str());
    }
#endif

    if (ssl_session_) {
      socket_->set_handler(Memory::allocate<SslHandshakeHandler>(this));
      ssl_handshake();
    } else {
      finish();
    }
  } else if (tcp_connector->is_cancelled()) {
    finish();
  } else {
    on_error(SOCKET_ERROR_CONNECT,
             "Connect error '" +
             String(UV_ERRSTR(tcp_connector->uv_status(), socket_->loop())) +
             "'");
  }
}

void SocketConnector::on_resolve(NameResolver* resolver) {
  SocketConnector* connector = static_cast<SocketConnector*>(resolver->data());
  connector->handle_resolve(resolver);
}

void SocketConnector::handle_resolve(NameResolver* resolver) {
  if (resolver->is_success()) {
    LOG_DEBUG("Resolved the hostname %s for address %s",
              resolver->hostname().c_str(),
              resolver->address().to_string().c_str());

    hostname_ = resolver->hostname();
    internal_connect(resolver->loop());
  } else if (resolver->is_cancelled()) {
    finish();
  } else if (resolver->is_timed_out()) {
    on_error(SOCKET_ERROR_RESOLVE_TIMEOUT,
             "Timed out attempting to resolve hostname");
  } else {
    on_error(SOCKET_ERROR_RESOLVE,
             "Unable to resolve hostname '" +
             String(UV_ERRSTR(resolver->uv_status(), socket_->loop())) +
             "'");
  }
}

} // namespace cass
