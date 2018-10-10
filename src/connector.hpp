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

#include "auth.hpp"
#include "callback.hpp"
#include "connection.hpp"
#include "socket_connector.hpp"

#ifndef __CASS_CONNECTION_CONNECTOR_HPP_INCLUDED__
#define __CASS_CONNECTION_CONNECTOR_HPP_INCLUDED__

namespace cass {

class AuthResponseRequest;
class Config;
class ResponseMessage;
class RequestCallback;

/**
 * Connection settings.
 */
struct ConnectionSettings {
  /**
   * Constructor. Initialize with default settings.
   */
  ConnectionSettings();

  /**
   * Constructor. Initialize connection settings from a config object.
   *
   * @param config The config object.
   */
  ConnectionSettings(const Config& config);

  SocketSettings socket_settings;
  uint64_t connect_timeout_ms;
  AuthProvider::Ptr auth_provider;
  unsigned int idle_timeout_secs;
  unsigned int heartbeat_interval_secs;
  bool no_compact;
};

/**
 * A connector. This contains all the initialization code to connect a
 * connection.
 */
class Connector
    : public RefCounted<Connector>
    , public RecordingConnectionListener {
  friend class StartupCallback;

public:
  typedef SharedRefPtr<Connector> Ptr;
  typedef Vector<Ptr> Vec;

  typedef cass::Callback<void, Connector*> Callback;

  enum ConnectionError {
    CONNECTION_OK,
    CONNECTION_CANCELED,
    CONNECTION_ERROR_AUTH,
    CONNECTION_ERROR_CONNECT,
    CONNECTION_ERROR_CLOSE,
    CONNECTION_ERROR_INTERNAL,
    CONNECTION_ERROR_INVALID_OPCODE,
    CONNECTION_ERROR_INVALID_PROTOCOL,
    CONNECTION_ERROR_KEYSPACE,
    CONNECTION_ERROR_RESPONSE,
    CONNECTION_ERROR_SSL_HANDSHAKE,
    CONNECTION_ERROR_SSL_VERIFY,
    CONNECTION_ERROR_SOCKET,
    CONNECTION_ERROR_TIMEOUT
  };

public:
  /**
   * Constructor
   *
   * @param address The address to connect to.
   * @param protocol_version The protocol version to use for the connection.
   * @param callback A callback that is called when the connection is connected or
   * if an error occurred.
   */
  Connector(const Address& address,
            int protocol_version,
            const Callback& callback);

  /**
   * Set the keyspace to connect with. Calls "USE <keyspace>" after
   * the connection is connected and protocol handshake is completed.
   *
   * @param keyspace A keyspace to register after connection.
   * @return The connector to chain calls.
   */
  Connector* with_keyspace(const String& keyspace);

  /**
   * Set the event types to register for during the protocol handshake.
   *
   * @param event_types A bit set of event types to register.
   * @return The connector to chain calls.
   */
  Connector* with_event_types(int event_types);

  /**
   * Set the connection listener to use after the connection is connected.
   *
   * @param listener A listener that handles connection events.
   * @return The connector to chain calls.
   */
  Connector* with_listener(ConnectionListener* listener);

  /**
   * Set the metrics object to use to record metrics.
   *
   * @param metrics A metrics object.
   * @return The connector to chain calls.
   */
  Connector* with_metrics(Metrics* metrics);

  /**
   * Set the connection and socket settings.
   *
   * @param The settings to use for connecting the connection.
   * @return The connector to chain calls.
   */
  Connector* with_settings(const ConnectionSettings& settings);

  /**
   * Connect the connection.
   *
   * @param loop An event loop to use for connecting the connection.
   */
  void connect(uv_loop_t* loop);

  /**
   * Cancel the connection process.
   */
  void cancel();

  /**
   * Release the connection from the connector. If not released in the callback
   * the connection automatically be closed.
   *
   * @return The connection object for this connector. This returns a null object
   * if the connection is not connected or an error occurred.
   */
  Connection::Ptr release_connection();

public:
  uv_loop_t* loop() { return loop_; }

  const Address& address() const { return socket_connector_->address(); }


  bool is_ok() const {
    return error_code_ == CONNECTION_OK;
  }
  bool is_canceled() const {
    return error_code_ == CONNECTION_CANCELED;
  }
  bool is_invalid_protocol() const {
    return error_code_ == CONNECTION_ERROR_INVALID_PROTOCOL;
  }
  bool is_auth_error() const {
    return error_code_ == CONNECTION_ERROR_AUTH;
  }
  bool is_ssl_error() const {
    return error_code_ == CONNECTION_ERROR_SSL_HANDSHAKE ||
        error_code_ == CONNECTION_ERROR_SSL_VERIFY;
  }
  bool is_timeout_error() const {
    return error_code_ == CONNECTION_ERROR_TIMEOUT;
  }
  bool is_keyspace_error() const {
    return error_code_ == CONNECTION_ERROR_KEYSPACE;
  }
  bool is_critical_error() const {
    return is_auth_error() || is_ssl_error() ||
        is_invalid_protocol() || is_keyspace_error();
  }

  ConnectionError error_code() { return error_code_; }
  const String& error_message() { return error_message_; }

  SocketConnector::SocketError socket_error_code() { return socket_connector_->error_code(); }
  const String& socket_error_message() { return socket_connector_->error_message(); }
  CassError ssl_error_code() { return socket_connector_->ssl_error_code(); }

private:
  void finish();

  void on_error(ConnectionError code, const String& message);
  void on_ready_or_set_keyspace();
  void on_ready_or_register_for_events();
  void on_supported(ResponseMessage* response);

  void on_authenticate(const String& class_name);
  void on_auth_challenge(const AuthResponseRequest* request, const String& token);
  void on_auth_success(const AuthResponseRequest* request, const String& token);

  virtual void on_close(Connection* connection);

  void on_connect(SocketConnector* socket_connector);
  void on_timeout(Timer* timer);

private:
  Callback callback_;
  uv_loop_t* loop_;

  Connection::Ptr connection_;
  SocketConnector::Ptr socket_connector_;
  Timer timer_;

  ConnectionError error_code_;
  String error_message_;

  int protocol_version_;
  String keyspace_;
  int event_types_;
  ConnectionListener* listener_;
  Metrics* metrics_;
  ConnectionSettings settings_;
};

} // namespace cass

#endif
