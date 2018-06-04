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

#ifndef __CASS_POOLED_CONNECTOR_HPP_INCLUDED__
#define __CASS_POOLED_CONNECTOR_HPP_INCLUDED__

#include "connector.hpp"
#include "pooled_connection.hpp"
#include "ref_counted.hpp"
#include "string.hpp"
#include "vector.hpp"

namespace cass {

class ConnectionPool;
class EventLoop;

/**
 * A connector for a pooled connection. This handles the connection process for
 * a pooled connection.
 */
class PooledConnector : public RefCounted<PooledConnector> {
public:
  typedef SharedRefPtr<PooledConnector> Ptr;
  typedef Vector<Ptr> Vec;

  typedef void (*Callback)(PooledConnector*);

  /**
   * Constructor
   *
   * @param pool The pool for this connection.
   * @param data User data that's passed to the callback.
   * @param callback A callback that is called when the connection is connected or
   * if an error occurred.
   */
  PooledConnector(ConnectionPool* pool,
                  void* data,
                  Callback callback);

  /**
   * Connect a pooled connection.
   */
  void connect();

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
  PooledConnection::Ptr release_connection();

public:
  void* data() { return data_; }

  bool is_canceled() const;
  bool is_ok() const;
  bool is_critical_error() const;
  bool is_keyspace_error() const;

  Connector::ConnectionError error_code() const { return connector_->error_code(); }
  const String& error_message() const { return connector_->error_message(); }

public: // Only to be called on the event loop thread
  class Protected {
    friend class ConnectionPool;
    Protected() { }
    Protected(Protected const&) { }
  };

  /**
   * Connect the pooled connection after a delay from the event loop thread.
   *
   * @param wait_time_ms The amount of time to delay.
   * @param A key to restrict access to the method.
   */
  void delayed_connect(uint64_t wait_time_ms, Protected);

private:
  void internal_connect();

  static void on_connect(Connector* connector);
  void handle_connect(Connector* connector);

  static void on_delayed_connect(Timer* timer);
  void handle_delayed_connect(Timer* timer);

private:
  ConnectionPool* pool_;
  PooledConnection::Ptr connection_;
  Connector::Ptr connector_;

  void* data_;
  Callback callback_;

  Timer delayed_connect_timer_;
  bool is_canceled_;
};

} // namespace cass

#endif
