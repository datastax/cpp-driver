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

#ifndef __CASS_DELAYED_CONNECTOR_HPP_INCLUDED__
#define __CASS_DELAYED_CONNECTOR_HPP_INCLUDED__

#include "callback.hpp"
#include "connector.hpp"
#include "ref_counted.hpp"
#include "string.hpp"
#include "vector.hpp"

namespace cass {

class ConnectionPool;
class EventLoop;

/**
 * A connector that starts the connection process after some delay.
 */
class DelayedConnector : public RefCounted<DelayedConnector> {
public:
  typedef SharedRefPtr<DelayedConnector> Ptr;
  typedef Vector<Ptr> Vec;

  typedef cass::Callback<void, DelayedConnector*> Callback;

  /**
   * Constructor
   *
   * @param callback A callback that is called when the connection is connected or
   * if an error occurred.
   */
  DelayedConnector(const Address& address, int protocol_version,
                   const Callback& callback);

  /**
   * Same as Connector::with_keyspace()
   *
   * @param keyspace
   * @return
   */
  DelayedConnector* with_keyspace(const String& keyspace);

  /**
   * Same as Connector::with_metrics()
   *
   * @param metrics
   * @return
   */
  DelayedConnector* with_metrics(Metrics* metrics);

  /**
   * Same as Connector::with_settings()
   *
   * @param settings
   * @return
   */
  DelayedConnector* with_settings(const ConnectionSettings& settings);

  /**
   * Connect to a host after a delay.
   *
   * @param wait_time_ms The amount of time to delay.
   * @param A key to restrict access to the method.
   */
  void delayed_connect(uv_loop_t* loop, uint64_t wait_time_ms);

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
  bool is_canceled() const;
  bool is_ok() const;
  bool is_critical_error() const;
  bool is_keyspace_error() const;

  Connector::ConnectionError error_code() const;
  const String& error_message() const { return connector_->error_message(); }

private:
  void internal_connect(uv_loop_t* loop);

  void on_connect(Connector* connector);
  void on_delayed_connect(Timer* timer);

private:
  Connector::Ptr connector_;

  Callback callback_;

  Timer delayed_connect_timer_;
  bool is_canceled_;
};

} // namespace cass

#endif
