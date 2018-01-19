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

#ifndef __CASS_POOLED_CONNECTION_HPP_INCLUDED__
#define __CASS_POOLED_CONNECTION_HPP_INCLUDED__

#include "atomic.hpp"
#include "connection.hpp"
#include "ref_counted.hpp"
#include "vector.hpp"

#include <uv.h>

namespace cass {

class ConnectionPool;
class EventLoop;
class RequestQueue;
class RequestCallback;

/**
 * A connection wrapper that handles connection pool functionality.
 */
class PooledConnection : public RefCounted<PooledConnection>
                       , public ConnectionListener {
public:
  typedef SharedRefPtr<PooledConnection> Ptr;
  typedef Vector<Ptr> Vec;

  /**
   * Constructor. Don't use directly.
   *
   * @param pool The connection pool of the connection.
   * @param event_loop The event loop that's handling the connection.
   * @param connection The wrapped connection.
   */
  PooledConnection(ConnectionPool* pool,
                   EventLoop* event_loop,
                   const Connection::Ptr& connection);

  /**
   * Queues a request to be written to the wrapped connection (thread-safe).
   *
   * The event loop thread automatically handles flushing the connection.
   *
   * @param callback A request callback that will handle the request.
   * @return Returns true if the request was queued, otherwise the queue is full.
   */
  bool write(RequestCallback* callback);

  /**
   * Closes the wrapped connection (thread-safe)
   */
  void close();

  /**
   * Get the number of outstanding requests including the number of queued and
   * written requests.
   *
   * *Note:* It's possible for this to go negative, but it shouldn't affect the
   * intended purpose.
   *
   * @return The total number of outstanding requests.
   */
  int total_request_count() const;

public:
  EventLoop* event_loop() { return event_loop_; }
  const String& keyspace() const { return connection_->keyspace(); } // Test only

public: // Only to be called on the event loop thread
  class Protected {
    friend class RequestQueue;
    friend class RunClose;
    Protected() { }
    Protected(Protected const&) { }
  };

  /**
   * Write a request to the connection from the event loop thread.
   *
   * @param callback A request callback that will handle the request.
   * @param A key to restrict access to the method.
   * @return The number of bytes written, or negative if an error occurred.
   */
  int32_t write(RequestCallback* callback, Protected);

  /**
   * Flush outstanding requests from the event loop thread.
   *
   * @param A key to restrict access to the method.
   */
  void flush(Protected);

  /**
   * Determine if the connection is closing.
   *
   * @param A key to restrict access to the method.
   * @return Returns true if closing.
   */
  bool is_closing(Protected) const;

  /**
   * Close the connection from the event loop thread.
   *
   * @param A key to restrict access to the method.
   */
  void close(Protected);

private:
  virtual void on_close(Connection* connection);

private:
  const Connection::Ptr connection_;
  RequestQueue* const request_queue_;
  ConnectionPool* const pool_;
  EventLoop* const event_loop_;

  Atomic<int> pending_request_count_;
};

} // namespace cass

#endif
