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

#include "pool.hpp"

#include "connection.hpp"
#include "error_response.hpp"
#include "io_worker.hpp"
#include "logger.hpp"
#include "query_request.hpp"
#include "session.hpp"
#include "request_handler.hpp"
#include "result_response.hpp"
#include "timer.hpp"

#include <algorithm>

namespace cass {

static bool least_busy_comp(Connection* a, Connection* b) {
  return a->pending_request_count() < b->pending_request_count();
}

class SetKeyspaceCallback : public SimpleRequestCallback {
public:
  SetKeyspaceCallback(const std::string& keyspace,
                      const RequestCallback::Ptr& callback);

private:
  class SetKeyspaceRequest : public QueryRequest {
  public:
      SetKeyspaceRequest(const std::string& keyspace, uint64_t request_timeout_ms)
        : QueryRequest("USE \"" + keyspace + "\"") {
        set_request_timeout_ms(request_timeout_ms);
      }
  };

private:
  virtual void on_internal_set(ResponseMessage* response);
  virtual void on_internal_error(CassError code, const std::string& message);
  virtual void on_internal_timeout();

  void on_result_response(ResponseMessage* response);

private:
  RequestCallback::Ptr callback_;
};

SetKeyspaceCallback::SetKeyspaceCallback(const std::string& keyspace,
                                         const RequestCallback::Ptr& callback)
  : SimpleRequestCallback(
      Request::ConstPtr(
        new SetKeyspaceRequest(keyspace,
                               callback->request_timeout_ms())))
  , callback_(callback) { }

void SetKeyspaceCallback::on_internal_set(ResponseMessage* response) {
  switch (response->opcode()) {
    case CQL_OPCODE_RESULT:
      on_result_response(response);
      break;
    case CQL_OPCODE_ERROR:
      connection()->defunct();
      callback_->on_error(CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE,
                          "Unable to set keyspace");
      break;
    default:
      break;
  }
}

void SetKeyspaceCallback::on_internal_error(CassError code, const std::string& message) {
  connection()->defunct();
  callback_->on_error(CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE,
                      "Unable to set keyspace");
}

void SetKeyspaceCallback::on_internal_timeout() {
  callback_->on_retry_next_host();
}

void SetKeyspaceCallback::on_result_response(ResponseMessage* response) {
  ResultResponse* result =
      static_cast<ResultResponse*>(response->response_body().get());
  if (result->kind() == CASS_RESULT_KIND_SET_KEYSPACE) {
    if (!connection()->write(callback_)) {
      // Try on the same host but a different connection
      callback_->on_retry_current_host();
    }
  } else {
    connection()->defunct();
    callback_->on_error(CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE,
                        "Unable to set keyspace");
  }
}

Pool::Pool(IOWorker* io_worker,
           const Host::ConstPtr& host,
           bool is_initial_connection)
    : io_worker_(io_worker)
    , host_(host)
    , loop_(io_worker->loop())
    , config_(io_worker->config())
    , metrics_(io_worker->metrics())
    , state_(POOL_STATE_NEW)
    , error_code_(Connection::CONNECTION_OK)
    , is_initial_connection_(is_initial_connection)
    , is_pending_flush_(false)
    , is_pending_request_processing_(false)
    , cancel_reconnect_(false) { }

Pool::~Pool() {
  LOG_DEBUG("Pool(%p) dtor with %u pending requests",
            static_cast<void*>(this),
            static_cast<unsigned int>(pending_requests_.size()));
  for (RequestCallback::Vec::iterator it = pending_requests_.begin(),
       end = pending_requests_.end(); it != end; ++it) {
    (*it)->on_retry_next_host();
  }
}

void Pool::connect() {
  if (state_ == POOL_STATE_NEW ||
      state_ == POOL_STATE_WAITING_TO_CONNECT) {
    LOG_DEBUG("Connect pool(%p) for host %s",
              static_cast<void*>(this),
              host_->address_string().c_str());

    connect_timer.stop(); // There could be a delayed connect waiting

    for (unsigned i = 0; i < config_.core_connections_per_host(); ++i) {
      spawn_connection();
    }
    state_ = POOL_STATE_CONNECTING;
    maybe_notify_ready();
  }
}

void Pool::delayed_connect() {
  if (state_ == POOL_STATE_NEW) {
    state_ = POOL_STATE_WAITING_TO_CONNECT;
    connect_timer.start(loop_,
                        config_.reconnect_wait_time_ms(),
                        this, on_wait_to_connect);
  }
}

void Pool::close(bool cancel_reconnect) {
  if (state_ != POOL_STATE_CLOSING && state_ != POOL_STATE_CLOSED) {
    LOG_DEBUG("Closing pool(%p) for host %s",
              static_cast<void*>(this),
              host_->address_string().c_str());

    connect_timer.stop();

    // We're closing before we've connected (likely because of an error), we need
    // to notify we're "ready"
    if (state_ == POOL_STATE_CONNECTING) {
      state_ = POOL_STATE_CLOSING;
      io_worker_->notify_pool_ready(this);
    } else {
      state_ = POOL_STATE_CLOSING;
    }

    cancel_reconnect_ = cancel_reconnect;

    for (ConnectionVec::iterator it = connections_.begin(),
                                 end = connections_.end();
         it != end; ++it) {
      (*it)->close();
    }
    for (ConnectionVec::iterator it = pending_connections_.begin(),
                                 end = pending_connections_.end();
         it != end; ++it) {
      (*it)->close();
    }
  }

  maybe_close();
}

bool Pool::write(const RequestCallback::Ptr& callback) {
  Connection* connection = borrow_connection();
  if (connection != NULL) {
    if (internal_write(connection, callback)) {
      return true;
    }
  } else { // Too busy, or no connections
    wait_for_connection(callback);
    return true; // Waiting for connection
  }
  return false;
}

Connection* Pool::borrow_connection() {
  if (connections_.empty()) {
    for (unsigned i = 0; i < config_.core_connections_per_host(); ++i) {
      maybe_spawn_connection();
    }
    return NULL;
  }

  Connection* connection = find_least_busy();

  if (connection == NULL ||
      connection->pending_request_count() >=
        config_.max_concurrent_requests_threshold()) {
      maybe_spawn_connection();
  }

  return connection;
}

bool Pool::internal_write(Connection* connection, const RequestCallback::Ptr& callback) {
  std::string keyspace(io_worker_->keyspace());
  if (keyspace == connection->keyspace()) {
    if (!connection->write(callback, false)) {
      return false;
    }
  } else {
    LOG_DEBUG("Setting keyspace %s on connection(%p) pool(%p)",
              keyspace.c_str(),
              static_cast<void*>(connection),
              static_cast<void*>(this));
    if (!connection->write(RequestCallback::Ptr(
                             new SetKeyspaceCallback(keyspace,
                                                     callback)),
                           false)) {
      return false;
    }
  }
  if (!is_pending_flush_) {
    io_worker_->add_pending_flush(this);
  }
  is_pending_flush_ = true;
  return true;
}

void Pool::wait_for_connection(const RequestCallback::Ptr& callback) {
  if (callback->state() == RequestCallback::REQUEST_STATE_CANCELLED) {
    return;
  }

  pending_requests_.push_back(callback);

  if (!is_pending_request_processing_) {
    io_worker_->add_pending_request_processing(this);
  }
  is_pending_request_processing_ = true;
}

void Pool::flush() {
  is_pending_flush_ = false;
  for (ConnectionVec::iterator it = connections_.begin(),
       end = connections_.end(); it != end; ++it) {
    (*it)->flush();
  }
}

bool Pool::process_pending_requests() {
  RequestCallback::Vec::iterator it = pending_requests_.begin();
  for (RequestCallback::Vec::iterator end = pending_requests_.end();
       it != end; ++it) {
    const RequestCallback::Ptr& callback(*it);

    // Skip cancelled requests (and remove them)
    if (callback->state() == RequestCallback::REQUEST_STATE_CANCELLED) {
      continue;
    }

    Connection* connection = borrow_connection();

    // Stop processing requests because there are no more streams available.
    if (connection == NULL) break;

    if (!internal_write(connection, callback)) {
      callback->on_retry_next_host();
    }
  }

  LOG_TRACE("Processed (or cancelled) %u pending request(s) on %s pool(%p)",
            static_cast<unsigned int>(it - pending_requests_.begin()),
            host_->address_string().c_str(),
            static_cast<void*>(this));

  pending_requests_.erase(pending_requests_.begin(), it);

  is_pending_request_processing_ = !pending_requests_.empty();

  return is_pending_request_processing_;
}

void Pool::maybe_notify_ready() {
  // This will notify ready even if all the connections fail.
  // it is up to the holder to inspect state
  if (state_ == POOL_STATE_CONNECTING && pending_connections_.empty()) {
    LOG_DEBUG("Pool(%p) connected to host %s",
              static_cast<void*>(this),
              host_->address_string().c_str());
    state_ = POOL_STATE_READY;
    io_worker_->notify_pool_ready(this);
  }
}

void Pool::maybe_close() {
  if (state_ == POOL_STATE_CLOSING && connections_.empty() &&
      pending_connections_.empty()) {

    LOG_DEBUG("Pool(%p) closed connections to host %s",
              static_cast<void*>(this),
              host_->address_string().c_str());
    state_ = POOL_STATE_CLOSED;
    io_worker_->notify_pool_closed(this);
  }
}

void Pool::spawn_connection() {
  if (state_ != POOL_STATE_CLOSING && state_ != POOL_STATE_CLOSED) {
    Connection* connection =
        new Connection(loop_, config_, metrics_,
                       host_,
                       io_worker_->keyspace(),
                       io_worker_->protocol_version(),
                       this);

    LOG_DEBUG("Spawning new connection to host %s for pool(%p)",
              host_->address_string().c_str(),
              static_cast<void*>(this));
    connection->connect();

    pending_connections_.push_back(connection);
  }
}

void Pool::maybe_spawn_connection() {
  if (pending_connections_.size() >= config_.max_concurrent_creation()) {
    return;
  }

  if (connections_.size() + pending_connections_.size() >=
      config_.max_connections_per_host()) {
    return;
  }

  if (state_ != POOL_STATE_READY) {
    return;
  }

  spawn_connection();
}

Connection* Pool::find_least_busy() {
  ConnectionVec::iterator it = std::min_element(
      connections_.begin(), connections_.end(), least_busy_comp);
  if ((*it)->is_ready() && (*it)->available_streams() > 0) {
    return *it;
  }
  return NULL;
}

void Pool::on_ready(Connection* connection) {
  pending_connections_.erase(std::remove(pending_connections_.begin(), pending_connections_.end(), connection),
                             pending_connections_.end());
  connections_.push_back(connection);

  maybe_notify_ready();

  metrics_->total_connections.inc();
}

void Pool::on_close(Connection* connection) {
  pending_connections_.erase(std::remove(pending_connections_.begin(), pending_connections_.end(), connection),
                             pending_connections_.end());

  ConnectionVec::iterator it =
      std::find(connections_.begin(), connections_.end(), connection);
  if (it != connections_.end()) {
    connections_.erase(it);
    metrics_->total_connections.dec();
  }

  // For timeouts, if there are any valid connections left then don't close the
  // entire pool, but attempt to reconnect the timed out connections.
  if (connection->is_timeout_error() && !connections_.empty()) {
    if (!connect_timer.is_running()) {
      connect_timer.start(loop_,
                          config_.reconnect_wait_time_ms(),
                          this, on_partial_reconnect);
    }
    maybe_notify_ready();
  } else if (connection->is_defunct()) {
    // Log only the first connection failure as an error or warning.
    if (state_ != POOL_STATE_CLOSING) {
      // Log as an error if the connection pool was either estabilished or
      // it's the first attempt, otherwise log as a warning.
      if (state_ == POOL_STATE_READY) {
        LOG_ERROR("Closing established connection pool to host %s because of the following error: %s",
                 host_->address_string().c_str(),
                 connection->error_message().c_str());
      } else if (is_initial_connection_) {
        LOG_ERROR("Connection pool was unable to connect to host %s because of the following error: %s",
                  host_->address_string().c_str(),
                  connection->error_message().c_str());
      } else {
        LOG_WARN("Connection pool was unable to reconnect to host %s because of the following error: %s",
                 host_->address_string().c_str(),
                 connection->error_message().c_str());
      }
    }

    // If at least one connection has a critical failure then don't try to
    // reconnect automatically. Also, don't set the error to something else if
    // it has already been set to something critical.
    if (!is_critical_failure()) {
      error_code_ = connection->error_code();
    }

    close();
  } else {
    maybe_notify_ready();
    maybe_close();
  }
}

void Pool::on_partial_reconnect(Timer* timer) {
  Pool* pool = static_cast<Pool*>(timer->data());

  size_t current = pool->connections_.size() +
                   pool->pending_connections_.size();

  size_t want = pool->config_.core_connections_per_host();

  if (current < want) {
    size_t need = want - current;
    for (size_t i = 0; i < need; ++i) {
      pool->spawn_connection();
    }
  }
}

void Pool::on_wait_to_connect(Timer* timer) {
  Pool* pool = static_cast<Pool*>(timer->data());
  pool->connect();
}

} // namespace cass
