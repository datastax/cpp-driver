/*
  Copyright (c) 2014 DataStax

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
#include "prepare_handler.hpp"
#include "session.hpp"
#include "set_keyspace_handler.hpp"
#include "request_handler.hpp"
#include "result_response.hpp"
#include "timer.hpp"

#include "third_party/boost/boost/bind.hpp"

namespace cass {

static bool least_busy_comp(Connection* a, Connection* b) {
  return a->pending_request_count() < b->pending_request_count();
}

Pool::Pool(IOWorker* io_worker, const Address& address,
           bool is_initial_connection)
    : io_worker_(io_worker)
    , address_(address)
    , loop_(io_worker->loop())
    , logger_(io_worker->logger())
    , config_(io_worker->config())
    , state_(POOL_STATE_NEW)
    , is_initial_connection_(is_initial_connection)
    , is_defunct_(false)
    , is_critical_failure_(false) {}

Pool::~Pool() {
  logger_->debug("Pool: dtor with %u pending requests pool(%p)", pending_requests_.size(), this);
  while (!pending_requests_.is_empty()) {
    RequestHandler* request_handler
        = static_cast<RequestHandler*>(pending_requests_.front());
    pending_requests_.remove(request_handler);
    request_handler->stop_timer();
    request_handler->retry(RETRY_WITH_NEXT_HOST);
  }
}

void Pool::connect() {
  logger_->debug("Pool: connect %s pool(%p)", address_.to_string().c_str(), this);
  if (state_ == POOL_STATE_NEW) {
    for (unsigned i = 0; i < config_.core_connections_per_host(); ++i) {
      spawn_connection();
    }
    state_ = POOL_STATE_CONNECTING;
    maybe_notify_ready();
  }
}

void Pool::close() {
  if (state_ != POOL_STATE_CLOSING && state_ != POOL_STATE_CLOSED) {
    logger_->debug("Pool: closing pool(%p)", this);
    // We're closing before we've connected (likely beause of an error), we need
    // to notify we're "ready"
    if (state_ == POOL_STATE_CONNECTING) {
      state_ = POOL_STATE_CLOSING;
      io_worker_->notify_pool_ready(this);
    } else {
      state_ = POOL_STATE_CLOSING;
    }

    for (ConnectionVec::iterator it = connections_.begin(),
                                 end = connections_.end();
         it != end; ++it) {
      (*it)->close();
    }
    for (ConnectionSet::iterator it = connections_pending_.begin(),
                                 end = connections_pending_.end();
         it != end; ++it) {
      (*it)->close();
    }
    maybe_close();
  }
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
        config_.max_simultaneous_requests_threshold()) {
      maybe_spawn_connection();
  }

  return connection;
}

void Pool::return_connection(Connection* connection) {
  if (connection->is_ready() && !pending_requests_.is_empty()) {
    RequestHandler* request_handler
        = static_cast<RequestHandler*>(pending_requests_.front());
    pending_requests_.remove(request_handler);
    request_handler->stop_timer();
    if (!execute(connection, request_handler)) {
      request_handler->retry(RETRY_WITH_NEXT_HOST);
    }
  }
}

bool Pool::execute(Connection* connection, RequestHandler* request_handler) {
  request_handler->set_connection_and_pool(connection, this);
  if (io_worker_->is_current_keyspace(connection->keyspace())) {
    return connection->execute(request_handler);
  } else {
    logger_->debug("Pool: setting keyspace %s on connection(%p) pool(%p)", io_worker_->keyspace().c_str(), connection, this);
    return connection->execute(new SetKeyspaceHandler(
        connection, io_worker_->keyspace(), request_handler));
  }
}

void Pool::defunct() {
  is_defunct_ = true;
  close();
}

void Pool::maybe_notify_ready() {
  // This will notify ready even if all the connections fail.
  // it is up to the holder to inspect state
  if (state_ == POOL_STATE_CONNECTING && connections_pending_.empty()) {
    state_ = POOL_STATE_READY;
    io_worker_->notify_pool_ready(this);
  }
}

void Pool::maybe_close() {
  if (state_ == POOL_STATE_CLOSING && connections_.empty() &&
      connections_pending_.empty()) {
    state_ = POOL_STATE_CLOSED;
    io_worker_->notify_pool_closed(this);
  }
}

void Pool::spawn_connection() {
  if (state_ != POOL_STATE_CLOSING && state_ != POOL_STATE_CLOSED) {
    Connection* connection =
        new Connection(loop_, logger_, config_,
                       address_,
                       io_worker_->keyspace(),
                       io_worker_->protocol_version());

    logger_->info("Pool: Spawning new conneciton to host %s", address_.to_string(true).c_str());
    connection->set_ready_callback(
          boost::bind(&Pool::on_connection_ready, this, _1));
    connection->set_close_callback(
          boost::bind(&Pool::on_connection_closed, this, _1));
    connection->connect();

    connections_pending_.insert(connection);
  }
}

void Pool::maybe_spawn_connection() {
  if (connections_pending_.size() >= config_.max_simultaneous_creation()) {
    return;
  }

  if (connections_.size() + connections_pending_.size() >=
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

void Pool::on_connection_ready(Connection* connection) {
  connections_pending_.erase(connection);
  maybe_notify_ready();

  connections_.push_back(connection);
  return_connection(connection);
}

void Pool::on_connection_closed(Connection* connection) {
  connections_pending_.erase(connection);

  ConnectionVec::iterator it =
      std::find(connections_.begin(), connections_.end(), connection);
  if (it != connections_.end()) {
    connections_.erase(it);
  }

  if (connection->is_defunct()) {
    // If at least one connection has a critical failure then don't try to
    // reconnect automatically.
    if (connection->is_critical_failure()) {
      is_critical_failure_ = true;
    }
    defunct();
  }

  maybe_notify_ready();
  maybe_close();
}

void Pool::on_pending_request_timeout(RequestTimer* timer) {
  RequestHandler* request_handler = static_cast<RequestHandler*>(timer->data());
  pending_requests_.remove(request_handler);
  request_handler->retry(RETRY_WITH_NEXT_HOST);
  logger_->debug("Pool: timeout waiting for connection to %s pool(%p)",
                 address_.to_string().c_str(),
                 this);
  maybe_close();
}

bool Pool::wait_for_connection(RequestHandler* request_handler) {
  if (pending_requests_.size() + 1 > config_.max_pending_requests()) {
    logger_->warn("Exceeded the max pending requests setting of %u on host %s pool(%p)",
                  config_.max_pending_requests(),
                  address_.to_string().c_str(),
                  this);
    return false;
  }

  if (pending_requests_.size() % 10 == 0) {
    logger_->debug("Pool: %u request%s pending on %s pool(%p)",
                   pending_requests_.size()+1,
                   pending_requests_.size() > 0 ? "s":"",
                   address_.to_string().c_str(),
                   this);
  }

  request_handler->start_timer(loop_, config_.connect_timeout(), request_handler,
                               boost::bind(&Pool::on_pending_request_timeout, this, _1));
  pending_requests_.add_to_back(request_handler);
  return true;
}

} // namespace cass
