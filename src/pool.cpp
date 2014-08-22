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

Pool::Pool(const Host& host, uv_loop_t* loop,
           Logger* logger, const Config& config)
    : state_(POOL_STATE_NEW)
    , host_(host)
    , loop_(loop)
    , logger_(logger)
    , config_(config)
    , protocol_version_(config.protocol_version())
    , is_defunct_(false) {}

Pool::~Pool() {
  for (RequestHandlerList::iterator it = pending_request_queue_.begin(),
                                    end = pending_request_queue_.end();
       it != end; ++it) {
    RequestHandler* request_handler = *it;
    request_handler->stop_timer();
    request_handler->retry(RETRY_WITH_NEXT_HOST);
  }
  pending_request_queue_.clear();
}

void Pool::connect(const std::string& keyspace) {
  if (state_ == POOL_STATE_NEW) {
    for (unsigned i = 0; i < config_.core_connections_per_host(); ++i) {
      spawn_connection(keyspace);
    }
    state_ = POOL_STATE_CONNECTING;
  }
}

void Pool::close() {
  if (state_ != POOL_STATE_CLOSING && state_ != POOL_STATE_CLOSED) {
    // We're closing before we've connected (likely beause of an error), we need
    // to notify we're "ready"
    if (state_ == POOL_STATE_CONNECTING) {
      if (ready_callback_) {
        ready_callback_(this);
      }
    }
    state_ = POOL_STATE_CLOSING;
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

Connection* Pool::borrow_connection(const std::string& keyspace) {
  if (connections_.empty()) {
    for (unsigned i = 0; i < config_.core_connections_per_host(); ++i) {
      maybe_spawn_connection(keyspace);
    }
    return NULL;
  }

  Connection* connection = find_least_busy();

  if (connection == NULL ||
      connection->pending_request_count() >=
        config_.max_simultaneous_requests_threshold()) {
      maybe_spawn_connection(keyspace);
  }

  return connection;
}

bool Pool::execute(Connection* connection, RequestHandler* request_handler) {
  request_handler->set_connection_and_pool(connection, this);
  if (request_handler->keyspace == connection->keyspace()) {
    return connection->execute(request_handler);
  } else {
    return connection->execute(new SetKeyspaceHandler(
        connection, request_handler->keyspace, request_handler));
  }
}

void Pool::defunct() {
  is_defunct_ = true;
  close();
}

void Pool::maybe_notify_ready(Connection* connection) {
  // Currently, this will notify ready even if all the connections fail.
  // This might use some improvement.

  // We won't notify until we've tried all valid protocol versions
  if (!connection->is_invalid_protocol() ||
      connection->protocol_version() <= 1) {
    if (state_ == POOL_STATE_CONNECTING && connections_pending_.empty()) {
      state_ = POOL_STATE_READY;
      if (ready_callback_) {
        ready_callback_(this);
      }
    }
  }
}

void Pool::maybe_close() {
  if (state_ == POOL_STATE_CLOSING && connections_.empty() &&
      connections_pending_.empty()) {
    state_ = POOL_STATE_CLOSED;
    if (closed_callback_) {
      closed_callback_(this);
    }
  }
}

void Pool::spawn_connection(const std::string& keyspace) {
  if (state_ != POOL_STATE_CLOSING && state_ != POOL_STATE_CLOSED) {
    Connection* connection =
        new Connection(loop_, host_, logger_, config_,
                       keyspace, protocol_version_);

    logger_->info("Spawning new conneciton to %s", host_.address.to_string().c_str());
    connection->set_ready_callback(
          boost::bind(&Pool::on_connection_ready, this, _1));
    connection->set_close_callback(
          boost::bind(&Pool::on_connection_closed, this, _1));
    connection->connect();

    connections_pending_.insert(connection);
  }
}

void Pool::maybe_spawn_connection(const std::string& keyspace) {
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

  spawn_connection(keyspace);
}

Connection* Pool::find_least_busy() {
  ConnectionVec::iterator it = std::min_element(
      connections_.begin(), connections_.end(), least_busy_comp);
  if ((*it)->is_ready() && (*it)->available_streams() > 0) {
    return *it;
  }
  return NULL;
}

void Pool::return_connection(Connection* connection) {
  if (connection->is_ready() && !pending_request_queue_.empty()) {
    RequestHandler* request_handler = pending_request_queue_.front();
    pending_request_queue_.pop_front();
    request_handler->stop_timer();
    if (!execute(connection, request_handler)) {
      request_handler->retry(RETRY_WITH_NEXT_HOST);
    }
  }
}

void Pool::on_connection_ready(Connection* connection) {
  connections_pending_.erase(connection);
  maybe_notify_ready(connection);

  connections_.push_back(connection);
  return_connection(connection);
}

void Pool::on_connection_closed(Connection* connection) {
  connections_pending_.erase(connection);
  maybe_notify_ready(connection);

  ConnectionVec::iterator it =
      std::find(connections_.begin(), connections_.end(), connection);
  if (it != connections_.end()) {
    connections_.erase(it);
  }

  if (connection->is_invalid_protocol() &&
      connection->protocol_version() > 1) {
    protocol_version_ = connection->protocol_version() - 1;
    spawn_connection(connection->keyspace());
  } else if (connection->is_defunct()) {
    // TODO(mpenick): Conviction policy
    defunct();
  }

  maybe_close();
}

void Pool::on_timeout(void* data) {
  RequestHandler* request_handler = static_cast<RequestHandler*>(data);
  pending_request_queue_.remove(request_handler);
  request_handler->retry(RETRY_WITH_NEXT_HOST);
  maybe_close();
}

bool Pool::wait_for_connection(RequestHandler* request_handler) {
  if (pending_request_queue_.size() + 1 > config_.max_pending_requests()) {
    logger_->warn("Exceeded the max pending requests setting of %u on '%s'",
                  config_.max_pending_requests(),
                  host_.address.to_string().c_str());
    return false;
  }

  request_handler->start_timer(loop_, config_.connect_timeout(), request_handler,
                               boost::bind(&Pool::on_timeout, this, _1));
  pending_request_queue_.push_back(request_handler);
  return true;
}

} // namespace cass
