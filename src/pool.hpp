/*
  Copyright 2014 DataStax

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

#ifndef __CASS_POOL_HPP_INCLUDED__
#define __CASS_POOL_HPP_INCLUDED__

#include <list>
#include <string>
#include <algorithm>
#include <functional>

#include "connection.hpp"
#include "session.hpp"
#include "timer.hpp"
#include "prepare_handler.hpp"
#include "logger.hpp"
#include "set_keyspace_handler.hpp"

namespace cass {

class Pool {
  typedef std::set<Connection*> ConnectionSet;
  typedef std::vector<Connection*> ConnectionCollection;
  typedef std::function<void(Host host)> ConnectCallback;
  typedef std::function<void(Host host)> CloseCallback;

  Host host_;
  uv_loop_t* loop_;
  SSLContext* ssl_context_;
  Logger* logger_;
  const Config& config_;
  ConnectionCollection connections_;
  ConnectionSet connections_pending_;
  std::list<RequestHandler*> pending_request_queue_;
  bool is_closing_;
  ConnectCallback connect_callback_;
  CloseCallback close_callback_;
  RetryCallback retry_callback_;
  SetKeyspaceCallback set_keyspace_callback_;


 public:
  class PoolHandler : public ResponseCallback {
    public:
      PoolHandler(Pool* pool,
                  Connection* connection,
                  RequestHandler* request_handler)
        : pool_(pool)
        , connection_(connection)
        , request_handler_(request_handler) { }

      virtual Message* request() const {
        return request_handler_->request();
      }

      virtual void on_set(Message* response) {
        switch(response->opcode) {
          case CQL_OPCODE_RESULT:
            request_handler_->on_set(response);
            break;
          case CQL_OPCODE_ERROR:
            on_error_response(response);
            break;
          default:
            // TODO(mpenick): Log this
            request_handler_->on_error(CASS_ERROR_LIB_UNEXPECTED_RESPONSE, "Unexpected response");
            connection_->defunct();
        }
        finish_request();
      }

      virtual void on_error(CassError code, const std::string& message) {
        if(code == CASS_ERROR_LIB_WRITE_ERROR || code == CASS_ERROR_UNABLE_TO_SET_KEYSPACE) {
          pool_->retry_callback_(request_handler_.release(), RETRY_WITH_NEXT_HOST);
        } else {
          request_handler_->on_error(code, message);
        }
        finish_request();
      }

      virtual void on_timeout() {
        request_handler_->on_timeout();
        finish_request();
      }

    private:
      void finish_request() {
        if(connection_->is_ready()) {
          pool_->execute_pending_request(connection_);
        }
      }

      void on_error_response(Message* response) {
        ErrorResponse* error = static_cast<ErrorResponse*>(response->body.get());
        switch(error->code) {
          case CQL_ERROR_UNPREPARED: {
            RequestHandler* request_handler = request_handler_.release();
            std::unique_ptr<PrepareHandler> prepare_handler(
                  new PrepareHandler(pool_->retry_callback_, request_handler));
            std::string prepared_id(error->prepared_id, error->prepared_id_size);
            if(prepare_handler->init(prepared_id)) {
              connection_->execute(prepare_handler.release());
            } else {
              request_handler->on_error(CASS_ERROR_LIB_UNEXPECTED_RESPONSE,
                                         "Received unprepared error for invalid request type or invalid prepared id");
            }
            break;
          }
          default:
            request_handler_->on_set(response);
            break;
        }
      }

      Pool* pool_;
      Connection* connection_;
      std::unique_ptr<RequestHandler> request_handler_;
  };

  Pool(const Host& host,
       uv_loop_t* loop,
       SSLContext* ssl_context,
       Logger* logger,
       const Config& config,
       const std::string& keyspace,
       ConnectCallback connect_callback,
       CloseCallback close_callback,
       RetryCallback retry_callback,
       SetKeyspaceCallback set_keyspace_callback)
    : host_(host)
    , loop_(loop)
    , ssl_context_(ssl_context)
    , logger_(logger)
    , config_(config)
    , is_closing_(false)
    , connect_callback_(connect_callback)
    , close_callback_(close_callback)
    , retry_callback_(retry_callback)
    , set_keyspace_callback_(set_keyspace_callback) {
    for (size_t i = 0; i < config.core_connections_per_host(); ++i) {
      spawn_connection(keyspace);
    }
  }

  ~Pool() {
    for(auto request_handler : pending_request_queue_) {
      if(request_handler->timer) {
        Timer::stop(request_handler->timer);
        request_handler->timer = nullptr;
        retry_callback_(request_handler, RETRY_WITH_NEXT_HOST);
      }
    }
    pending_request_queue_.clear();
  }

  void on_connection_connect(Connection* connection) {
    auto it = connections_pending_.find(connection);
    if(it != connections_pending_.end()) {
      connect_callback_(host_);

      connections_pending_.erase(it);

      if(is_closing_) {
        connection->close();
      } else if(connection->is_ready()) {
        connections_.push_back(connection);
        execute_pending_request(connection);
      }
    }
  }

  void on_connection_close(Connection* connection) {
    auto it = std::find(connections_.begin(), connections_.end(), connection);
    if(it != connections_.end()) {
      connections_.erase(it);
    }
    connections_pending_.erase(connection);

    if(connection->is_defunct()) {
      is_closing_ = true; // TODO(mpenick): Conviction policy
    }

    maybe_close();
  }

  void maybe_close() {
    if(is_closing_) {
      for(auto c : connections_) {
        if(!c->is_closing()) {
          c->close();
        }
      }
      if(connections_.empty() && connections_pending_.empty()) {
        close_callback_(host_);
      }
    }
  }

  void close() {
    is_closing_ = true;
    for (auto c : connections_) {
      c->close();
    }
    maybe_close();
  }

  void spawn_connection(const std::string& keyspace) {
    if(is_closing_) {
      return;
    }

    Connection* connection
        = new Connection(loop_,
                         ssl_context_ ? ssl_context_->session_new() : nullptr,
                         host_,
                         logger_,
                         config_,
                         keyspace,
                         std::bind(&Pool::on_connection_connect, this, std::placeholders::_1),
                         std::bind(&Pool::on_connection_close, this, std::placeholders::_1),
                         set_keyspace_callback_);

    connection->connect();
    connections_pending_.insert(connection);
  }

  void maybe_spawn_connection(const std::string& keyspace) {
    if (connections_pending_.size() >= config_.max_simultaneous_creation()) {
      return;
    }

    if (connections_.size() + connections_pending_.size()
        >= config_.max_connections_per_host()) {
      return;
    }

    spawn_connection(keyspace);
  }

  static bool least_busy_comp(
      Connection* a,
      Connection* b) {
    return a->available_streams() < b->available_streams();
  }

  Connection* find_least_busy() {
    ConnectionCollection::iterator it =
        std::max_element(
          connections_.begin(),
          connections_.end(),
          Pool::least_busy_comp);
    if ((*it)->is_ready() && (*it)->available_streams()) {
      return *it;
    }
    return nullptr;
  }

  Connection* borrow_connection(const std::string& keyspace) {
    if(is_closing_) {
      return nullptr;
    }

    if(connections_.empty()) {
      for (size_t i = 0; i < config_.core_connections_per_host(); ++i) {
        spawn_connection(keyspace);
      }
      return nullptr;
    }

    maybe_spawn_connection(keyspace);

    return find_least_busy();
  }

  bool execute(Connection* connection, RequestHandler* request_handler) {
    PoolHandler* pool_handler = new PoolHandler(this, connection, request_handler);
    std::shared_ptr<std::string> keyspace = request_handler->keyspace;
    if(!keyspace || *keyspace == connection->keyspace()) {
      return connection->execute(pool_handler);
    } else {
      return connection->execute(new SetKeyspaceHandler(*keyspace, connection, retry_callback_, pool_handler));
    }
  }

  void on_timeout(Timer* timer) {
    RequestHandler* request_handler = static_cast<RequestHandler*>(timer->data());
    pending_request_queue_.remove(request_handler);
    retry_callback_(request_handler, RETRY_WITH_NEXT_HOST);
    maybe_close();
  }

  bool wait_for_connection(RequestHandler* request_handler) {
    if(is_closing_ || pending_request_queue_.size() + 1 > config_.max_pending_requests()) {
      return false;
    }
    request_handler->timer = Timer::start(loop_, config_.connect_timeout(),
                                request_handler,
                                std::bind(&Pool::on_timeout, this, std::placeholders::_1));
    pending_request_queue_.push_back(request_handler);
    return true;
  }

  void execute_pending_request(Connection* connection) {
    if(!pending_request_queue_.empty()) {
      RequestHandler* request_handler = pending_request_queue_.front();
      pending_request_queue_.pop_front();
      if(request_handler->timer) {
        Timer::stop(request_handler->timer);
        request_handler->timer = nullptr;
      }
      if(!execute(connection, request_handler)) {
        retry_callback_(request_handler, RETRY_WITH_NEXT_HOST);
      }
    }
  }
};

} // namespace cass

#endif
