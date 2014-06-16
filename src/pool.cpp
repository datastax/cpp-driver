#include "pool.hpp"

#include "session.hpp"
#include "timer.hpp"
#include "prepare_handler.hpp"
#include "logger.hpp"
#include "set_keyspace_handler.hpp"

namespace cass {

static bool least_busy_comp(Connection* a, Connection* b) {
  return a->available_streams() < b->available_streams();
}

Pool::PoolHandler::PoolHandler(Pool* pool, Connection* connection,
                               RequestHandler* request_handler)
    : pool_(pool)
    , connection_(connection)
    , request_handler_(request_handler) {
}

void Pool::PoolHandler::on_set(Message* response) {
  switch (response->opcode) {
    case CQL_OPCODE_RESULT:
      on_result_response(response);
      break;
    case CQL_OPCODE_ERROR:
      on_error_response(response);
      break;
    default:
      // TODO(mpenick): Log this
      request_handler_->on_error(CASS_ERROR_LIB_UNEXPECTED_RESPONSE,
                                 "Unexpected response");
      connection_->defunct();
  }
  finish_request();
}

void Pool::PoolHandler::on_error(CassError code, const std::string& message) {
  if (code == CASS_ERROR_LIB_WRITE_ERROR ||
      code == CASS_ERROR_UNABLE_TO_SET_KEYSPACE) {
    request_handler_.release()->retry(RETRY_WITH_NEXT_HOST);
  } else {
    request_handler_->on_error(code, message);
  }
  finish_request();
}

void Pool::PoolHandler::on_timeout() {
  request_handler_->on_timeout();
  finish_request();
}

void Pool::PoolHandler::finish_request() {
  if (connection_->is_ready()) {
    pool_->execute_pending_request(connection_);
  }
}

void Pool::PoolHandler::on_result_response(Message* response) {
  ResultResponse* result = static_cast<ResultResponse*>(response->body.get());
  switch (result->kind) {
    case CASS_RESULT_KIND_SET_KEYSPACE:
      if (pool_->keyspace_callback_) {
        pool_->keyspace_callback_(
            std::string(result->keyspace, result->keyspace_size));
      }
      break;
  }
  request_handler_->on_set(response);
}

void Pool::PoolHandler::on_error_response(Message* response) {
  ErrorResponse* error = static_cast<ErrorResponse*>(response->body.get());
  switch (error->code) {
    case CQL_ERROR_UNPREPARED: {
      RequestHandler* request_handler = request_handler_.release();
      std::unique_ptr<PrepareHandler> prepare_handler(
          new PrepareHandler(request_handler));
      std::string prepared_id(error->prepared_id, error->prepared_id_size);
      if (prepare_handler->init(prepared_id)) {
        connection_->execute(prepare_handler.release());
      } else {
        request_handler->on_error(CASS_ERROR_LIB_UNEXPECTED_RESPONSE,
                                  "Received unprepared error for invalid "
                                  "request type or invalid prepared id");
      }
      break;
    }
    default:
      request_handler_->on_set(response);
      break;
  }
}

Pool::Pool(const Host& host, uv_loop_t* loop, SSLContext* ssl_context,
           Logger* logger, const Config& config)
    : state_(POOL_STATE_NEW)
    , host_(host)
    , loop_(loop)
    , ssl_context_(ssl_context)
    , logger_(logger)
    , config_(config)
    , is_defunct_(false) {
}

Pool::~Pool() {
  for (auto request_handler : pending_request_queue_) {
    if (request_handler->timer) {
      Timer::stop(request_handler->timer);
      request_handler->timer = nullptr;
      request_handler->retry(RETRY_WITH_NEXT_HOST);
    }
  }
  pending_request_queue_.clear();
}

void Pool::connect(const std::string& keyspace) {
  if (state_ == POOL_STATE_NEW) {
    for (size_t i = 0; i < config_.core_connections_per_host(); ++i) {
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
    for (auto connection : connections_) {
      connection->close();
    }
    for (auto connection : connections_pending_) {
      connection->close();
    }
    maybe_close();
  }
}

Connection* Pool::borrow_connection(const std::string& keyspace) {
  if (connections_.empty()) {
    for (size_t i = 0; i < config_.core_connections_per_host(); ++i) {
      spawn_connection(keyspace);
    }
    return nullptr;
  }

  maybe_spawn_connection(keyspace);

  return find_least_busy();
}

bool Pool::execute(Connection* connection, RequestHandler* request_handler) {
  PoolHandler* pool_handler =
      new PoolHandler(this, connection, request_handler);
  if (request_handler->keyspace == connection->keyspace()) {
    return connection->execute(pool_handler);
  } else {
    return connection->execute(new SetKeyspaceHandler(
        request_handler->keyspace, connection, pool_handler));
  }
}

void Pool::defunct() {
  is_defunct_ = true;
  close();
}

void Pool::maybe_notify_ready(Connection* connection) {
  // Currently, this will notify ready even if all the connections fail.
  // This might use some improvement.
  if (state_ == POOL_STATE_CONNECTING && connections_pending_.empty()) {
    state_ = POOL_STATE_READY;
    if (ready_callback_) {
      ready_callback_(this);
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
  if (state_ == POOL_STATE_NEW || state_ == POOL_STATE_READY) {
    Connection* connection = new Connection(
        loop_, ssl_context_ ? ssl_context_->session_new() : nullptr, host_,
        logger_, config_, keyspace);

    connection->set_ready_callback(
        std::bind(&Pool::on_connection_ready, this, std::placeholders::_1));
    connection->set_close_callback(
        std::bind(&Pool::on_connection_closed, this, std::placeholders::_1));
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

  spawn_connection(keyspace);
}

Connection* Pool::find_least_busy() {
  ConnectionCollection::iterator it = std::max_element(
      connections_.begin(), connections_.end(), least_busy_comp);
  if ((*it)->is_ready() && (*it)->available_streams()) {
    return *it;
  }
  return nullptr;
}

void Pool::execute_pending_request(Connection* connection) {
  if (!pending_request_queue_.empty()) {
    RequestHandler* request_handler = pending_request_queue_.front();
    pending_request_queue_.pop_front();
    if (request_handler->timer) {
      Timer::stop(request_handler->timer);
      request_handler->timer = nullptr;
    }
    if (!execute(connection, request_handler)) {
      request_handler->retry(RETRY_WITH_NEXT_HOST);
    }
  }
}

void Pool::on_connection_ready(Connection* connection) {
  connections_pending_.erase(connection);

  maybe_notify_ready(connection);

  connections_.push_back(connection);
  execute_pending_request(connection);
}

void Pool::on_connection_closed(Connection* connection) {
  connections_pending_.erase(connection);

  maybe_notify_ready(connection);

  auto it = std::find(connections_.begin(), connections_.end(), connection);
  if (it != connections_.end()) {
    connections_.erase(it);
  }

  if (connection->is_defunct()) {
    // TODO(mpenick): Conviction policy
    defunct();
  }

  maybe_close();
}

void Pool::on_timeout(Timer* timer) {
  RequestHandler* request_handler = static_cast<RequestHandler*>(timer->data());
  pending_request_queue_.remove(request_handler);
  request_handler->retry(RETRY_WITH_NEXT_HOST);
  maybe_close();
}

bool Pool::wait_for_connection(RequestHandler* request_handler) {
  if (pending_request_queue_.size() + 1 > config_.max_pending_requests()) {
    return false;
  }

  request_handler->timer =
      Timer::start(loop_, config_.connect_timeout(), request_handler,
                   std::bind(&Pool::on_timeout, this, std::placeholders::_1));
  pending_request_queue_.push_back(request_handler);
  return true;
}

} // namespace cass
