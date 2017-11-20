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

#include "io_worker.hpp"

#include "config.hpp"
#include "logger.hpp"
#include "pool.hpp"
#include "request_handler.hpp"
#include "session.hpp"
#include "scoped_lock.hpp"
#include "timer.hpp"

namespace cass {

/**
 * A handler that tracks the progress of prepares on all hosts and returns the
 * initial "PREPARED" result response when the last prepare is finished.
 */
class PrepareAllHandler : public RefCounted<PrepareAllHandler> {
public:
  typedef SharedRefPtr<PrepareAllHandler> Ptr;

  PrepareAllHandler(const Host::Ptr& current_host,
                    const Response::Ptr& response,
                    const RequestHandler::Ptr& request_handler,
                    int remaining)
    : current_host_(current_host)
    , response_(response)
    , request_handler_(request_handler)
    , remaining_(remaining) {
    assert(remaining > 0);
  }

  const RequestWrapper& wrapper() const { return request_handler_->wrapper(); }

  uv_loop_t* loop() { return request_handler_->io_worker()->loop(); }

  void finish() {
    if (--remaining_ <= 0) { // The last request sets the response on the future.
      request_handler_->set_response(current_host_, response_);
    }
  }

private:
  Host::Ptr current_host_;
  Response::Ptr response_;
  RequestHandler::Ptr request_handler_;
  int remaining_;
};

/**
 * A callback for preparing a statement. It's used in conjunction with
 * `PrepareAllHandler` to prepare a statement on all hosts. It calls finish
 *  on the handler when the request is done (success, error, or timeout).
 */
class PrepareAllCallback : public RequestCallback {
public:
  typedef SharedRefPtr<PrepareAllCallback> Ptr;

  PrepareAllCallback(const Address& address, const PrepareAllHandler::Ptr& handler)
    : RequestCallback(handler->wrapper())
    , address_(address)
    , handler_(handler)
    , is_finished_(false) { }

  ~PrepareAllCallback() {
    finish();
  }

private:
  void finish() {
    // Only finish the callback one time. This can happen if the request times out.
    if (!is_finished_) {
      timer_.stop();
      handler_->finish();
      is_finished_ = true;
    }
  }

  virtual void on_retry_current_host() { }
  virtual void on_retry_next_host() { }

  virtual void on_cancel() { }

  virtual void on_set(ResponseMessage* response) {
    if (timer_.is_running()) { // The request hasn't timed out
      LOG_DEBUG("Successfully prepared all on host %s",
                address_.to_string().c_str());
    }
  }

  virtual void on_error(CassError code, const std::string& message) {
    if (timer_.is_running()) { // The request hasn't timed out
      LOG_WARN("Failed to prepare all on host %s with error: '%s'",
               address_.to_string().c_str(),
               message.c_str());
    }
  }

  virtual void on_start() {
    int request_timeout_ms = this->request_timeout_ms();
    if (request_timeout_ms > 0) { // 0 means no timeout
      timer_.start(handler_->loop(),
                   request_timeout_ms,
                   this, on_timeout);
    }
  }

  static void on_timeout(Timer* timer) {
    PrepareAllCallback* callback = static_cast<PrepareAllCallback*>(timer->data());
    LOG_WARN("Prepare all timed out on host %s",
             callback->address_.to_string().c_str());
    callback->finish(); // Don't wait for the request to come back
  }

private:
  Address address_;
  PrepareAllHandler::Ptr handler_;
  bool is_finished_;
  Timer timer_;
};

IOWorker::IOWorker(Session* session)
    : state_(IO_WORKER_STATE_READY)
    , session_(session)
    , config_(session->config())
    , metrics_(session->metrics())
    , protocol_version_(-1)
    , pending_request_count_(0)
    , request_queue_(config_.queue_size_io()) {
  pools_.set_empty_key(Address::EMPTY_KEY);
  pools_.set_deleted_key(Address::DELETED_KEY);
  check_.data = this;
  prepare_.data = this;
  uv_mutex_init(&keyspace_mutex_);
}

IOWorker::~IOWorker() {
  uv_mutex_destroy(&keyspace_mutex_);
}

int IOWorker::init() {
  int rc = EventThread<IOWorkerEvent>::init(config_.queue_size_event());
  if (rc != 0) return rc;
  rc = request_queue_.init(loop(), this, &IOWorker::on_execute);
  if (rc != 0) return rc;
  rc = uv_check_init(loop(), &check_);
  if (rc != 0) return rc;
  rc = uv_check_start(&check_, on_check);
  if (rc != 0) return rc;
  rc = uv_prepare_init(loop(), &prepare_);
  if (rc != 0) return rc;
  rc = uv_prepare_start(&prepare_, on_prepare);
  if (rc != 0) return rc;
  return rc;
}

std::string IOWorker::keyspace() const {
  ScopedMutex l(&keyspace_mutex_);
  return keyspace_;
}

void IOWorker::set_keyspace(const std::string& keyspace) {
  ScopedMutex l(&keyspace_mutex_);
  keyspace_ = keyspace;
}

void IOWorker::broadcast_keyspace_change(const std::string& keyspace) {
  set_keyspace(keyspace);
  session_->broadcast_keyspace_change(keyspace, this);
}

bool IOWorker::is_host_up(const Address& address) const {
  PoolMap::const_iterator it = pools_.find(address);
  return it != pools_.end() && it->second->is_ready();
}

bool IOWorker::add_pool_async(const Host::ConstPtr& host, bool is_initial_connection) {
  IOWorkerEvent event;
  event.type = IOWorkerEvent::ADD_POOL;
  event.host = host;
  event.is_initial_connection = is_initial_connection;
  return send_event_async(event);
}

bool IOWorker::remove_pool_async(const Host::ConstPtr& host, bool cancel_reconnect) {
  IOWorkerEvent event;
  event.type = IOWorkerEvent::REMOVE_POOL;
  event.host = host;
  event.cancel_reconnect = cancel_reconnect;
  return send_event_async(event);
}

void IOWorker::close_async() {
  while (!request_queue_.enqueue(NULL)) {
    // Keep trying
  }
}

void IOWorker::add_pool(const Host::ConstPtr& host, bool is_initial_connection) {
  if (!is_ready()) return;

  const Address& address = host->address();

  PoolMap::iterator it = pools_.find(address);
  if (it == pools_.end()) {
    LOG_DEBUG("Adding pool for host %s io_worker(%p)",
              host->address_string().c_str(),
              static_cast<void*>(this));

    Pool::Ptr pool(new Pool(this, host, is_initial_connection));
    pools_[address] = pool;
    pool->connect();
  } else  {
    // We could have a connection that's waiting to reconnect. In that case,
    // this will start to connect immediately.
    LOG_DEBUG("Host %s already present attempting to initiate immediate connection for io_worker(%p)",
              host->address_string().c_str(),
              static_cast<void*>(this));
    it->second->connect();
  }
}

bool IOWorker::execute(const RequestHandler::Ptr& request_handler) {
  request_handler->inc_ref(); // Queue reference
  if (!request_queue_.enqueue(request_handler.get())) {
    request_handler->dec_ref();
    return false;
  }
  return true;
}

bool IOWorker::prepare_all(const Host::Ptr& current_host,
                           const Response::Ptr& response,
                           const RequestHandler::Ptr& request_handler) {
  assert(request_handler->request()->opcode() == CQL_OPCODE_PREPARE);

  if (!config_.prepare_on_all_hosts() ||
      // If there's only 1 node (or 0 nodes) to prepare then we're done.
      pools_.size() < 2) {
    return false; // Either not enabled or not enough hosts
  }

  PrepareAllHandler::Ptr prepare_all_handler(
        new PrepareAllHandler(current_host,
                              response,
                              request_handler,
                              // Subtract the node that's already been prepared
                              pools_.size() - 1));

  for (PoolMap::iterator it = pools_.begin(),
       end = pools_.end(); it != end; ++it)  {
    // Skip over the node we've already prepared
    if (current_host->address() == it->first) {
      continue;
    }

    const Pool::Ptr& pool = it->second;
    // The destructor of `PrepareAllCallback` will decrement the remaining
    // count in `PrepareAllHandler` even if this is unable to write to a
    // connection successfully.
    PrepareAllCallback::Ptr prepare_all_callback(
          new PrepareAllCallback(pool->host()->address(),
                                 prepare_all_handler));

    if (pool->is_ready()) {
      pool->write(prepare_all_callback);
    }
  }

  return true;
}

void IOWorker::retry(const RequestExecution::Ptr& request_execution) {
  while (request_execution->current_host()) {
    PoolMap::const_iterator it = pools_.find(request_execution->current_host()->address());
    if (it != pools_.end() &&
        it->second->is_ready() &&
        it->second->write(request_execution)) {
        return; // Successfully written or pending
    }
    request_execution->next_host();
  }

  request_execution->on_error(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
                              "All hosts in current policy attempted "
                              "and were either unavailable or failed");
}

void IOWorker::request_finished() {
  pending_request_count_--;
  maybe_close();
  request_queue_.send();
}

void IOWorker::notify_pool_ready(Pool* pool) {
  if (pool->is_initial_connection()) {
    if (pool->is_keyspace_error()) {
      session_->notify_keyspace_error_async();
    } else {
      session_->notify_ready_async();
    }
  } else if (is_ready() && pool->is_ready()){
    session_->notify_up_async(pool->host()->address());
  }
}

void IOWorker::notify_pool_closed(Pool* pool) {
  Host::ConstPtr host = pool->host();
  bool is_critical_failure = pool->is_critical_failure();
  bool cancel_reconnect = pool->cancel_reconnect();

  LOG_DEBUG("Pool for host %s closed: pool(%p) io_worker(%p)",
            host->address_string().c_str(),
            static_cast<void*>(pool),
            static_cast<void*>(this));

  // All non-shared pointers to this pool are invalid after this call
  // and it must be done before maybe_notify_closed().
  pools_.erase(host->address());

  if (is_closing()) {
    maybe_notify_closed();
  } else {
    session_->notify_down_async(host->address());
    if (!is_critical_failure && !cancel_reconnect) {
      schedule_reconnect(host);
    }
  }
}

void IOWorker::add_pending_flush(Pool* pool) {
  pools_pending_flush_.push_back(Pool::Ptr(pool));
}

void IOWorker::add_pending_request_processing(Pool* pool) {
  pools_pending_request_processing_.push_back(Pool::Ptr(pool));
}

void IOWorker::maybe_close() {
  if (is_closing() && pending_request_count_ <= 0) {
    if (config_.core_connections_per_host() > 0) {
      for (PoolMap::iterator it = pools_.begin(); it != pools_.end();) {
        // Get the next iterator because Pool::close() can invalidate the
        // current iterator.
        PoolMap::iterator curr_it = it++;
        curr_it->second->close();
      }
      maybe_notify_closed();
    } else {
      // Pool::close is intertwined with this class via notify_pool_closed.
      // Requires special handling to avoid iterator invalidation and double closing
      // other resources.
      // This path only possible for internal config. API does not allow.
      while (!pools_.empty()) pools_.begin()->second->close();
    }
  }
}

void IOWorker::maybe_notify_closed() {
  if (is_closing() && pools_.empty()) {
    state_ = IO_WORKER_STATE_CLOSED;
    session_->notify_worker_closed_async();
    close_handles();
  }
}

void IOWorker::close_handles() {
  EventThread<IOWorkerEvent>::close_handles();
  request_queue_.close_handles();
  uv_check_stop(&check_);
  uv_close(reinterpret_cast<uv_handle_t*>(&check_), NULL);
  uv_prepare_stop(&prepare_);
  uv_close(reinterpret_cast<uv_handle_t*>(&prepare_), NULL);
}

void IOWorker::on_event(const IOWorkerEvent& event) {
  const Address& address = event.host->address();

  switch (event.type) {
    case IOWorkerEvent::ADD_POOL: {
      add_pool(event.host, event.is_initial_connection);
      break;
    }

    case IOWorkerEvent::REMOVE_POOL: {
      PoolMap::iterator it = pools_.find(address);
      if (it != pools_.end()) {
        LOG_DEBUG("Remove pool event for %s closing pool(%p) io_worker(%p)",
                  event.host->address_string().c_str(),
                  static_cast<void*>(it->second.get()),
                  static_cast<void*>(this));
        it->second->close(event.cancel_reconnect);
      }
      break;
    }

    default:
      assert(false);
      break;
  }
}

#if UV_VERSION_MAJOR == 0
void IOWorker::on_execute(uv_async_t* async, int status) {
#else
void IOWorker::on_execute(uv_async_t* async) {
#endif
  IOWorker* io_worker = static_cast<IOWorker*>(async->data);

  RequestHandler* temp = NULL;
  size_t remaining = io_worker->config().max_requests_per_flush();
  while (remaining != 0 && io_worker->request_queue_.dequeue(temp)) {
    RequestHandler::Ptr request_handler(temp);
    if (request_handler) {
      request_handler->dec_ref(); // Queue reference
      io_worker->pending_request_count_++;
      request_handler->start_request(io_worker);
      RequestExecution::Ptr request_execution(new RequestExecution(request_handler,
                                                                   request_handler->current_host()));
      request_execution->execute();
    } else {
      io_worker->state_ = IO_WORKER_STATE_CLOSING;
    }
    remaining--;
  }

  io_worker->maybe_close();
}

#if UV_VERSION_MAJOR == 0
void IOWorker::on_check(uv_check_t* check, int status) {
#else
void IOWorker::on_check(uv_check_t* check) {
#endif
  IOWorker* io_worker = static_cast<IOWorker*>(check->data);

  PoolVec still_requires_processing;
  for (PoolVec::iterator it = io_worker->pools_pending_request_processing_.begin(),
       end = io_worker->pools_pending_request_processing_.end(); it != end; ++it) {
    if ((*it)->process_pending_requests()) {
      still_requires_processing.push_back(*it);
    }
  }

  io_worker->pools_pending_request_processing_.swap(still_requires_processing);
}

#if UV_VERSION_MAJOR == 0
void IOWorker::on_prepare(uv_prepare_t* prepare, int status) {
#else
void IOWorker::on_prepare(uv_prepare_t* prepare) {
#endif
  IOWorker* io_worker = static_cast<IOWorker*>(prepare->data);

  for (PoolVec::iterator it = io_worker->pools_pending_flush_.begin(),
       end = io_worker->pools_pending_flush_.end(); it != end; ++it) {
    (*it)->flush();
  }
  io_worker->pools_pending_flush_.clear();
}

void IOWorker::schedule_reconnect(const Host::ConstPtr& host) {
  if (pools_.count(host->address()) == 0) {
    LOG_INFO("Scheduling reconnect for host %s in %u ms on io_worker(%p)",
             host->address_string().c_str(),
             config_.reconnect_wait_time_ms(),
             static_cast<void*>(this));
    Pool::Ptr pool(new Pool(this, host, false));
    pools_[host->address()] = pool;
    pool->delayed_connect();
  }
}


} // namespace cass
