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

#include "io_worker.hpp"

#include "config.hpp"
#include "logger.hpp"
#include "pool.hpp"
#include "request_handler.hpp"
#include "session.hpp"
#include "timer.hpp"

#include "third_party/boost/boost/bind.hpp"

namespace cass {

IOWorker::IOWorker(Session* session, Logger* logger, const Config& config)
    : session_(session)
    , logger_(logger)
    , is_closing_(false)
    , pending_request_count_(0)
    , config_(config)
    , request_queue_(config.queue_size_io()) {
  prepare_.data = this;
}

IOWorker::~IOWorker() {
  cleanup();
}

int IOWorker::init() {
  int rc = EventThread<IOWorkerEvent>::init(config_.queue_size_event());
  if (rc != 0) return rc;
  rc = request_queue_.init(loop(), this, &IOWorker::on_execute);
  if (rc != 0) return rc;
  rc = uv_prepare_init(loop(), &prepare_);
  if (rc != 0) return rc;
  rc = uv_prepare_start(&prepare_, on_prepare);
  return rc;
}

bool IOWorker::add_pool_async(const Address& address) {
  IOWorkerEvent event;
  event.type = IOWorkerEvent::ADD_POOL;
  event.address = address;
  return send_event_async(event);
}

bool IOWorker::remove_pool_async(const Address& address) {
  IOWorkerEvent event;
  event.type = IOWorkerEvent::REMOVE_POOL;
  event.address = address;
  return send_event_async(event);
}

void IOWorker::close_async() {
  while (!request_queue_.enqueue(NULL)) {
    // Keep trying
  }
}

void IOWorker::add_pool(const Address& address, bool is_reconnect) {
  if (!is_closing_ && pools.count(address) == 0) {
    Pool* pool = new Pool(address, loop(), logger_, config_);
    if (!is_reconnect) {
      pool->set_ready_callback(boost::bind(&IOWorker::on_pool_ready, this, _1));
    }
    pool->set_closed_callback(boost::bind(&IOWorker::on_pool_closed, this, _1));
    pool->set_keyspace_callback(
        boost::bind(&IOWorker::on_set_keyspace, this, _1));

    pool->connect(session_->keyspace());

    pools[address] = pool;
  }
}

bool IOWorker::execute(RequestHandler* request_handler) {
  return request_queue_.enqueue(request_handler);
}

void IOWorker::on_set_keyspace(const std::string& keyspace) {
  session_->set_keyspace(keyspace);
}

void IOWorker::maybe_close() {
  if (is_closing_ && pending_request_count_ <= 0) {
    for (PoolMap::iterator it = pools.begin(), end = pools.end(); it != end;
         ++it) {
      it->second->close();
    }
    maybe_notify_closed();
  }
}

void IOWorker::maybe_notify_closed() {
  if (pools.empty()) {
    session_->notify_closed_async();
    close_handles();
  }
}

void IOWorker::cleanup() {
  PoolList::iterator it = pending_delete_.begin();
  while (it != pending_delete_.end()) {
    delete *it;
    it = pending_delete_.erase(it);
  }
}

void IOWorker::close_handles() {
  EventThread<IOWorkerEvent>::close_handles();
  request_queue_.close_handles();
  uv_prepare_stop(&prepare_);
  uv_close(copy_cast<uv_prepare_t*, uv_handle_t*>(&prepare_), NULL);
  while (!pending_reconnects_.is_empty()) {
    ReconnectRequest* reconnect_request = pending_reconnects_.front();
    reconnect_request->stop_timer();
    pending_reconnects_.remove(reconnect_request);
    delete reconnect_request;
  }
  logger_->debug("IO worker active handles %d", loop()->active_handles);
}

void IOWorker::on_pool_ready(Pool* pool) {
  session_->notify_ready_async();
}

void IOWorker::on_pool_closed(Pool* pool) {
  const Address& address = pool->address();
  logger_->info("IOWorker: Pool for '%s' closed", address.to_string().c_str());
  pending_delete_.push_back(pool);
  pools.erase(address);
  if (is_closing_) {
    maybe_notify_closed();
  } else {
    ReconnectRequest* reconnect_request = new ReconnectRequest(this, address);
    pending_reconnects_.add_to_back(reconnect_request);
    reconnect_request->timer = Timer::start(loop(),
                                            config_.reconnect_wait(),
                                            reconnect_request,
                                            IOWorker::on_pool_reconnect);
  }
}

void IOWorker::on_retry(RequestHandler* request_handler, RetryType retry_type) {

  if (retry_type == RETRY_WITH_NEXT_HOST) {
    request_handler->next_host();
  }

  Address address;
  if (!request_handler->get_current_host_address(&address)) {
    request_handler->on_error(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
                              "No hosts available");
    return;
  }

  PoolMap::iterator it = pools.find(address);
  if (it != pools.end()) {
    Pool* pool = it->second;
    Connection* connection = pool->borrow_connection(request_handler->keyspace);
    if (connection != NULL) {
      if (!pool->execute(connection, request_handler)) {
        on_retry(request_handler, RETRY_WITH_NEXT_HOST);
      }
    } else { // Too busy, or no connections
      if (!pool->wait_for_connection(request_handler)) {
        on_retry(request_handler, RETRY_WITH_NEXT_HOST);
      }
    }
  } else {
    on_retry(request_handler, RETRY_WITH_NEXT_HOST);
  }
}

void IOWorker::on_request_finished(RequestHandler* request_handler) {
  request_handler->dec_ref();
  pending_request_count_--;
  maybe_close();
}

void IOWorker::on_event(const IOWorkerEvent& event) {
  if (event.type == IOWorkerEvent::ADD_POOL) {
    add_pool(event.address);
  } else if (event.type == IOWorkerEvent::REMOVE_POOL) {
    // TODO(mpenick):
  }
}

void IOWorker::on_pool_reconnect(Timer* timer) {
  ReconnectRequest* reconnect_request =
      static_cast<ReconnectRequest*>(timer->data());
  IOWorker* io_worker = reconnect_request->io_worker;
  if (!io_worker->is_closing_) {
    io_worker->logger_->info(
        "IOWorker: Attempting to reconnect to '%s'",
        reconnect_request->address.to_string(true).c_str());
    io_worker->add_pool(reconnect_request->address, true);
  }
  io_worker->pending_reconnects_.remove(reconnect_request);
  delete reconnect_request;
}

void IOWorker::on_execute(uv_async_t* async, int status) {
  IOWorker* io_worker = static_cast<IOWorker*>(async->data);

  RequestHandler* request_handler = NULL;
  while (io_worker->request_queue_.dequeue(request_handler)) {
    if (request_handler != NULL) {
      io_worker->pending_request_count_++;
      request_handler->set_retry_callback(
          boost::bind(&IOWorker::on_retry, io_worker, _1, _2));
      request_handler->set_finished_callback(
          boost::bind(&IOWorker::on_request_finished, io_worker, _1));
      request_handler->set_loop(io_worker->loop());
      request_handler->retry(RETRY_WITH_CURRENT_HOST);
    } else {
      io_worker->is_closing_ = true;
    }
  }
  io_worker->maybe_close();
}

void IOWorker::on_prepare(uv_prepare_t* prepare, int status) {
  IOWorker* io_worker = static_cast<IOWorker*>(prepare->data);
  io_worker->cleanup();
}

void IOWorker::ReconnectRequest::stop_timer() {
  assert(timer != NULL);
  Timer::stop(timer);
  timer = NULL;
}

} // namespace cass
