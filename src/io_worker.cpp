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
#include "pool.hpp"

namespace cass {

IOWorker::IOWorker(Session* session, const Config& config)
  : session_(session)
  , loop_(uv_loop_new())
  , ssl_context_(nullptr)
  , is_stopped_(false)
  , is_shutting_down_(false)
  , is_shutdown_(false)
  , config_(config)
  , request_queue_(config.queue_size_io())
  , event_queue_(config.queue_size_event()) {
  prepare_.data = this;
}

IOWorker::~IOWorker() {
  uv_loop_delete(loop_);
  cleanup();
}

int IOWorker::init() {
  int rc = request_queue_.init(loop_, this, &IOWorker::on_execute);
  if(rc != 0) return rc;
  rc = event_queue_.init(loop_, this, &IOWorker::on_event);
  if(rc != 0) return rc;
  rc = uv_prepare_init(loop_, &prepare_);
  if(rc != 0) return rc;
  rc = uv_prepare_start(&prepare_, on_prepare);
  return rc;
}

void IOWorker::run() {
  uv_thread_create(&thread_, &IOWorker::on_run, this);
}

void IOWorker::join() {
  if(!is_stopped_) {
    uv_thread_join(&thread_);
    is_stopped_ = true;
  }
}

bool IOWorker::add_pool_q(const Host& host) {
  Payload payload;
  payload.type = Payload::ADD_POOL;
  payload.host = host;
  return event_queue_.enqueue(payload);
}

bool IOWorker::remove_pool_q(const Host& host) {
  Payload payload;
  payload.type = Payload::REMOVE_POOL;
  payload.host = host;
  return event_queue_.enqueue(payload);
}

bool IOWorker::shutdown_q() {
  Payload payload;
  payload.type = Payload::SHUTDOWN;
  return event_queue_.enqueue(payload);
}

void IOWorker::add_pool(Host host) {
  if(!is_shutting_down_ && pools.count(host) == 0) {
    pools[host] = new Pool(host,
                           loop_,
                           ssl_context_,
                           config_,
                           std::bind(&IOWorker::on_connect, this, std::placeholders::_1),
                           std::bind(&IOWorker::on_pool_close, this, std::placeholders::_1),
                           std::bind(&IOWorker::retry, this, std::placeholders::_1, std::placeholders::_2));
  }
}

bool IOWorker::execute(RequestHandler* request_handler) {
  return request_queue_.enqueue(request_handler);
}

void IOWorker::retry(RequestHandler* request_handler, RetryType retry_type) {
  Host host;

  if(retry_type == RETRY_WITH_NEXT_HOST) {
    request_handler->next_host();
  }

  if(!request_handler->get_current_host(&host)) {
    request_handler->on_error(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, "No hosts available");
    return;
  }

  auto it = pools.find(host);
  if(it != pools.end()) {
    auto pool = it->second;
    Connection* connection =  pool->borrow_connection();
    if(connection != nullptr) {
      if(!pool->execute(connection, request_handler)) {
        retry(request_handler, RETRY_WITH_NEXT_HOST);
      }
    } else { // Too busy, or no connections
      if(!pool->wait_for_connection(request_handler)) {
        retry(request_handler, RETRY_WITH_NEXT_HOST);
      }
    }
  } else {
    retry(request_handler, RETRY_WITH_NEXT_HOST);
  }
}

void IOWorker::maybe_shutdown() {
  if(pools.empty()) {
    is_shutdown_ = true;
    uv_stop(loop_);
    session_->notify_shutdown_q();
  }
}

void IOWorker::cleanup()
{
  if(!pending_delete_.empty()) {
    for(auto pool : pending_delete_) {
      delete pool;
    }
    pending_delete_.clear();
  }
}


void IOWorker::on_connect(Host host) {
  session_->notify_connect_q(host);
}

void IOWorker::on_pool_close(Host host) {
  auto it = pools.find(host);
  if(it != pools.end()) {
    pending_delete_.push_back(it->second);
    pools.erase(it);
  }
  if(is_shutting_down_) {
    maybe_shutdown();
  } else {
    ReconnectRequest* reconnect_request = new ReconnectRequest(this, host);
    Timer::start(loop_,
                 config_.reconnect_wait(),
                 reconnect_request,
                 IOWorker::on_pool_reconnect);
  }
}

void IOWorker::on_pool_reconnect(Timer* timer) {
  ReconnectRequest* reconnect_request = static_cast<ReconnectRequest*>(timer->data());
  IOWorker* io_worker = reconnect_request->io_worker;
  if(!io_worker->is_shutting_down_) {
    io_worker->add_pool(reconnect_request->host);
  }
  delete reconnect_request;
}

void IOWorker::on_run(void* data) {
  IOWorker* io_worker = reinterpret_cast<IOWorker*>(data);
  io_worker->is_stopped_ = false;
  uv_run(io_worker->loop_, UV_RUN_DEFAULT);
}

void IOWorker::on_event(uv_async_t *async, int status) {
  IOWorker* io_worker  = reinterpret_cast<IOWorker*>(async->data);

  Payload payload;
  while(io_worker->event_queue_.dequeue(payload)) {
    if(payload.type == Payload::ADD_POOL) {
      io_worker->add_pool(payload.host);
    } else if(payload.type == Payload::REMOVE_POOL) {
      // TODO(mpenick):
    } else if(payload.type == Payload::SHUTDOWN) {
      io_worker->is_shutting_down_ = true;
      for(auto entry : io_worker->pools) {
        entry.second->close();
      }
      io_worker->maybe_shutdown();
    }
  }
}

void IOWorker::on_execute(uv_async_t* async, int status) {
  IOWorker* io_worker  = reinterpret_cast<IOWorker*>(async->data);
  RequestHandler* request_handler = nullptr;
  while (io_worker->request_queue_.dequeue(request_handler)) {
    io_worker->retry(request_handler, RETRY_WITH_CURRENT_HOST);
  }
}

void IOWorker::on_prepare(uv_prepare_t* prepare, int status) {
  IOWorker* io_worker  = reinterpret_cast<IOWorker*>(prepare->data);
  io_worker->cleanup();
}


} // namespace cass
