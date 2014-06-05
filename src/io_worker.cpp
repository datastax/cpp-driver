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

IOWorker::IOWorker(Session* session,
                   Logger* logger,
                   const Config& config)
  : session_(session)
  , logger_(logger)
  , ssl_context_(nullptr)
  , is_shutting_down_(false)
  , is_shutdown_(false)
  , config_(config)
  , request_queue_(config.queue_size_io()) {
  prepare_.data = this;
}

IOWorker::~IOWorker() {
  cleanup();
}

int IOWorker::init() {
  int rc = EventThread::init(config_.queue_size_event());
  if(rc != 0) return rc;
  rc = request_queue_.init(loop(), this, &IOWorker::on_execute);
  if(rc != 0) return rc;
  rc = uv_prepare_init(loop(), &prepare_);
  if(rc != 0) return rc;
  rc = uv_prepare_start(&prepare_, on_prepare);
  return rc;
}

bool IOWorker::add_pool_async(Host host) {
  IOWorkerEvent event;
  event.type = IOWorkerEvent::ADD_POOL;
  event.host = host;
  return send_event_async(event);
}

bool IOWorker::remove_pool_async(Host host) {
  IOWorkerEvent event;
  event.type = IOWorkerEvent::REMOVE_POOL;
  event.host = host;
  return send_event_async(event);
}

bool IOWorker::shutdown_async() {
  IOWorkerEvent event;
  event.type = IOWorkerEvent::SHUTDOWN;
  return send_event_async(event);
}

void IOWorker::add_pool(Host host) {
  if(!is_shutting_down_ && pools.count(host) == 0) {
    std::shared_ptr<std::string> keyspace = session_->keyspace();
    pools[host] = new Pool(host,
                           loop(),
                           ssl_context_,
                           logger_,
                           config_,
                           keyspace ? *keyspace : "",
                           std::bind(&IOWorker::on_connect, this, std::placeholders::_1),
                           std::bind(&IOWorker::on_pool_close, this, std::placeholders::_1),
                           std::bind(&IOWorker::retry, this, std::placeholders::_1, std::placeholders::_2),
                           std::bind(&IOWorker::set_keyspace, this, std::placeholders::_1));
  }
}

bool IOWorker::execute(RequestHandler* request_handler) {
  return request_queue_.enqueue(request_handler);
}

void IOWorker::retry(RequestHandler* request_handler, RetryType retry_type) {
  Host host;
  std::shared_ptr<std::string> keyspace = request_handler->keyspace;

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
    Connection* connection =  pool->borrow_connection(keyspace ? *keyspace : "");
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

void IOWorker::set_keyspace(const std::string& keyspace) {
  session_->set_keyspace(keyspace);
}

void IOWorker::maybe_shutdown() {
  if(pools.empty()) {
    is_shutdown_ = true;
    session_->notify_shutdown_async();
    close_handles();
  }
}

void IOWorker::cleanup() {
  auto it = pending_delete_.begin();
  while(it != pending_delete_.end()) {
    delete *it;
    it = pending_delete_.erase(it);
  }
}

void IOWorker::close_handles() {
  EventThread::close_handles();
  request_queue_.close_handles();
  uv_prepare_stop(&prepare_);
  logger_->debug("IO worker active handles %d", loop()->active_handles);
}

void IOWorker::on_connect(Host host) {
  session_->notify_connect_async(host);
}

void IOWorker::on_pool_close(Host host) {
  auto it = pools.find(host);
  if(it != pools.end()) {
    logger_->info("Pool for '%s' closed", host.address.to_string().c_str());
    pending_delete_.push_back(it->second);
    pools.erase(it);
    if(is_shutting_down_) {
      maybe_shutdown();
    } else {
      ReconnectRequest* reconnect_request = new ReconnectRequest(this, host);
      Timer::start(loop(),
                   config_.reconnect_wait(),
                   reconnect_request,
                   IOWorker::on_pool_reconnect);
    }
  }
}

void IOWorker::on_event(const IOWorkerEvent& event) {
  if(event.type == IOWorkerEvent::ADD_POOL) {
    add_pool(event.host);
  } else if(event.type == IOWorkerEvent::REMOVE_POOL) {
    // TODO(mpenick):
  } else if(event.type == IOWorkerEvent::SHUTDOWN) {
    is_shutting_down_ = true;
    for(auto entry : pools) {
      entry.second->close();
    }
    maybe_shutdown();
  }
}

void IOWorker::on_pool_reconnect(Timer* timer) {
  ReconnectRequest* reconnect_request = static_cast<ReconnectRequest*>(timer->data());
  IOWorker* io_worker = reconnect_request->io_worker;
  if(!io_worker->is_shutting_down_) {
    io_worker->logger_->info("Attempting to reconnect to '%s'", reconnect_request->host.address.to_string().c_str());
    io_worker->add_pool(reconnect_request->host);
  }
  delete reconnect_request;
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
