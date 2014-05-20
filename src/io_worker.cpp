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

void IOWorker::try_next_host(RequestFuture* request_future) {
  if(request_future->hosts.empty()) {
    request_future->set_error(CASS_ERROR(CASS_ERROR_SOURCE_LIBRARY,
                                 CASS_ERROR_LIB_BAD_PARAMS,
                                 "No available hosts"));
    return;
  }

  Host host = request_future->hosts.front();

  request_future->hosts.pop_front();
  request_future->hosts_attempted.push_back(host);

  auto it = pools.find(host);
  if(it != pools.end()) {
    auto pool = it->second;
    ClientConnection* connection =  pool->borrow_connection();
    if(connection != nullptr) {
      if(!pool->execute(connection, request_future)) {
        try_next_host(request_future);
      }
    } else { // Too busy, or no connections
      if(!pool->wait_for_connection(request_future)) {
        try_next_host(request_future);
      }
    }
  } else {
    try_next_host(request_future);
  }
}

void IOWorker::add_pool(Host host) {
  if(pools.count(host) == 0) {
    size_t core_connections_per_host = config_.core_connections_per_host();
    size_t max_connections_per_host = config_.max_connections_per_host();
    pools.insert(
          std::make_pair(
            host,
            PoolPtr(
              new Pool(
                this,
                host,
                std::bind(&IOWorker::on_close, this, std::placeholders::_1),
                core_connections_per_host,
                max_connections_per_host))));
  }
}

void IOWorker::on_close(Host host) {
  printf("closing host %s\n", host.address.to_string().c_str());
  pools.erase(host);
  if(!is_shutdown_) {
    ReconnectRequest* reconnect_request = new ReconnectRequest(this, host);
    Timer::start(loop,
                 config_.reconnect_wait(),
                 reconnect_request,
                 IOWorker::on_reconnect);
  }
}

void IOWorker::on_reconnect(Timer* timer) {
  ReconnectRequest* reconnect_request = static_cast<ReconnectRequest*>(timer->data());
  IOWorker* io_worker = reconnect_request->io_worker;
  if(!io_worker->is_shutdown_) {
    printf("reconnecting to host %s\n", reconnect_request->host.address.to_string().c_str());
    io_worker->add_pool(reconnect_request->host);
  }
  delete reconnect_request;
}

void IOWorker::on_event(uv_async_t *async, int status) {
  IOWorker* io_worker  = reinterpret_cast<IOWorker*>(async->data);

  Payload payload;
  while(io_worker->event_queue_.dequeue(payload)) {
    if(payload.type == Payload::ADD_POOL) {
      io_worker->add_pool(payload.host);
    } else if(payload.type == Payload::REMOVE_POOL) {
      // TODO(mpenick):
    }
  }
}

void IOWorker::on_execute(uv_async_t* data, int status) {
  IOWorker* io_worker  = reinterpret_cast<IOWorker*>(data->data);
  RequestFuture*  request_future = nullptr;
  while (io_worker->request_future_queue_.dequeue(request_future)) {
    io_worker->try_next_host(request_future);
  }
}

} // namespace cass
