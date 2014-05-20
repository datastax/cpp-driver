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

void IOWorker::on_event(uv_async_t *async, int status) {
  IOWorker* io_worker  = reinterpret_cast<IOWorker*>(async->data);

  Payload payload;
  while(io_worker->event_queue_.dequeue(payload)) {
    if(payload.type == Payload::ADD_POOL) {
      if(io_worker->pools.count(payload.host) == 0) {
        io_worker->pools.insert(
            std::make_pair(
                payload.host,
                PoolPtr(
                    new Pool(
                        io_worker,
                        payload.host,
                        payload.core_connections_per_host,
                        payload.max_connections_per_host))));
      }
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
