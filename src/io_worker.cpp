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
                        io_worker->session_,
                        io_worker->loop,
                        io_worker->ssl_context,
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
  IOWorker* worker  = reinterpret_cast<IOWorker*>(data->data);
  Request*  request = nullptr;

  while (worker->request_queue_.dequeue(request)) {
    while(!request->hosts.empty()) {
      const auto& host = request->hosts.front();
      request->hosts.pop_front();
      auto it = worker->pools.find(host);
      if(it != worker->pools.end()) {
        auto pool = it->second;
        ClientConnection* connection;
        if(pool->borrow_connection(&connection)) {
          Error* error = connection->execute(request->message, request->future);
          if(error) {
            request->future->error.reset(error);
            request->future->notify(worker->loop);
          }
          break;
        } else { // Too busy, or no connections
          if(pool->wait_for_connection(request)) {
            break; // We're waiting for the next available connection
          }
          // Move to the next host
        }
      }
    }
  }
}

} // namespace cass
