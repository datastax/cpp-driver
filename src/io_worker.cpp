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
      // TODO:(mpenick)
    }
  }
}

void IOWorker::on_execute(uv_async_t* data, int status) {
  IOWorker* worker  = reinterpret_cast<IOWorker*>(data->data);
  Request*  request = nullptr;

  while (worker->request_queue_.dequeue(request)) {
    for (const Host& host : request->hosts) {
      auto it = worker->pools.find(host);
      if(it != worker->pools.end()) {
        PoolPtr pool = it->second;

        ClientConnection* connection;
        if(pool->borrow_connection(&connection)) {
          connection->execute(request->message, request->future);
        }
      }
    }
  }
}

} // namespace cass
