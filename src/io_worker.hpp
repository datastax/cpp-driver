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

#ifndef __CASS_IO_WORKER_HPP_INCLUDED__
#define __CASS_IO_WORKER_HPP_INCLUDED__

#include <uv.h>
#include <string>
#include <unordered_map>
#include <vector>

#include "event_thread.hpp"
#include "async_queue.hpp"
#include "config.hpp"
#include "host.hpp"
#include "ssl_context.hpp"
#include "spsc_queue.hpp"
#include "request_handler.hpp"
#include "logger.hpp"

namespace cass {

class Pool;
class Session;

struct IOWorkerEvent {
    enum Type {
      ADD_POOL,
      REMOVE_POOL,
      CLOSE,
    };
    Type type;
    Host host;
};


class IOWorker : public EventThread<IOWorkerEvent> {
  public:
    IOWorker(Session* session,
             Logger* logger,
             const Config& config);
    ~IOWorker();

    int init();

    bool add_pool_async(Host host);
    bool remove_pool_async(Host host);
    bool close_async();

    bool execute(RequestHandler* request_handler);

  private:
    void add_pool(Host host);
    void retry(RequestHandler* request_handler, RetryType retry_type);
    void set_keyspace(const std::string& keyspace);
    void maybe_close();
    void cleanup();
    void close_handles();

    void on_connect(Host host);
    void on_pool_close(Host host);
    static void on_pool_reconnect(Timer* timer);
    virtual void on_event(const IOWorkerEvent& event);

    static void on_execute(uv_async_t* data, int status);
    static void on_prepare(uv_prepare_t* prepare, int status);

  private:
    typedef std::unordered_map<Host, Pool*> PoolCollection;

    struct ReconnectRequest {
        ReconnectRequest(IOWorker* io_worker, Host host)
          : io_worker(io_worker)
          , host(host) { }

        IOWorker* io_worker;
        Host host;
    };

  private:
    Session* session_;
    Logger* logger_;
    uv_prepare_t prepare_;
    SSLContext* ssl_context_;
    PoolCollection pools;
    std::list<Pool*> pending_delete_;
    bool is_closing_;

    const Config& config_;
    AsyncQueue<SPSCQueue<RequestHandler*>> request_queue_;

};

} // namespace cass

#endif
