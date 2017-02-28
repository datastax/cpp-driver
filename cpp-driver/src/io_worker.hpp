/*
  Copyright (c) 2014-2016 DataStax

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

#include "address.hpp"
#include "atomic.hpp"
#include "async_queue.hpp"
#include "copy_on_write_ptr.hpp"
#include "constants.hpp"
#include "event_thread.hpp"
#include "host.hpp"
#include "logger.hpp"
#include "metrics.hpp"
#include "pool.hpp"
#include "request_handler.hpp"
#include "spsc_queue.hpp"
#include "timer.hpp"

#include <sparsehash/dense_hash_map>

#include <string>
#include <uv.h>

namespace cass {

class Config;
class Pool;
class RequestHandler;
class Session;
class SSLContext;
class Timer;

struct IOWorkerEvent {
  enum Type {
    INVALID,
    ADD_POOL,
    REMOVE_POOL
  };

  IOWorkerEvent()
    : type(INVALID)
    , is_initial_connection(false)
    , cancel_reconnect(false) {}

  Type type;
  Host::ConstPtr host;
  bool is_initial_connection;
  bool cancel_reconnect;
};

class IOWorker
    : public EventThread<IOWorkerEvent>
    , public RefCounted<IOWorker> {
public:
  typedef SharedRefPtr<IOWorker> Ptr;

  enum State {
    IO_WORKER_STATE_READY,
    IO_WORKER_STATE_CLOSING,
    IO_WORKER_STATE_CLOSED
  };

  IOWorker(Session* session);
  ~IOWorker();

  int init();

  bool is_closing() const { return state_ == IO_WORKER_STATE_CLOSING; }
  bool is_ready() const { return state_ == IO_WORKER_STATE_READY; }

  const Config& config() const { return config_; }
  Metrics* metrics() const { return metrics_; }

  int protocol_version() const {
    return protocol_version_.load();
  }
  void set_protocol_version(int protocol_version) {
    protocol_version_.store(protocol_version);
  }

  const CopyOnWritePtr<std::string> keyspace() const { return keyspace_; }
  void set_keyspace(const std::string& keyspace);
  void broadcast_keyspace_change(const std::string& keyspace);

  void set_host_is_available(const Address& address, bool is_available);
  bool is_host_available(const Address& address);

  bool is_host_up(const Address& address) const;

  bool add_pool_async(const Host::ConstPtr& host, bool is_initial_connection);
  bool remove_pool_async(const Host::ConstPtr& host, bool cancel_reconnect);
  void close_async();

  bool execute(const RequestHandler::Ptr& request_handler);

  void retry(const SpeculativeExecution::Ptr& speculative_execution);
  void request_finished();

  void notify_pool_ready(Pool* pool);
  void notify_pool_closed(Pool* pool);

  void add_pending_flush(Pool* pool);

private:
  void add_pool(const Host::ConstPtr& host, bool is_initial_connection);
  void maybe_close();
  void maybe_notify_closed();
  void close_handles();

  static void on_pending_pool_reconnect(Timer* timer);

  virtual void on_event(const IOWorkerEvent& event);

#if UV_VERSION_MAJOR == 0
  static void on_execute(uv_async_t* async, int status);
  static void on_prepare(uv_prepare_t *prepare, int status);
#else
  static void on_execute(uv_async_t* async);
  static void on_prepare(uv_prepare_t *prepare);
#endif

private:
  typedef sparsehash::dense_hash_map<Address, Pool::Ptr, AddressHash> PoolMap;
  typedef std::vector<Pool::Ptr > PoolVec;

  void schedule_reconnect(const Host::ConstPtr& host);

private:
  State state_;
  Session* session_;
  const Config& config_;
  Metrics* metrics_;
  Atomic<int> protocol_version_;
  uv_prepare_t prepare_;

  CopyOnWritePtr<std::string> keyspace_;

  AddressSet unavailable_addresses_;
  uv_mutex_t unavailable_addresses_mutex_;

  PoolMap pools_;
  PoolVec pools_pending_flush_;
  bool is_closing_;
  int pending_request_count_;

  AsyncQueue<SPSCQueue<RequestHandler*> > request_queue_;
};

} // namespace cass

#endif
