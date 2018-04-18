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

#ifndef __CASS_SESSION_HPP_INCLUDED__
#define __CASS_SESSION_HPP_INCLUDED__

#include "config.hpp"
#include "control_connection.hpp"
#include "external.hpp"
#include "future.hpp"
#include "host.hpp"
#include "load_balancing.hpp"
#include "metadata.hpp"
#include "metrics.hpp"
#include "mpmc_queue.hpp"
#include "prepared.hpp"
#include "random.hpp"
#include "ref_counted.hpp"
#include "request_processor_manager_initializer.hpp"
#include "request_handler.hpp"
#include "request_queue.hpp"
#include "resolver.hpp"
#include "row.hpp"
#include "scoped_lock.hpp"
#include "scoped_ptr.hpp"
#include "speculative_execution.hpp"
#include "string.hpp"
#include "token_map.hpp"
#include "vector.hpp"
#include "event_loop.hpp"

#include <memory>
#include <uv.h>

namespace cass {

class Future;
class Request;
class Statement;

class Session : public EventLoop
              , public RequestProcessorListener {
public:
  enum State {
    SESSION_STATE_CONNECTING,
    SESSION_STATE_CONNECTED,
    SESSION_STATE_CLOSING,
    SESSION_STATE_CLOSED
  };

  Session();
  ~Session();

  const Config& config() const { return config_; }
  Metrics* metrics() const { return metrics_.get(); }
  const PreparedMetadata& prepared_metadata() const { return prepared_metadata_; }

  PreparedMetadata::Entry::Vec prepared_metadata_entries() const {
    return prepared_metadata_.copy();
  }

public:
  Host::Ptr get_host(const Address& address);

  void connect_async(const Config& config, const String& keyspace, const Future::Ptr& future);
  void close_async(const Future::Ptr& future);

  Future::Ptr prepare(const char* statement, size_t length);
  Future::Ptr prepare(const Statement* statement);
  Future::Ptr execute(const Request::ConstPtr& request,
                      const Address* preferred_address = NULL);

  const Metadata& metadata() const { return metadata_; }

  const VersionNumber& cassandra_version() const {
    return control_connection_.cassandra_version();
  }

  bool token_map_init(const String& partitioner);
  void token_map_host_add(const Host::Ptr& host, const Value* tokens);
  void token_map_host_update(const Host::Ptr& host, const Value* tokens);
  void token_map_host_remove(const Host::Ptr& host);
  void token_map_hosts_cleared();
  void token_map_keyspaces_add(const VersionNumber& cassandra_version,
                               const ResultResponse::Ptr& keyspaces);
  void token_map_keyspaces_update(const VersionNumber& cassandra_version,
                                  const ResultResponse::Ptr& keyspaces);

  void load_balancing_policy_host_add_remove(const Host::Ptr& host, bool is_add);

  void notify_connected();
  void notify_connect_error(CassError code, const String& message);
  void notify_closed();

  void on_keyspace_update(const String& keyspace);
  void on_prepared_metadata_update(const String& id,
                                   const PreparedMetadata::Entry::Ptr& entry);

private:
  class NotifyConnect : public Task {
  public:
    virtual void run(EventLoop* event_loop);
  };

  void handle_notify_connect();

private:
  void clear(const Config& config);
  int init();

  void close_handles();

  void internal_connect();
  void internal_close();

  void execute(const RequestHandler::Ptr& request_handler);

  virtual void on_run();
  virtual void on_after_run();

  static void on_resolve(MultiResolver<Session*>::Resolver* resolver);
  static void on_resolve_done(MultiResolver<Session*>* resolver);

  QueryPlan* new_query_plan();

private:
  // TODO(mpenick): Consider removing friend access to session
  friend class ControlConnection;

  Host::Ptr add_host(const Address& address);
  void purge_hosts(bool is_initial_connection);

  Metadata& metadata() { return metadata_; }

  // Asynchronously notify request processors of a token map update
  void notify_token_map_update();

  // Asynchronously prepare all queries on a host
  bool prepare_host(const Host::Ptr& host,
                    PrepareHostHandler::Callback callback);

  static void on_prepare_host_add(const PrepareHostHandler* handler);
  static void on_prepare_host_up(const PrepareHostHandler* handler);

  void on_control_connection_ready();
  void on_control_connection_error(CassError code, const String& message);

  void on_add(Host::Ptr host);
  void internal_on_add(Host::Ptr host);

  void on_remove(Host::Ptr host);

  void on_up(Host::Ptr host);
  void internal_on_up(Host::Ptr host);

  void on_down(Host::Ptr host);

  static void on_request_processor_manager_initialize(RequestProcessorManagerInitializer* initializer);
  void handle_request_processor_manager_initialize(const RequestProcessorManager::Ptr& request_processor_manager,
                                                   const RequestProcessor::Vec& failures);

private:
  Atomic<State> state_;
  uv_mutex_t state_mutex_;

  Config config_;
  ScopedPtr<Metrics> metrics_;
  CassError connect_error_code_;
  String connect_error_message_;
  String connect_keyspace_;
  Future::Ptr connect_future_;
  Future::Ptr close_future_;

  HostMap hosts_;
  uv_mutex_t hosts_mutex_;

  ScopedPtr<RoundRobinEventLoopGroup> event_loop_group_;
  RequestProcessorManager::Ptr request_processor_manager_;
  ScopedPtr<MPMCQueue<RequestHandler*> > request_queue_;

  ScopedPtr<TokenMap> token_map_;
  Metadata metadata_;
  PreparedMetadata prepared_metadata_;
  ScopedPtr<Random> random_;
  ControlConnection control_connection_;
  bool current_host_mark_;
};

class SessionFuture : public Future {
public:
  typedef SharedRefPtr<SessionFuture> Ptr;

  SessionFuture()
    : Future(FUTURE_TYPE_SESSION) {}
};

} // namespace cass

EXTERNAL_TYPE(cass::Session, CassSession)

#endif
