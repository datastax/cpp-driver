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

#include "async_queue.hpp"
#include "config.hpp"
#include "connection_pool_connector.hpp"
#include "connection_pool_manager.hpp"
#include "control_connection.hpp"
#include "external.hpp"
#include "future.hpp"
#include "host.hpp"
#include "load_balancing.hpp"
#include "metadata.hpp"
#include "metrics.hpp"
#include "mpmc_queue.hpp"
#include "prepared.hpp"
#include "prepare_host_handler.hpp"
#include "random.hpp"
#include "ref_counted.hpp"
#include "request_handler.hpp"
#include "request_queue.hpp"
#include "resolver.hpp"
#include "row.hpp"
#include "schema_agreement_handler.hpp"
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

class ConnectionPoolManagerInitializer;
class Future;
class IOWorker;
class Request;
class Statement;

class Session : public EventLoop,
    public RequestListener,
    public ConnectionPoolManagerListener,
    public SchemaAgreementListener {
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

  PreparedMetadata::Entry::Vec prepared_metadata_entries() const {
    return prepared_metadata_.copy();
  }

public:
  Host::Ptr get_host(const Address& address);

  void notify_initialize_async(const ConnectionPoolManager::Ptr& manager,
                               const ConnectionPoolConnector::Vec& failures);
  void notify_up_async(const Address& address);
  void notify_down_async(const Address& address);

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


private:
  class NotifyConnect : public Task {
  public:
    virtual void run(EventLoop* event_loop);
  };

  void handle_notify_connect();

  class NotifyInitalize : public Task {
  public:
    NotifyInitalize(const ConnectionPoolManager::Ptr& manager,
                    const ConnectionPoolConnector::Vec& failures)
      : manager_(manager)
      , failures_(failures) { }

    virtual void run(EventLoop* event_loop);

  private:
    ConnectionPoolManager::Ptr manager_;
    ConnectionPoolConnector::Vec failures_;
  };

  void handle_notify_initialize(const ConnectionPoolManager::Ptr& manager,
                                const ConnectionPoolConnector::Vec& failures);

  class NotifyUp : public Task {
  public:
    NotifyUp(const Address& address)
      : address_(address) { }

    virtual void run(EventLoop* event_loop);

  private:
    Address address_;
  };

  class NotifyDown : public Task {
  public:
    NotifyDown(const Address& address)
      : address_(address) { }

    virtual void run(EventLoop* event_loop);

  private:
    Address address_;
  };

private:
  void clear(const Config& config);
  int init();

  void close_handles();

  void internal_connect();
  void internal_close();

  void notify_connected();
  void notify_connect_error(CassError code, const String& message);
  void notify_closed();

  void execute(const RequestHandler::Ptr& request_handler);

  virtual void on_run();
  virtual void on_after_run();

  static void on_resolve(MultiResolver<Session*>::Resolver* resolver);
  static void on_resolve_done(MultiResolver<Session*>* resolver);

  static void on_execute(Async* async);

  QueryPlan* new_query_plan();

  void on_reconnect(Timer* timer);

private:
  // TODO(mpenick): Consider removing friend access to session
  friend class ControlConnection;

  Host::Ptr add_host(const Address& address);
  void purge_hosts(bool is_initial_connection);

  Metadata& metadata() { return metadata_; }

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

  static void on_initialize(ConnectionPoolManagerInitializer* initializer);
  void handle_initialize(ConnectionPoolManagerInitializer* initializer);

  // Request listener callbacks
  virtual void on_result_metadata_changed(const String& prepared_id,
                                          const String& query,
                                          const String& keyspace,
                                          const String& result_metadata_id,
                                          const ResultResponse::ConstPtr& result_response);
  virtual void on_keyspace_changed(const String& keyspace);
  virtual bool on_wait_for_schema_agreement(const RequestHandler::Ptr& request_handler,
                                            const Host::Ptr& current_host,
                                            const Response::Ptr& response);
  virtual bool on_prepare_all(const RequestHandler::Ptr& request_handler,
                              const Host::Ptr& current_host,
                              const Response::Ptr& response);

  // Conneciton pool callbacks
  virtual void on_up(const Address& address);
  virtual void on_down(const Address& address);
  virtual void on_critical_error(const Address& address,
                                 Connector::ConnectionError code,
                                 const String& message);

  // Schema aggreement callback
  virtual bool on_is_host_up(const Address& address);

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
  ScopedPtr<RequestQueueManager> request_queue_manager_;
  ConnectionPoolManager::Ptr manager_;

  ScopedPtr<AsyncQueue<MPMCQueue<RequestHandler*> > > request_queue_;

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
