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

#ifndef DATASTAX_INTERNAL_SESSION_HPP
#define DATASTAX_INTERNAL_SESSION_HPP

#include "allocated.hpp"
#include "metrics.hpp"
#include "mpmc_queue.hpp"
#include "request_processor.hpp"
#include "session_base.hpp"

#include <uv.h>

namespace datastax { namespace internal { namespace core {

class RequestProcessorInitializer;
class Statement;

class Session
    : public Allocated
    , public SessionBase
    , public RequestProcessorListener {
public:
  Session();
  ~Session();

  Future::Ptr prepare(const char* statement, size_t length);

  Future::Ptr prepare(const Statement* statement);

  Future::Ptr execute(const Request::ConstPtr& request);

private:
  void execute(const RequestHandler::Ptr& request_handler);

  void join();

private:
  // Session base methods

  virtual void on_connect(const Host::Ptr& connected_host, ProtocolVersion protocol_version,
                          const HostMap& hosts, const TokenMap::Ptr& token_map,
                          const String& local_dc);

  virtual void on_close();

  using SessionBase::on_close; // Intentional overload

private:
  // Cluster listener methods

  virtual void on_host_up(const Host::Ptr& host);

  virtual void on_host_down(const Host::Ptr& host);

  virtual void on_host_added(const Host::Ptr& host);

  virtual void on_host_removed(const Host::Ptr& host);

  virtual void on_token_map_updated(const TokenMap::Ptr& token_map);

  virtual void on_host_maybe_up(const Host::Ptr& host);

  virtual void on_host_ready(const Host::Ptr& host);

private:
  // Request processor listener methods

  virtual void on_pool_up(const Address& address);

  virtual void on_pool_down(const Address& address);

  virtual void on_pool_critical_error(const Address& address, Connector::ConnectionError code,
                                      const String& message);

  virtual void on_keyspace_changed(const String& keyspace,
                                   const KeyspaceChangedHandler::Ptr& handler);

  virtual void on_prepared_metadata_changed(const String& id,
                                            const PreparedMetadata::Entry::Ptr& entry);

  virtual void on_close(RequestProcessor* processor);

  using RequestProcessorListener::on_connect; // Intentional overload

private:
  friend class SessionInitializer;

private:
  ScopedPtr<RoundRobinEventLoopGroup> event_loop_group_;
  uv_mutex_t mutex_;
  RequestProcessor::Vec request_processors_;
  size_t request_processor_count_;
  bool is_closing_;
};

}}} // namespace datastax::internal::core

EXTERNAL_TYPE(datastax::internal::core::Session, CassSession)

#endif
