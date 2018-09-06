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

#include "metrics.hpp"
#include "mpmc_queue.hpp"
#include "request_processor.hpp"
#include "session_base.hpp"

#include <uv.h>

namespace cass {

class RequestProcessorInitializer;
class Statement;

class Session
    : public SessionBase
    , public RequestProcessorListener {
public:
  Session();
  ~Session();

  Future::Ptr prepare(const char* statement, size_t length);

  Future::Ptr prepare(const Statement* statement);

  Future::Ptr execute(const Request::ConstPtr& request,
                      const Address* preferred_address = NULL);

private:
  void execute(const RequestHandler::Ptr& request_handler);

  void join();

private:
  // Session base methods

  virtual void on_connect(const Host::Ptr& connected_host,
                          int protocol_version,
                          const HostMap& hosts,
                          const TokenMap::Ptr& token_map);

  virtual void on_close();

private:
  // Cluster listener methods

  virtual void on_up(const Host::Ptr& host);

  virtual void on_down(const Host::Ptr& host);

  virtual void on_add(const Host::Ptr& host);

  virtual void on_remove(const Host::Ptr& host);

  virtual void on_update_token_map(const TokenMap::Ptr& token_map);

  // Tell the compiler that we're intentionally using an overloaded virtual
  // method.
  using SessionBase::on_close;

private:
  // Request processor listener methods

  virtual void on_pool_up(const Address& address);

  virtual void on_pool_down(const Address& address);

  virtual void on_pool_critical_error(const Address& address,
                                      Connector::ConnectionError code,
                                      const String& message);

  virtual void on_keyspace_changed(const String& keyspace,
                                   const KeyspaceChangedHandler::Ptr& handler);

  virtual void on_prepared_metadata_changed(const String& id,
                                           const PreparedMetadata::Entry::Ptr& entry);

  virtual void on_close(RequestProcessor* processor);

private:
  friend class SessionInitializer;

private:
  ScopedPtr<RoundRobinEventLoopGroup> event_loop_group_;
  RequestProcessor::Vec request_processors_;
  uv_mutex_t request_processor_mutex_;
  size_t request_processor_count_;
};

class SessionFuture : public Future {
public:
  typedef SharedRefPtr<SessionFuture> Ptr;

  SessionFuture()
    : Future(FUTURE_TYPE_SESSION) { }
};

} // namespace cass

EXTERNAL_TYPE(cass::Session, CassSession)

#endif
