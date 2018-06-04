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

#ifndef __CASS_SCHEMA_CHANGE_HANDLER_HPP_INCLUDED__
#define __CASS_SCHEMA_CHANGE_HANDLER_HPP_INCLUDED__

#include "connection.hpp"
#include "ref_counted.hpp"
#include "request_handler.hpp"
#include "request_callback.hpp"
#include "response.hpp"

#include <uv.h>

namespace cass {

/**
 * A listener that used for determining if a host is up.
 */
class SchemaAgreementListener {
public:
  virtual ~SchemaAgreementListener() { }

  /**
   * A callback for determining if a host is up.
   *
   * @param address A host address.
   * @return Returns true if the host is up.
   */
  virtual bool on_is_host_up(const Address& address) = 0;
};

/**
 * A handler that waits for schema agreement after schema changes. It waits for
 * schema to propagate to all nodes then it sets the future of the request that
 * originally made the schema change.
 */
class SchemaAgreementHandler : public RefCounted<SchemaAgreementHandler> {
public:
  typedef SharedRefPtr<SchemaAgreementHandler> Ptr;

  /**
   * Constructor.
   *
   * @param request_handler The request handler for the schema change.
   * @param current_host The host that processed the schema change.
   * @param response The original response for the schema change.
   * @param listener A listener for determining host liveness.
   */
  SchemaAgreementHandler(const RequestHandler::Ptr& request_handler,
                         const Host::Ptr& current_host,
                         const Response::Ptr& response,
                         SchemaAgreementListener* listener,
                         uint64_t max_schema_wait_time_ms);

  /**
   * Gets a request callback for executing queries on behalf of the handler.
   *
   * @return Returns a chain of queries to execute.
   */
  ChainedRequestCallback::Ptr callback();

private:
  friend class SchemaAgreementCallback;

  void start(Connection* connection);
  void schedule();
  void finish();

private:
  void on_retry_timeout(Timer* timer);
  void on_timeout(Timer* timer);

private:
  Timer timer_;
  Timer retry_timer_;
  bool is_finished_;
  Connection::Ptr connection_; // The connection could close so keep a reference
  const uint64_t start_time_ms_;
  const uint64_t max_schema_wait_time_ms_;
  SchemaAgreementListener* const listener_;
  const RequestHandler::Ptr request_handler_;
  const Host::Ptr current_host_;
  const Response::Ptr response_;
};

} // namespace cass

#endif
