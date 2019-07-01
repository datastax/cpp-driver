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

#ifndef DATASTAX_INTERNAL_TRACING_DATA_HANDLER_HPP
#define DATASTAX_INTERNAL_TRACING_DATA_HANDLER_HPP

#include "wait_for_handler.hpp"

#include <uv.h>

#define SELECT_TRACES_SESSION "SELECT session_id FROM system_traces.sessions WHERE session_id = ?"

namespace datastax { namespace internal { namespace core {

/**
 * A handler that waits for tracing data to become available for a specified
 * tracing ID.
 */
class TracingDataHandler : public WaitForHandler {
public:
  typedef SharedRefPtr<TracingDataHandler> Ptr;

  /**
   * Constructor.
   *
   * @param request_handler The request handler for the original query.
   * @param current_host The host that processed the original query.
   * @param response The original response for the query. This contains the
   * tracing ID.
   * @param consistency The consistency to use for the tracing data requests.
   * @param max_wait_time_ms The maximum amount of time to wait for the data to
   * become available.
   * @param retry_wait_time_ms The amount of time to wait between failed attempts
   * to retrieve tracing data.
   */
  TracingDataHandler(const RequestHandler::Ptr& request_handler, const Host::Ptr& current_host,
                     const Response::Ptr& response, CassConsistency consistency,
                     uint64_t max_wait_time_ms, uint64_t retry_wait_time_ms);

  /**
   * Gets a request callback for executing queries on behalf of the handler.
   *
   * @return Returns a chain of queries to execute.
   */
  ChainedRequestCallback::Ptr callback();

private:
  virtual bool on_set(const ChainedRequestCallback::Ptr& callback);
  virtual void on_error(WaitForError code, const String& message);

private:
  CassConsistency consistency_;
};

}}} // namespace datastax::internal::core

#endif
