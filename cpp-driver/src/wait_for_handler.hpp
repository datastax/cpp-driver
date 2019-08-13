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

#ifndef DATASTAX_INTERNAL_WAIT_FOR_HANDLER_HPP
#define DATASTAX_INTERNAL_WAIT_FOR_HANDLER_HPP

#include "connection.hpp"
#include "ref_counted.hpp"
#include "request_callback.hpp"
#include "request_handler.hpp"
#include "response.hpp"

#include <uv.h>

namespace datastax { namespace internal { namespace core {

/**
 * A handler that waits for server-side data by running queries and verifying
 * the result.
 */
class WaitForHandler : public RefCounted<WaitForHandler> {
public:
  typedef SharedRefPtr<WaitForHandler> Ptr;

  typedef std::pair<String, Request::ConstPtr> WaitForRequest;
  typedef Vector<WaitForRequest> WaitforRequestVec;

  enum WaitForError {
    WAIT_FOR_ERROR_REQUEST_ERROR,
    WAIT_FOR_ERROR_REQUEST_TIMEOUT,
    WAIT_FOR_ERROR_CONNECTION_CLOSED,
    WAIT_FOR_ERROR_NO_STREAMS,
    WAIT_FOR_ERROR_TIMEOUT
  };

  /**
   * Constructor.
   *
   * @param request_handler The request handler for the original request.
   * @param current_host The host that processed the original request.
   * @param response The original response for the original request.
   * @param max_wait_time_ms The maximum amount of time to wait for the query
   * predicate (@see on_set()) to be fulfilled.
   * @param retry_wait_time_ms The amount of time to wait between failed
   * attempts.
   */
  WaitForHandler(const RequestHandler::Ptr& request_handler, const Host::Ptr& current_host,
                 const Response::Ptr& response, uint64_t max_wait_time_ms,
                 uint64_t retry_wait_time_ms);

  virtual ~WaitForHandler() {}

protected:
  /**
   * Create request callbacks for the given requests.
   *
   * @param requests The request to run.
   * @return A request callback chain.
   */
  ChainedRequestCallback::Ptr callback(const WaitforRequestVec& requests);

  /**
   * A callback called when the requests have successfully returned responses.
   *
   * @param callback The request callback chain.
   * @return Return true if the responses successfully finish the request,
   * otherwise return false to schedule the requests to run again.
   */
  virtual bool on_set(const ChainedRequestCallback::Ptr& callback) = 0;

  /**
   * A callback called when the request encounter an error.
   * @param code A code associated with the error.
   * @param message An error message.
   */
  virtual void on_error(WaitForError code, const String& message) = 0;

protected:
  const Host::Ptr& host() const { return connection_->host(); }

  const Response::Ptr& response() const { return response_; }

  uint64_t max_wait_time_ms() const { return max_wait_time_ms_; }
  uint64_t retry_wait_time_ms() const { return retry_wait_time_ms_; }
  uint64_t request_timeout_ms() const { return request_handler_->request()->request_timeout_ms(); }
  uint64_t start_time_ms() const { return start_time_ms_; }

protected:
  WaitForRequest make_request(const String& key, const String& query);

private:
  friend class WaitForCallback;

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
  WaitforRequestVec requests_;
  const uint64_t start_time_ms_;
  const uint64_t max_wait_time_ms_;
  const uint64_t retry_wait_time_ms_;
  const RequestHandler::Ptr request_handler_;
  const Host::Ptr current_host_;
  const Response::Ptr response_;
};

}}} // namespace datastax::internal::core

#endif
