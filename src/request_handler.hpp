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

#ifndef __CASS_REQUEST_HANDLER_HPP_INCLUDED__
#define __CASS_REQUEST_HANDLER_HPP_INCLUDED__

#include "constants.hpp"
#include "future.hpp"
#include "handler.hpp"
#include "host.hpp"
#include "load_balancing.hpp"
#include "request.hpp"
#include "response.hpp"
#include "scoped_ptr.hpp"

#include "third_party/boost/boost/function.hpp"

#include <list>
#include <string>
#include <uv.h>

namespace cass {

class Timer;
class Connection;
class Pool;

class ResponseFuture : public ResultFuture<Response> {
public:
  ResponseFuture()
      : ResultFuture<Response>(CASS_FUTURE_TYPE_RESPONSE) {}

  std::string statement;
};

class RequestHandler : public Handler {
public:
  typedef boost::function1<void, RequestHandler*> Callback;
  typedef boost::function2<void, RequestHandler*, RetryType> RetryCallback;

  RequestHandler(const Request* request, ResponseFuture* future)
      : request_(request)
      , future_(future)
      , is_query_plan_exhauted_(false)
      , connection_(NULL)
      , pool_(NULL) {}

  virtual const Request* request() const { return request_.get(); }

  virtual void on_set(ResponseMessage* response);

  virtual void on_error(CassError code, const std::string& message);

  virtual void on_timeout();

  void set_query_plan(QueryPlan* query_plan) {
    query_plan_.reset(query_plan);
    next_host();
  }

  void set_retry_callback(RetryCallback callback) {
    retry_callback_ = callback;
  }

  void retry(RetryType type) {
    // Reset the original request so it can be executed again
    set_state(REQUEST_STATE_NEW);
    pool_ = NULL;

    if (retry_callback_) {
      retry_callback_(this, type);
    }
  }

  void set_finished_callback(Callback callback) {
    finished_callback_ = callback;
  }

  // It's important to use a loop with the same thread as where on_set(),
  // on_error(), or on_timeout() are going to be called because
  // uv_queue_work() is NOT threadsafe.
  void set_loop(uv_loop_t* loop) {
    future_->set_loop(loop);
  }

  void set_connection_and_pool(Connection* connection, Pool* pool) {
    connection_ = connection;
    pool_ = pool;
  }

  bool get_current_host(Host* host) {
    if (is_query_plan_exhauted_) {
      return false;
    }
    *host = current_host_;
    return true;
  }

  void next_host() {
    is_query_plan_exhauted_ = !query_plan_->compute_next(&current_host_);
  }

public:
  std::string keyspace;

private:
  void set_error(CassError code, const std::string& message);
  void return_connection();

  void notify_finished() {
    if (finished_callback_) {
      finished_callback_(this);
    }
  }

  void on_result_response(ResponseMessage* response);
  void on_error_response(ResponseMessage* response);

  ScopedRefPtr<const Request> request_;
  ScopedRefPtr<ResponseFuture> future_;
  bool is_query_plan_exhauted_;
  Host current_host_;
  ScopedPtr<QueryPlan> query_plan_;
  Connection* connection_;
  Pool* pool_;
  RetryCallback retry_callback_;
  Callback finished_callback_;
};

} // namespace cass

#endif
