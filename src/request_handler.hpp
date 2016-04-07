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

#ifndef __CASS_REQUEST_HANDLER_HPP_INCLUDED__
#define __CASS_REQUEST_HANDLER_HPP_INCLUDED__

#include "constants.hpp"
#include "error_response.hpp"
#include "future.hpp"
#include "handler.hpp"
#include "host.hpp"
#include "load_balancing.hpp"
#include "metadata.hpp"
#include "request.hpp"
#include "response.hpp"
#include "retry_policy.hpp"
#include "scoped_ptr.hpp"

#include <string>
#include <uv.h>

namespace cass {

class Connection;
class IOWorker;
class Pool;
class Timer;

class ResponseFuture : public Future {
public:
  ResponseFuture(const Metadata& metadata)
      : Future(CASS_FUTURE_TYPE_RESPONSE)
      , schema_metadata(metadata.schema_snapshot()) { }

  void set_response(Address address, const SharedRefPtr<Response>& response) {
    ScopedMutex lock(&mutex_);
    address_ = address;
    response_ = response;
    internal_set(lock);
  }

  const SharedRefPtr<Response>& response() {
    ScopedMutex lock(&mutex_);
    internal_wait(lock);
    return response_;
  }

  void set_error_with_host_address(Address address, CassError code, const std::string& message) {
    ScopedMutex lock(&mutex_);
    address_ = address;
    internal_set_error(code, message, lock);
  }

  void set_error_with_response(Address address, const SharedRefPtr<Response>& response,
                               CassError code, const std::string& message) {
    ScopedMutex lock(&mutex_);
    address_ = address;
    response_ = response;
    internal_set_error(code, message, lock);
  }

  Address get_host_address() {
    ScopedMutex lock(&mutex_);
    internal_wait(lock);
    return address_;
  }

  std::string statement;
  Metadata::SchemaSnapshot schema_metadata;

private:
  Address address_;
  SharedRefPtr<Response> response_;
};


class RequestHandler : public Handler {
public:
  RequestHandler(const Request* request,
                 ResponseFuture* future,
                 RetryPolicy* retry_policy)
      : Handler(request)
      , future_(future)
      , retry_policy_(retry_policy)
      , num_retries_(0)
      , is_query_plan_exhausted_(true)
      , io_worker_(NULL)
      , pool_(NULL) {
    set_timestamp(request->timestamp());
  }

  virtual void on_set(ResponseMessage* response);
  virtual void on_error(CassError code, const std::string& message);
  virtual void on_timeout();

  virtual void retry();

  void set_query_plan(QueryPlan* query_plan) {
    query_plan_.reset(query_plan);
  }

  void set_io_worker(IOWorker* io_worker);

  Pool* pool() const { return pool_; }

  void set_pool(Pool* pool) {
    pool_ = pool;
  }

  bool get_current_host_address(Address* address);
  void next_host();

  bool is_host_up(const Address& address) const;

  void set_response(const SharedRefPtr<Response>& response);

private:
  void set_error(CassError code, const std::string& message);
  void set_error_with_error_response(const SharedRefPtr<Response>& error,
                                     CassError code, const std::string& message);
  void return_connection();
  void return_connection_and_finish();

  void on_result_response(ResponseMessage* response);
  void on_error_response(ResponseMessage* response);
  void on_error_unprepared(ErrorResponse* error);

  void handle_retry_decision(ResponseMessage* response,
                             const RetryPolicy::RetryDecision& decision);

  ScopedRefPtr<ResponseFuture> future_;
  RetryPolicy* retry_policy_;
  int num_retries_;
  bool is_query_plan_exhausted_;
  SharedRefPtr<Host> current_host_;
  ScopedPtr<QueryPlan> query_plan_;
  IOWorker* io_worker_;
  Pool* pool_;
};

} // namespace cass

#endif
