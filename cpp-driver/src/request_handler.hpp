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
#include "request_callback.hpp"
#include "host.hpp"
#include "load_balancing.hpp"
#include "metadata.hpp"
#include "request.hpp"
#include "response.hpp"
#include "retry_policy.hpp"
#include "scoped_ptr.hpp"
#include "small_vector.hpp"
#include "speculative_execution.hpp"

#include <string>
#include <uv.h>

namespace cass {

class Connection;
class IOWorker;
class Pool;
class Timer;

class ResponseFuture : public Future {
public:
  typedef SharedRefPtr<ResponseFuture> Ptr;

  ResponseFuture()
      : Future(CASS_FUTURE_TYPE_RESPONSE) { }

  ResponseFuture(const Metadata::SchemaSnapshot& schema_metadata)
      : Future(CASS_FUTURE_TYPE_RESPONSE)
      , schema_metadata(new Metadata::SchemaSnapshot(schema_metadata)) { }

  bool set_response(Address address, const Response::Ptr& response) {
    ScopedMutex lock(&mutex_);
    if (!is_set()) {
      address_ = address;
      response_ = response;
      internal_set(lock);
      return true;
    }
    return false;
  }

  const Response::Ptr& response() {
    ScopedMutex lock(&mutex_);
    internal_wait(lock);
    return response_;
  }

  bool set_error_with_address(Address address, CassError code, const std::string& message) {
    ScopedMutex lock(&mutex_);
    if (!is_set()) {
      address_ = address;
      internal_set_error(code, message, lock);
      return true;
    }
    return false;
  }

  bool set_error_with_response(Address address, const Response::Ptr& response,
                               CassError code, const std::string& message) {
    ScopedMutex lock(&mutex_);
    if (!is_set()) {
      address_ = address;
      response_ = response;
      internal_set_error(code, message, lock);
      return true;
    }
    return false;
  }

  Address address() {
    ScopedMutex lock(&mutex_);
    internal_wait(lock);
    return address_;
  }

  // Currently, used for testing only, but it could be exposed in the future.
  AddressVec attempted_addresses() {
    ScopedMutex lock(&mutex_);
    internal_wait(lock);
    return attempted_addresses_;
  }

  std::string statement;
  ScopedPtr<Metadata::SchemaSnapshot> schema_metadata;

private:
  friend class RequestHandler;

  void add_attempted_address(const Address& address) {
    ScopedMutex lock(&mutex_);
    attempted_addresses_.push_back(address);
  }

private:
  Address address_;
  Response::Ptr response_;
  AddressVec attempted_addresses_;
};

class SpeculativeExecution;

class RequestHandler : public RefCounted<RequestHandler> {
public:
  typedef SharedRefPtr<RequestHandler> Ptr;

  RequestHandler(const Request::ConstPtr& request,
                 const ResponseFuture::Ptr& future,
                 RetryPolicy* retry_policy)
    : request_(request)
    , timestamp_(request->timestamp())
    , future_(future)
    , retry_policy_(retry_policy)
    , io_worker_(NULL)
    , running_executions_(0)
    , start_time_ns_(uv_hrtime()) { }

  const Request* request() const { return request_.get(); }

  int64_t timestamp() const { return timestamp_; }
  void set_timestamp(int64_t timestamp) { timestamp_ = timestamp; }

  Request::EncodingCache* encoding_cache() { return &encoding_cache_; }

  RetryPolicy* retry_policy() { return retry_policy_; }

  const Address& preferred_address() const {
    return preferred_address_;
  }

  void set_preferred_address(const Address& preferred_address) {
    preferred_address_ = preferred_address;
  }

  void set_query_plan(QueryPlan* query_plan) { query_plan_.reset(query_plan); }

  void set_execution_plan(SpeculativeExecutionPlan* execution_plan) {
    execution_plan_.reset(execution_plan);
  }

  const Host::Ptr& current_host() const { return current_host_; }
  const Host::Ptr& next_host() {
    current_host_ = query_plan_->compute_next();
    return current_host_;
  }

  IOWorker* io_worker() { return io_worker_; }

  void start_request(IOWorker* io_worker);

  void set_response(const Host::Ptr& host,
                    const Response::Ptr& response);
  void set_error(CassError code, const std::string& message);
  void set_error(const Host::Ptr& host,
                 CassError code, const std::string& message);
  void set_error_with_error_response(const Host::Ptr& host,
                                     const Response::Ptr& error,
                                     CassError code, const std::string& message);

private:
  static void on_timeout(Timer* timer);

private:
  friend class SpeculativeExecution;

  void add_execution(SpeculativeExecution* speculative_execution);
  void add_attempted_address(const Address& address);
  void schedule_next_execution(const Host::Ptr& current_host);

  // This MUST only be called once and that's currently guaranteed by the
  // response future.
  void stop_request();

private:
  typedef SmallVector<SpeculativeExecution*, 4> SpeculativeExecutionVec;

  const Request::ConstPtr request_;
  int64_t timestamp_;
  SharedRefPtr<ResponseFuture> future_;
  RetryPolicy* retry_policy_;
  ScopedPtr<QueryPlan> query_plan_;
  ScopedPtr<SpeculativeExecutionPlan> execution_plan_;
  Host::Ptr current_host_;
  IOWorker* io_worker_;
  Timer timer_;
  int running_executions_;
  SpeculativeExecutionVec speculative_executions_;
  Request::EncodingCache encoding_cache_;
  uint64_t start_time_ns_;
  Address preferred_address_;
};

class SpeculativeExecution : public RequestCallback {
public:
  typedef SharedRefPtr<SpeculativeExecution> Ptr;

  SpeculativeExecution(const RequestHandler::Ptr& request_handler,
                       const Host::Ptr& current_host = Host::Ptr());

  virtual const Request* request() const { return request_handler_->request(); }
  virtual int64_t timestamp() const { return request_handler_->timestamp(); }
  virtual Request::EncodingCache* encoding_cache() { return request_handler_->encoding_cache(); }

  Pool* pool() const { return pool_; }
  void set_pool(Pool* pool) { pool_ = pool; }

  const Host::Ptr& current_host() const { return current_host_; }
  void next_host() { current_host_ = request_handler_->next_host(); }

  void retry_current_host();
  void retry_next_host();

  void start_pending_request(Pool* pool, Timer::Callback cb);
  void stop_pending_request();

  void execute();
  void schedule_next(int64_t timeout = 0);
  void cancel();

  virtual void on_error(CassError code, const std::string& message);

private:
  static void on_execute(Timer* timer);

  virtual void on_start();

  virtual void on_retry(bool use_next_host);

  virtual void on_set(ResponseMessage* response);
  virtual void on_cancel();

  void on_result_response(ResponseMessage* response);
  void on_error_response(ResponseMessage* response);
  void on_error_unprepared(ErrorResponse* error);

  void return_connection();

private:
  friend class SchemaChangeCallback;

  bool is_host_up(const Address& address) const;

  void set_response(const Response::Ptr& response);
  void set_error(CassError code, const std::string& message);
  void set_error_with_error_response(const Response::Ptr& error,
                                     CassError code, const std::string& message);

private:
  RequestHandler::Ptr request_handler_;
  Host::Ptr current_host_;
  Pool* pool_;
  Connection* connection_;
  Timer schedule_timer_;
  Timer pending_request_timer_;
  int num_retries_;
  uint64_t start_time_ns_;
};

} // namespace cass

#endif
