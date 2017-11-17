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

#ifndef __CASS_REQUEST_HANDLER_HPP_INCLUDED__
#define __CASS_REQUEST_HANDLER_HPP_INCLUDED__

#include "constants.hpp"
#include "error_response.hpp"
#include "future.hpp"
#include "request_callback.hpp"
#include "host.hpp"
#include "load_balancing.hpp"
#include "metadata.hpp"
#include "prepare_request.hpp"
#include "request.hpp"
#include "response.hpp"
#include "result_response.hpp"
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
class TokenMap;

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

  PrepareRequest::ConstPtr prepare_request;
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

class RequestExecution;
class PreparedMetadata;

class RequestListener {
public:
  virtual void on_result_metadata_changed(const std::string& prepared_id,
                                          const std::string& query,
                                          const std::string& keyspace,
                                          const std::string& result_metadata_id,
                                          const ResultResponse::ConstPtr& result_response) = 0;
};

class RequestHandler : public RefCounted<RequestHandler> {
public:
  typedef SharedRefPtr<RequestHandler> Ptr;

  RequestHandler(const Request::ConstPtr& request,
                 const ResponseFuture::Ptr& future,
                 RequestListener* listener = NULL)
    : wrapper_(request)
    , future_(future)
    , io_worker_(NULL)
    , running_executions_(0)
    , start_time_ns_(uv_hrtime())
    , listener_(listener) { }

  void init(const Config& config, const std::string& connected_keyspace,
            const TokenMap* token_map, const PreparedMetadata& prepared_metdata);

  const RequestWrapper& wrapper() const { return wrapper_; }

  const Request* request() const { return wrapper_.request().get(); }

  CassConsistency consistency() const { return wrapper_.consistency(); }

  const Address& preferred_address() const {
    return preferred_address_;
  }

  void set_preferred_address(const Address& preferred_address) {
    preferred_address_ = preferred_address;
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
  friend class RequestExecution;

  void add_execution(RequestExecution* request_execution);
  void add_attempted_address(const Address& address);
  void schedule_next_execution(const Host::Ptr& current_host);

  // This MUST only be called once and that's currently guaranteed by the
  // response future.
  void stop_request();

private:
  typedef SmallVector<RequestExecution*, 4> RequestExecutionVec;

  RequestWrapper wrapper_;
  SharedRefPtr<ResponseFuture> future_;
  ScopedPtr<QueryPlan> query_plan_;
  ScopedPtr<SpeculativeExecutionPlan> execution_plan_;
  Host::Ptr current_host_;
  IOWorker* io_worker_;
  Timer timer_;
  int running_executions_;
  RequestExecutionVec request_executions_;
  uint64_t start_time_ns_;
  Address preferred_address_;
  ResultMetadata::Ptr prepared_result_metadata_;
  RequestListener* listener_;
};

class RequestExecution : public RequestCallback {
public:
  typedef SharedRefPtr<RequestExecution> Ptr;

  RequestExecution(const RequestHandler::Ptr& request_handler,
                   const Host::Ptr& current_host = Host::Ptr());

  const Host::Ptr& current_host() const { return current_host_; }
  void next_host() { current_host_ = request_handler_->next_host(); }

  void execute();
  void schedule_next(int64_t timeout = 0);
  void cancel();

  void on_result_metadata_changed(const Request *request,
                                  ResultResponse* result_response);

  virtual void on_error(CassError code, const std::string& message);

  virtual void on_retry_current_host();
  virtual void on_retry_next_host();

private:
  void retry_current_host();
  void retry_next_host();

private:
  static void on_execute(Timer* timer);

  virtual void on_start();

  virtual void on_set(ResponseMessage* response);
  virtual void on_cancel();

  void on_result_response(Connection* connection, ResponseMessage* response);
  void on_error_response(Connection* connection, ResponseMessage* response);
  void on_error_unprepared(Connection* connection, ErrorResponse* error);

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
  Connection* connection_;
  Timer schedule_timer_;
  int num_retries_;
  uint64_t start_time_ns_;
};

} // namespace cass

#endif
