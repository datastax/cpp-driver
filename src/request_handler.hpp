/*
  Copyright (c) 2014-2015 DataStax

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
#include "schema_metadata.hpp"
#include "scoped_ptr.hpp"

#include <string>
#include <uv.h>

namespace cass {

class Connection;
class IOWorker;
class Pool;
class Timer;

class ResponseFuture : public ResultFuture<Response> {
public:
  ResponseFuture(const Schema& schema)
      : ResultFuture<Response>(CASS_FUTURE_TYPE_RESPONSE)
      , schema(schema) {}

  std::string statement;
  Schema schema;
};

class RequestHandler : public Handler {
public:
  RequestHandler(const Request* request, ResponseFuture* future)
      : request_(request)
      , future_(future)
      , is_query_plan_exhausted_(true)
      , io_worker_(NULL)
      , pool_(NULL) {}

  virtual const Request* request() const { return request_.get(); }

  virtual void start_request();

  virtual void on_set(ResponseMessage* response);
  virtual void on_error(CassError code, const std::string& message);
  virtual void on_timeout();

  void set_query_plan(QueryPlan* query_plan) {
    query_plan_.reset(query_plan);
  }

  void set_io_worker(IOWorker* io_worker);

  Pool* pool() const { return pool_; }

  void set_pool(Pool* pool) {
    pool_ = pool;
  }

  void retry(RetryType type);
  bool get_current_host_address(Address* address);
  void next_host();

  bool is_host_up(const Address& address) const;

  void set_response(Response* response);

private:
  void set_error(CassError code, const std::string& message);
  void return_connection();
  void return_connection_and_finish();

  void on_result_response(ResponseMessage* response);
  void on_error_response(ResponseMessage* response);

  ScopedRefPtr<const Request> request_;
  ScopedRefPtr<ResponseFuture> future_;
  bool is_query_plan_exhausted_;
  SharedRefPtr<Host> current_host_;
  ScopedPtr<QueryPlan> query_plan_;
  IOWorker* io_worker_;
  Pool* pool_;
  uint64_t start_time_ns_;
};

} // namespace cass

#endif
