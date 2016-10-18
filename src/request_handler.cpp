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

#include "request_handler.hpp"

#include "batch_request.hpp"
#include "connection.hpp"
#include "constants.hpp"
#include "error_response.hpp"
#include "execute_request.hpp"
#include "io_worker.hpp"
#include "pool.hpp"
#include "prepare_request.hpp"
#include "response.hpp"
#include "result_response.hpp"
#include "row.hpp"
#include "schema_change_callback.hpp"
#include "session.hpp"

#include <uv.h>

namespace cass {

class PrepareCallback : public SimpleRequestCallback {
public:
  PrepareCallback(const std::string& query, SpeculativeExecution* speculative_execution)
    : SimpleRequestCallback(Request::ConstPtr(new PrepareRequest(query)))
    , speculative_execution_(speculative_execution) { }

private:
  virtual void on_internal_set(ResponseMessage* response);
  virtual void on_internal_error(CassError code, const std::string& message);
  virtual void on_internal_timeout();

private:
  SpeculativeExecution::Ptr speculative_execution_;
};

void PrepareCallback::on_internal_set(ResponseMessage* response) {
  switch (response->opcode()) {
    case CQL_OPCODE_RESULT: {
      ResultResponse* result =
          static_cast<ResultResponse*>(response->response_body().get());
      if (result->kind() == CASS_RESULT_KIND_PREPARED) {
        speculative_execution_->retry_current_host();
      } else {
        speculative_execution_->retry_next_host();
      }
    } break;
    case CQL_OPCODE_ERROR:
      speculative_execution_->retry_next_host();
      break;
    default:
      break;
  }
}

void PrepareCallback::on_internal_error(CassError code, const std::string& message) {
  speculative_execution_->retry_next_host();
}

void PrepareCallback::on_internal_timeout() {
  speculative_execution_->retry_next_host();
}

void RequestHandler::add_execution(SpeculativeExecution* speculative_execution) {
  running_executions_++;
  speculative_execution->inc_ref();
  speculative_executions_.push_back(speculative_execution);
}

void RequestHandler::add_attempted_address(const Address& address) {
  future_->add_attempted_address(address);
}

void RequestHandler::schedule_next_execution(const Host::Ptr& current_host) {
  int64_t timeout = execution_plan_->next_execution(current_host);
  if (timeout >= 0) {
    SpeculativeExecution::Ptr speculative_execution(
          new SpeculativeExecution(RequestHandler::Ptr(this)));
    speculative_execution->schedule_next(timeout);
  }
}

void RequestHandler::start_request(IOWorker* io_worker) {
  io_worker_ = io_worker;
  uint64_t request_timeout_ms = request_->request_timeout_ms(
                                  io_worker->config().request_timeout_ms());
  if (request_timeout_ms > 0) { // 0 means no timeout
    timer_.start(io_worker->loop(),
                 request_timeout_ms,
                 this,
                 on_timeout);
  }
}

void RequestHandler::set_response(const Host::Ptr& host,
                                  const Response::Ptr& response) {
  if (future_->set_response(host->address(), response)) {
    io_worker_->metrics()->record_request(uv_hrtime() - start_time_ns_);
    stop_request();
  }
}

void RequestHandler::set_error(CassError code,
                               const std::string& message) {
  if (future_->set_error(code, message)) {
    stop_request();
  }
}

void RequestHandler::set_error(const Host::Ptr& host,
                               CassError code, const std::string& message) {
  bool skip = (code == CASS_ERROR_LIB_NO_HOSTS_AVAILABLE && --running_executions_ > 0);
  if (!skip) {
    if (host) {
      if (future_->set_error_with_address(host->address(), code, message)) {
        stop_request();
      }
    } else {
      set_error(code, message);
    }
  }
}

void RequestHandler::set_error_with_error_response(const Host::Ptr& host,
                                                   const Response::Ptr& error,
                                                   CassError code, const std::string& message) {
  if (future_->set_error_with_response(host->address(), error, code, message)) {
    stop_request();
  }
}

void RequestHandler::on_timeout(Timer* timer) {
  RequestHandler* request_handler =
      static_cast<RequestHandler*>(timer->data());
  request_handler->io_worker_->metrics()->request_timeouts.inc();
  request_handler->set_error(CASS_ERROR_LIB_REQUEST_TIMED_OUT,
                             "Request timed out");
  LOG_DEBUG("Request timed out");
}

void RequestHandler::stop_request() {
  timer_.stop();
  for (SpeculativeExecutionVec::const_iterator i = speculative_executions_.begin(),
       end = speculative_executions_.end(); i != end; ++i) {
    SpeculativeExecution* speculative_execution = *i;
    speculative_execution->cancel();
    speculative_execution->dec_ref();
  }
  if (io_worker_ != NULL) {
    io_worker_->request_finished();
  }
}

SpeculativeExecution::SpeculativeExecution(const RequestHandler::Ptr& request_handler,
                                           const Host::Ptr& current_host)
  : RequestCallback()
  , request_handler_(request_handler)
  , current_host_(current_host)
  , pool_(NULL)
  , num_retries_(0)
  , start_time_ns_(0) {
  request_handler_->add_execution(this);
}

void SpeculativeExecution::on_execute(Timer* timer) {
  SpeculativeExecution* speculative_execution = static_cast<SpeculativeExecution*>(timer->data());
  speculative_execution->next_host();
  speculative_execution->execute();
}

void SpeculativeExecution::on_start() {
  assert(current_host_ && "Tried to start on a non-existent host");
  if (request()->record_attempted_addresses()) {
    request_handler_->add_attempted_address(current_host_->address());
  }
  start_time_ns_ = uv_hrtime();
}

void SpeculativeExecution::on_set(ResponseMessage* response) {
  assert(connection() != NULL);
  assert(current_host_ && "Tried to set on a non-existent host");

  return_connection();

  switch (response->opcode()) {
    case CQL_OPCODE_RESULT:
      on_result_response(response);
      break;
    case CQL_OPCODE_ERROR:
      on_error_response(response);
      break;
    default:
      connection()->defunct();
      set_error(CASS_ERROR_LIB_UNEXPECTED_RESPONSE, "Unexpected response");
      break;
  }
}

void SpeculativeExecution::on_error(CassError code, const std::string& message) {
  return_connection();

  // Handle recoverable errors by retrying with the next host
  if (code == CASS_ERROR_LIB_WRITE_ERROR ||
      code == CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE) {
    retry_next_host();
  } else {
    set_error(code, message);
  }
}

void SpeculativeExecution::on_retry(bool use_next_host) {
  return_connection();

  if (use_next_host) {
    retry_next_host();
  } else {
    retry_current_host();
  }
}

void SpeculativeExecution::on_cancel() {
  LOG_DEBUG("Cancelling speculative execution (%p) for request (%p) on host %s",
            static_cast<void*>(this),
            static_cast<void*>(request_handler_.get()),
            current_host_ ? current_host_->address_string().c_str()
                          : "<no current host>");
  return_connection();
}

void SpeculativeExecution::retry_current_host() {
  if (state() == REQUEST_STATE_CANCELLED) {
    return;
  }

  // Reset the request so it can be executed again
  set_state(REQUEST_STATE_NEW);
  pool_ = NULL;
  request_handler_->io_worker()->retry(RequestCallback::Ptr(this));
}

void SpeculativeExecution::retry_next_host() {
  next_host();
  retry_current_host();
}

void SpeculativeExecution::start_pending_request(Pool* pool, Timer::Callback cb) {
  pool_ = pool;
  pending_request_timer_.start(pool->loop(), pool->config().connect_timeout_ms(), this, cb);
}

void SpeculativeExecution::stop_pending_request() {
  pending_request_timer_.stop();
}

void SpeculativeExecution::execute() {
  if (request()->is_idempotent()) {
    request_handler_->schedule_next_execution(current_host_);
  }
  request_handler_->io_worker()->retry(RequestCallback::Ptr(this));

}

void SpeculativeExecution::schedule_next(int64_t timeout) {
  if (timeout > 0) {
    schedule_timer_.start(request_handler_->io_worker()->loop(), timeout, this, on_execute);
  } else {
    next_host();
    execute();
  }
}

void SpeculativeExecution::cancel() {
  schedule_timer_.stop();
  set_state(REQUEST_STATE_CANCELLED);
}

void SpeculativeExecution::on_result_response(ResponseMessage* response) {
  ResultResponse* result =
      static_cast<ResultResponse*>(response->response_body().get());

  switch (result->kind()) {
    case CASS_RESULT_KIND_ROWS:
      current_host_->update_latency(uv_hrtime() - start_time_ns_);

      // Execute statements with no metadata get their metadata from
      // result_metadata() returned when the statement was prepared.
      if (request()->opcode() == CQL_OPCODE_EXECUTE && result->no_metadata()) {
        const ExecuteRequest* execute = static_cast<const ExecuteRequest*>(request());
        if (!execute->skip_metadata()) {
          // Caused by a race condition in C* 2.1.0
          on_error(CASS_ERROR_LIB_UNEXPECTED_RESPONSE,
                   "Expected metadata but no metadata in response (see CASSANDRA-8054)");
          return;
        }
        result->set_metadata(execute->prepared()->result()->result_metadata().get());
      }
      set_response(response->response_body());
      break;

    case CASS_RESULT_KIND_SCHEMA_CHANGE: {
      SchemaChangeCallback::Ptr schema_change_handler(
            new SchemaChangeCallback(connection(),
                                     Ptr(this),
                                     response->response_body()));
      schema_change_handler->execute();
      break;
    }

    case CASS_RESULT_KIND_SET_KEYSPACE:
      request_handler_->io_worker()->broadcast_keyspace_change(result->keyspace().to_string());
      set_response(response->response_body());
      break;

    default:
      set_response(response->response_body());
      break;
  }
}

void SpeculativeExecution::on_error_response(ResponseMessage* response) {
  ErrorResponse* error =
      static_cast<ErrorResponse*>(response->response_body().get());

  RetryPolicy::RetryDecision decision = RetryPolicy::RetryDecision::return_error();


  switch(error->code()) {
    case CQL_ERROR_READ_TIMEOUT:
      decision =  request_handler_->retry_policy()->on_read_timeout(request(),
                                                                    error->consistency(),
                                                                    error->received(),
                                                                    error->required(),
                                                                    error->data_present() > 0,
                                                                    num_retries_);
      break;

    case CQL_ERROR_WRITE_TIMEOUT:
      if (request()->is_idempotent()) {
        decision =  request_handler_->retry_policy()->on_write_timeout(request(),
                                                                       error->consistency(),
                                                                       error->received(),
                                                                       error->required(),
                                                                       error->write_type(),
                                                                       num_retries_);
      }
      break;

    case CQL_ERROR_UNAVAILABLE:
      decision =  request_handler_->retry_policy()->on_unavailable(request(),
                                                                   error->consistency(),
                                                                   error->required(),
                                                                   error->received(),
                                                                   num_retries_);
      break;

    case CQL_ERROR_OVERLOADED:
      LOG_WARN("Host %s is overloaded.",
               connection()->address_string().c_str());
      if (request()->is_idempotent()) {
        decision = request_handler_->retry_policy()->on_request_error(request(),
                                                                      request()->consistency(),
                                                                      error,
                                                                      num_retries_);
      }
      break;

    case CQL_ERROR_SERVER_ERROR:
      LOG_WARN("Received server error '%s' from host %s. Defuncting the connection...",
               error->message().to_string().c_str(),
               connection()->address_string().c_str());
      connection()->defunct();
      if (request()->is_idempotent()) {
        decision = request_handler_->retry_policy()->on_request_error(request(),
                                                                      request()->consistency(),
                                                                      error,
                                                                      num_retries_);
      }
      break;

    case CQL_ERROR_IS_BOOTSTRAPPING:
      LOG_ERROR("Query sent to bootstrapping host %s. Retrying on the next host...",
                connection()->address_string().c_str());
      retry_next_host();
      return; // Done

    case CQL_ERROR_UNPREPARED:
      on_error_unprepared(error);
      return; // Done

    default:
      // Return the error response
      break;
  }

  // Process retry decision
  switch(decision.type()) {
    case RetryPolicy::RetryDecision::RETURN_ERROR:
      set_error_with_error_response(response->response_body(),
                                    static_cast<CassError>(CASS_ERROR(
                                                             CASS_ERROR_SOURCE_SERVER, error->code())),
                                    error->message().to_string());
      break;

    case RetryPolicy::RetryDecision::RETRY:
      set_consistency(decision.retry_consistency());
      if (decision.retry_current_host()) {
        retry_current_host();
      } else {
        retry_next_host();
      }
      num_retries_++;
      break;

    case RetryPolicy::RetryDecision::IGNORE:
      set_response(Response::Ptr(new ResultResponse()));
      break;
  }
}

void SpeculativeExecution::on_error_unprepared(ErrorResponse* error) {
  std::string prepared_statement;

  if (request()->opcode() == CQL_OPCODE_EXECUTE) {
    const ExecuteRequest* execute = static_cast<const ExecuteRequest*>(request());
    prepared_statement = execute->prepared()->statement();
  } else if (request()->opcode() == CQL_OPCODE_BATCH) {
    const BatchRequest* batch = static_cast<const BatchRequest*>(request());
    if (!batch->prepared_statement(error->prepared_id().to_string(), &prepared_statement)) {
      set_error(CASS_ERROR_LIB_UNEXPECTED_RESPONSE,
                "Unable to find prepared statement in batch statement");
      return;
    }
  } else {
    connection()->defunct();
    set_error(CASS_ERROR_LIB_UNEXPECTED_RESPONSE,
              "Received unprepared error for invalid "
              "request type or invalid prepared id");
    return;
  }

  if (!connection()->write(RequestCallback::Ptr(
                            new PrepareCallback(prepared_statement, this)))) {
    // Try to prepare on the same host but on a different connection
    retry_current_host();
  }
}

void SpeculativeExecution::return_connection() {
  if (pool_ != NULL && connection() != NULL) {
    pool_->return_connection(connection());
  }
}

bool SpeculativeExecution::is_host_up(const Address& address) const {
  return request_handler_->io_worker()->is_host_up(address);
}

void SpeculativeExecution::set_response(const Response::Ptr& response) {
  request_handler_->set_response(current_host_, response);
}

void SpeculativeExecution::set_error(CassError code, const std::string& message) {
  request_handler_->set_error(current_host_, code, message);
}

void SpeculativeExecution::set_error_with_error_response(const Response::Ptr& error,
                                                         CassError code, const std::string& message) {
  request_handler_->set_error_with_error_response(current_host_, error, code, message);
}

} // namespace cass
