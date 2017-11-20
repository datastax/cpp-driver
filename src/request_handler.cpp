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

#include "request_handler.hpp"

#include "batch_request.hpp"
#include "connection.hpp"
#include "constants.hpp"
#include "error_response.hpp"
#include "execute_request.hpp"
#include "io_worker.hpp"
#include "pool.hpp"
#include "prepare_request.hpp"
#include "protocol.hpp"
#include "response.hpp"
#include "result_response.hpp"
#include "row.hpp"
#include "schema_change_callback.hpp"
#include "session.hpp"

#include <uv.h>

namespace cass {

class PrepareCallback : public SimpleRequestCallback {
public:
  PrepareCallback(const std::string& query, RequestExecution* request_execution);

private:
  class PrepareRequest : public cass::PrepareRequest {
  public:
    PrepareRequest(const std::string& query,
                   const std::string& keyspace,
                   uint64_t request_timeout_ms)
      : cass::PrepareRequest(query) {
      set_keyspace(keyspace);
      set_request_timeout_ms(request_timeout_ms);
    }
  };

private:
  virtual void on_internal_set(ResponseMessage* response);
  virtual void on_internal_error(CassError code, const std::string& message);
  virtual void on_internal_timeout();

private:
  RequestExecution::Ptr request_execution_;
};

PrepareCallback::PrepareCallback(const std::string& query, RequestExecution* request_execution)
  : SimpleRequestCallback(
      Request::ConstPtr(
        new PrepareRequest(query,
                           request_execution->request()->keyspace(),
                           request_execution->request_timeout_ms())))
  , request_execution_(request_execution) { }

void PrepareCallback::on_internal_set(ResponseMessage* response) {
  switch (response->opcode()) {
    case CQL_OPCODE_RESULT: {
      ResultResponse* result =
          static_cast<ResultResponse*>(response->response_body().get());
      if (result->kind() == CASS_RESULT_KIND_PREPARED) {
        request_execution_->on_result_metadata_changed(request(), result);
        request_execution_->on_retry_current_host();
      } else {
        request_execution_->on_retry_next_host();
      }
    } break;
    case CQL_OPCODE_ERROR:
      request_execution_->on_retry_next_host();
      break;
    default:
      break;
  }
}

void PrepareCallback::on_internal_error(CassError code, const std::string& message) {
  request_execution_->on_retry_next_host();
}

void PrepareCallback::on_internal_timeout() {
  request_execution_->on_retry_next_host();
}

void RequestHandler::add_execution(RequestExecution* request_execution) {
  running_executions_++;
  request_execution->inc_ref();
  request_executions_.push_back(request_execution);
}

void RequestHandler::add_attempted_address(const Address& address) {
  future_->add_attempted_address(address);
}

void RequestHandler::schedule_next_execution(const Host::Ptr& current_host) {
  int64_t timeout = execution_plan_->next_execution(current_host);
  if (timeout >= 0) {
    RequestExecution::Ptr request_execution(
          new RequestExecution(RequestHandler::Ptr(this)));
    request_execution->schedule_next(timeout);
  }
}

void RequestHandler::init(const Config& config, const std::string& connected_keyspace,
                          const TokenMap* token_map, const PreparedMetadata& prepared_metdata) {
  wrapper_.init(config, prepared_metdata);

  // Attempt to use the statement's keyspace first then if not set then use the session's keyspace
  const std::string& keyspace(!request()->keyspace().empty() ? request()->keyspace() : connected_keyspace);

  query_plan_.reset(config.load_balancing_policy()->new_query_plan(keyspace, this, token_map));
  execution_plan_.reset(config.speculative_execution_policy()->new_plan(keyspace, wrapper_.request().get()));
}

void RequestHandler::start_request(IOWorker* io_worker) {
  io_worker_ = io_worker;
  uint64_t request_timeout_ms = wrapper_.request_timeout_ms();
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
  for (RequestExecutionVec::const_iterator i = request_executions_.begin(),
       end = request_executions_.end(); i != end; ++i) {
    RequestExecution* request_execution = *i;
    request_execution->cancel();
    request_execution->dec_ref();
  }
  if (io_worker_ != NULL) {
    io_worker_->request_finished();
  }
}

RequestExecution::RequestExecution(const RequestHandler::Ptr& request_handler,
                                   const Host::Ptr& current_host)
  : RequestCallback(request_handler->wrapper())
  , request_handler_(request_handler)
  , current_host_(current_host)
  , num_retries_(0)
  , start_time_ns_(0) {
  request_handler_->add_execution(this);
}

void RequestExecution::on_retry_current_host() {
  retry_current_host();
}

void RequestExecution::on_retry_next_host() {
  retry_next_host();
}

void RequestExecution::retry_current_host() {
  if (state() == REQUEST_STATE_CANCELLED) {
    return;
  }

  // Reset the request so it can be executed again
  set_state(REQUEST_STATE_NEW);
  request_handler_->io_worker()->retry(RequestExecution::Ptr(this));
}

void RequestExecution::retry_next_host() {
  next_host();
  retry_current_host();
}

void RequestExecution::on_execute(Timer* timer) {
  RequestExecution* request_execution = static_cast<RequestExecution*>(timer->data());
  request_execution->next_host();
  request_execution->execute();
}

void RequestExecution::on_start() {
  assert(current_host_ && "Tried to start on a non-existent host");
  if (request()->record_attempted_addresses()) {
    request_handler_->add_attempted_address(current_host_->address());
  }
  start_time_ns_ = uv_hrtime();
}

void RequestExecution::on_set(ResponseMessage* response) {
  assert(connection() != NULL);
  assert(current_host_ && "Tried to set on a non-existent host");

  switch (response->opcode()) {
    case CQL_OPCODE_RESULT:
      on_result_response(connection(), response);
      break;
    case CQL_OPCODE_ERROR:
      on_error_response(connection(), response);
      break;
    default:
      connection()->defunct();
      set_error(CASS_ERROR_LIB_UNEXPECTED_RESPONSE, "Unexpected response");
      break;
  }
}

void RequestExecution::on_error(CassError code, const std::string& message) {
  // Handle recoverable errors by retrying with the next host
  if (code == CASS_ERROR_LIB_WRITE_ERROR ||
      code == CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE) {
    retry_next_host();
  } else {
    set_error(code, message);
  }
}

void RequestExecution::on_result_metadata_changed(const Request* request,
                                                  ResultResponse* result_response) {
  if (request_handler_->listener_) {
    // Attempt to use the per-query keyspace first (v5+/DSEv2+ only) then
    // the keyspace in the result metadata.
    std::string keyspace;
    if (supports_set_keyspace(result_response->protocol_version()) &&
        !request->keyspace().empty()) {
      keyspace = request->keyspace();
    } else {
      keyspace = result_response->keyspace().to_string();
    }

    if (request->opcode() == CQL_OPCODE_EXECUTE &&
        result_response->kind() == CASS_RESULT_KIND_ROWS) {
      const ExecuteRequest* execute = static_cast<const ExecuteRequest*>(request);
      request_handler_->listener_->on_result_metadata_changed(execute->prepared()->id(),
                                                              execute->prepared()->query(),
                                                              keyspace,
                                                              result_response->new_metadata_id().to_string(),
                                                              ResultResponse::ConstPtr(result_response));
    } else if (request->opcode()  == CQL_OPCODE_PREPARE &&
               result_response->kind() == CASS_RESULT_KIND_PREPARED) {
      const PrepareRequest* prepare = static_cast<const PrepareRequest*>(request);
      request_handler_->listener_->on_result_metadata_changed(result_response->prepared_id().to_string(),
                                                              prepare->query(),
                                                              keyspace,
                                                              result_response->result_metadata_id().to_string(),
                                                              ResultResponse::ConstPtr(result_response));
    } else {
      assert (false && "Invalid response type for a result metadata change");
    }
  }
}

void RequestExecution::on_cancel() {
  LOG_DEBUG("Cancelling speculative execution (%p) for request (%p) on host %s",
            static_cast<void*>(this),
            static_cast<void*>(request_handler_.get()),
            current_host_ ? current_host_->address_string().c_str()
                          : "<no current host>");
}

void RequestExecution::execute() {
  if (request()->is_idempotent()) {
    request_handler_->schedule_next_execution(current_host_);
  }
  request_handler_->io_worker()->retry(RequestExecution::Ptr(this));

}

void RequestExecution::schedule_next(int64_t timeout) {
  if (timeout > 0) {
    schedule_timer_.start(request_handler_->io_worker()->loop(), timeout, this, on_execute);
  } else {
    next_host();
    execute();
  }
}

void RequestExecution::cancel() {
  schedule_timer_.stop();
  set_state(REQUEST_STATE_CANCELLED);
}

void RequestExecution::on_result_response(Connection* connection, ResponseMessage* response) {
  ResultResponse* result =
      static_cast<ResultResponse*>(response->response_body().get());

  switch (result->kind()) {
    case CASS_RESULT_KIND_ROWS:
      current_host_->update_latency(uv_hrtime() - start_time_ns_);

      // Execute statements with no metadata get their metadata from
      // result_metadata() returned when the statement was prepared.
      if (request()->opcode() == CQL_OPCODE_EXECUTE) {
        if (result->no_metadata()) {
          if (!skip_metadata()) {
            // Caused by a race condition in C* 2.1.0
            on_error(CASS_ERROR_LIB_UNEXPECTED_RESPONSE,
                     "Expected metadata but no metadata in response (see CASSANDRA-8054)");
            return;
          }
          result->set_metadata(prepared_metadata_entry()->result()->result_metadata().get());
        } else if (result->metadata_changed()) {
          on_result_metadata_changed(request(), result);
        }
      }
      set_response(response->response_body());
      break;

    case CASS_RESULT_KIND_SCHEMA_CHANGE: {
      SchemaChangeCallback::Ptr schema_change_handler(
            new SchemaChangeCallback(connection,
                                     Ptr(this),
                                     response->response_body()));
      schema_change_handler->execute();
      break;
    }

    case CASS_RESULT_KIND_SET_KEYSPACE:
      request_handler_->io_worker()->broadcast_keyspace_change(result->keyspace().to_string());
      set_response(response->response_body());
      break;


    case CASS_RESULT_KIND_PREPARED:
      on_result_metadata_changed(request(), result);
      if (!request_handler_->io_worker()->prepare_all(current_host(),
                                                      response->response_body(),
                                                      request_handler_)) {
        set_response(response->response_body());
      }
      break;

    default:
      set_response(response->response_body());
      break;
  }
}

void RequestExecution::on_error_response(Connection* connection, ResponseMessage* response) {
  ErrorResponse* error =
      static_cast<ErrorResponse*>(response->response_body().get());

  RetryPolicy::RetryDecision decision = RetryPolicy::RetryDecision::return_error();

  switch(error->code()) {
    case CQL_ERROR_READ_TIMEOUT:
      if (retry_policy()) {
        decision =  retry_policy()->on_read_timeout(request(),
                                                    error->consistency(),
                                                    error->received(),
                                                    error->required(),
                                                    error->data_present() > 0,
                                                    num_retries_);
      }
      break;

    case CQL_ERROR_WRITE_TIMEOUT:
      if (retry_policy() && request()->is_idempotent()) {
        decision = retry_policy()->on_write_timeout(request(),
                                                    error->consistency(),
                                                    error->received(),
                                                    error->required(),
                                                    error->write_type(),
                                                    num_retries_);
      }
      break;

    case CQL_ERROR_UNAVAILABLE:
      if (retry_policy()) {
        decision =  retry_policy()->on_unavailable(request(),
                                                   error->consistency(),
                                                   error->required(),
                                                   error->received(),
                                                   num_retries_);
      }
      break;

    case CQL_ERROR_OVERLOADED:
      LOG_WARN("Host %s is overloaded.",
               connection->address_string().c_str());
      if (retry_policy() && request()->is_idempotent()) {
        decision = retry_policy()->on_request_error(request(),
                                                    consistency(),
                                                    error,
                                                    num_retries_);
      }
      break;

    case CQL_ERROR_SERVER_ERROR:
      LOG_WARN("Received server error '%s' from host %s. Defuncting the connection...",
               error->message().to_string().c_str(),
               connection->address_string().c_str());
      connection->defunct();
      if (retry_policy() && request()->is_idempotent()) {
        decision = retry_policy()->on_request_error(request(),
                                                    consistency(),
                                                    error,
                                                    num_retries_);
      }
      break;

    case CQL_ERROR_IS_BOOTSTRAPPING:
      LOG_ERROR("Query sent to bootstrapping host %s. Retrying on the next host...",
                connection->address_string().c_str());
      retry_next_host();
      return; // Done

    case CQL_ERROR_UNPREPARED:
      on_error_unprepared(connection, error);
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
      set_retry_consistency(decision.retry_consistency());
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

void RequestExecution::on_error_unprepared(Connection* connection, ErrorResponse* error) {
  std::string query;

  if (request()->opcode() == CQL_OPCODE_EXECUTE) {
    const ExecuteRequest* execute = static_cast<const ExecuteRequest*>(request());
    query = execute->prepared()->query();
  } else if (request()->opcode() == CQL_OPCODE_BATCH) {
    const BatchRequest* batch = static_cast<const BatchRequest*>(request());
    if (!batch->find_prepared_query(error->prepared_id().to_string(), &query)) {
      set_error(CASS_ERROR_LIB_UNEXPECTED_RESPONSE,
                "Unable to find prepared statement in batch statement");
      return;
    }
  } else {
    connection->defunct();
    set_error(CASS_ERROR_LIB_UNEXPECTED_RESPONSE,
              "Received unprepared error for invalid "
              "request type or invalid prepared id");
    return;
  }

  if (!connection->write(RequestCallback::Ptr(
                           new PrepareCallback(query, this)))) {
    // Try to prepare on the same host but on a different connection
    retry_current_host();
  }
}

bool RequestExecution::is_host_up(const Address& address) const {
  return request_handler_->io_worker()->is_host_up(address);
}

void RequestExecution::set_response(const Response::Ptr& response) {
  request_handler_->set_response(current_host_, response);
}

void RequestExecution::set_error(CassError code, const std::string& message) {
  request_handler_->set_error(current_host_, code, message);
}

void RequestExecution::set_error_with_error_response(const Response::Ptr& error,
                                                     CassError code, const std::string& message) {
  request_handler_->set_error_with_error_response(current_host_, error, code, message);
}

} // namespace cass
