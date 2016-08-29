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

#include "connection.hpp"
#include "constants.hpp"
#include "execute_request.hpp"
#include "io_worker.hpp"
#include "pool.hpp"
#include "prepare_handler.hpp"
#include "result_response.hpp"
#include "row.hpp"
#include "schema_change_handler.hpp"
#include "session.hpp"

#include <uv.h>

namespace cass {

void RequestHandler::on_set(ResponseMessage* response) {
  assert(connection_ != NULL);
  assert(!is_query_plan_exhausted_ && "Tried to set on a non-existent host");
  switch (response->opcode()) {
    case CQL_OPCODE_RESULT:
      on_result_response(response);
      break;
    case CQL_OPCODE_ERROR:
      on_error_response(response);
      break;
    default:
      connection_->defunct();
      set_error(CASS_ERROR_LIB_UNEXPECTED_RESPONSE, "Unexpected response");
      break;
  }
}

void RequestHandler::on_error(CassError code, const std::string& message) {
  if (code == CASS_ERROR_LIB_WRITE_ERROR ||
      code == CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE) {
    next_host();
    retry();
    return_connection();
  } else {
    set_error(code, message);
  }
}

void RequestHandler::on_timeout() {
  assert(!is_query_plan_exhausted_ && "Tried to timeout on a non-existent host");
  set_error(CASS_ERROR_LIB_REQUEST_TIMED_OUT, "Request timed out");
}

void RequestHandler::set_io_worker(IOWorker* io_worker) {
  future_->set_loop(io_worker->loop());
  io_worker_ = io_worker;
}

void RequestHandler::retry() {
  // Reset the request so it can be executed again
  set_state(REQUEST_STATE_NEW);
  pool_ = NULL;
  io_worker_->retry(this);
}

bool RequestHandler::get_current_host_address(Address* address) {
  if (is_query_plan_exhausted_) {
    return false;
  }
  *address = current_host_->address();
  return true;
}

void RequestHandler::next_host() {
  current_host_ = query_plan_->compute_next();
  is_query_plan_exhausted_ = !current_host_;
}

bool RequestHandler::is_host_up(const Address& address) const {
  return io_worker_->is_host_up(address);
}

void RequestHandler::set_response(const SharedRefPtr<Response>& response) {
  uint64_t elapsed = uv_hrtime() - start_time_ns();
  current_host_->update_latency(elapsed);
  connection_->metrics()->record_request(elapsed);
  future_->set_response(current_host_->address(), response);
  return_connection_and_finish();
}

void RequestHandler::set_error(CassError code, const std::string& message) {
  if (is_query_plan_exhausted_) {
    future_->set_error(code, message);
  } else {
    future_->set_error_with_address(current_host_->address(), code, message);
  }
  return_connection_and_finish();
}

void RequestHandler::set_error_with_error_response(const SharedRefPtr<Response>& error,
                                                   CassError code, const std::string& message) {
  future_->set_error_with_response(current_host_->address(), error, code, message);
  return_connection_and_finish();
}

void RequestHandler::return_connection() {
  if (pool_ != NULL && connection_ != NULL) {
      pool_->return_connection(connection_);
  }
}

void RequestHandler::return_connection_and_finish() {
  return_connection();
  if (io_worker_ != NULL) {
    io_worker_->request_finished(this);
  }
  dec_ref();
}

void RequestHandler::on_result_response(ResponseMessage* response) {
  ResultResponse* result =
      static_cast<ResultResponse*>(response->response_body().get());
  switch (result->kind()) {
    case CASS_RESULT_KIND_ROWS:
      // Execute statements with no metadata get their metadata from
      // result_metadata() returned when the statement was prepared.
      if (request_->opcode() == CQL_OPCODE_EXECUTE && result->no_metadata()) {
        const ExecuteRequest* execute = static_cast<const ExecuteRequest*>(request_.get());
        if (!execute->skip_metadata()) {
          // Caused by a race condition in C* 2.1.0
          on_error(CASS_ERROR_LIB_UNEXPECTED_RESPONSE, "Expected metadata but no metadata in response (see CASSANDRA-8054)");
          return;
        }
        result->set_metadata(execute->prepared()->result()->result_metadata().get());
      }
      set_response(response->response_body());
      break;

    case CASS_RESULT_KIND_SCHEMA_CHANGE: {
      SharedRefPtr<SchemaChangeHandler> schema_change_handler(
            new SchemaChangeHandler(connection_,
                                    this,
                                    response->response_body()));
      schema_change_handler->execute();
      break;
    }

    case CASS_RESULT_KIND_SET_KEYSPACE:
      io_worker_->broadcast_keyspace_change(result->keyspace().to_string());
      set_response(response->response_body());
      break;

    default:
      set_response(response->response_body());
      break;
  }
}

void RequestHandler::on_error_response(ResponseMessage* response) {
  ErrorResponse* error =
      static_cast<ErrorResponse*>(response->response_body().get());


  switch(error->code()) {
    case CQL_ERROR_UNPREPARED:
      on_error_unprepared(error);
      break;

    case CQL_ERROR_READ_TIMEOUT:
      handle_retry_decision(response,
                            retry_policy_->on_read_timeout(error->consistency(),
                                                           error->received(),
                                                           error->required(),
                                                           error->data_present() > 0,
                                                           num_retries_));
      break;

    case CQL_ERROR_WRITE_TIMEOUT:
      handle_retry_decision(response,
                            retry_policy_->on_write_timeout(error->consistency(),
                                                            error->received(),
                                                            error->required(),
                                                            error->write_type(),
                                                            num_retries_));
      break;

    case CQL_ERROR_UNAVAILABLE:
      handle_retry_decision(response,
                            retry_policy_->on_unavailable(error->consistency(),
                                                          error->required(),
                                                          error->received(),
                                                          num_retries_));
      break;

    default:
      set_error(static_cast<CassError>(CASS_ERROR(
                                         CASS_ERROR_SOURCE_SERVER, error->code())),
                error->message().to_string());
      break;
  }
}

void RequestHandler::on_error_unprepared(ErrorResponse* error) {
  ScopedRefPtr<PrepareHandler> prepare_handler(new PrepareHandler(this));
  if (prepare_handler->init(error->prepared_id().to_string())) {
    if (!connection_->write(prepare_handler.get())) {
      // Try to prepare on the same host but on a different connection
      retry();
    }
  } else {
    connection_->defunct();
    set_error(CASS_ERROR_LIB_UNEXPECTED_RESPONSE,
              "Received unprepared error for invalid "
              "request type or invalid prepared id");
  }
}

void RequestHandler::handle_retry_decision(ResponseMessage* response,
                                           const RetryPolicy::RetryDecision& decision) {
  ErrorResponse* error =
      static_cast<ErrorResponse*>(response->response_body().get());

  switch(decision.type()) {
    case RetryPolicy::RetryDecision::RETURN_ERROR:
      set_error_with_error_response(response->response_body(),
                                    static_cast<CassError>(CASS_ERROR(
                                                             CASS_ERROR_SOURCE_SERVER, error->code())),
                                    error->message().to_string());
      break;

    case RetryPolicy::RetryDecision::RETRY:
      set_consistency(decision.retry_consistency());
      if (!decision.retry_current_host()) {
        next_host();
      }
      if (state() == REQUEST_STATE_DONE) {
        retry();
      } else {
        set_state(REQUEST_STATE_RETRY_WRITE_OUTSTANDING);
      }
      break;

    case RetryPolicy::RetryDecision::IGNORE:
      set_response(SharedRefPtr<Response>(new ResultResponse()));
      break;
  }
  num_retries_++;
}

} // namespace cass
