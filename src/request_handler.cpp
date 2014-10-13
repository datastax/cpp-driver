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

#include "request_handler.hpp"

#include "connection.hpp"
#include "error_response.hpp"
#include "execute_request.hpp"
#include "io_worker.hpp"
#include "pool.hpp"
#include "prepare_handler.hpp"
#include "result_response.hpp"
#include "row.hpp"
#include "schema_change_handler.hpp"
#include "session.hpp"

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
    retry(RETRY_WITH_NEXT_HOST);
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

void RequestHandler::retry(RetryType type) {
  // Reset the request so it can be executed again
  set_state(REQUEST_STATE_NEW);
  pool_ = NULL;

  io_worker_->retry(this, type);
}

bool RequestHandler::get_current_host_address(Address* address) {
  if (is_query_plan_exhausted_) {
    return false;
  }
  *address = current_address_;
  return true;
}

void RequestHandler::next_host() {
  is_query_plan_exhausted_ = !query_plan_->compute_next(&current_address_);
}

bool RequestHandler::is_host_up(const Address& address) const {
  return io_worker_->is_host_up(address);
}

void RequestHandler::set_response(Response* response) {
  future_->set_result(current_address_, response);
  return_connection_and_finish();
}

void RequestHandler::set_error(CassError code, const std::string& message) {
  if (is_query_plan_exhausted_) {
    future_->set_error(code, message);
  } else {
    future_->set_error_with_host_address(current_address_, code, message);
  }
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
        result->set_metadata(execute->prepared()->result()->result_metadata().get());
      }
      set_response(response->response_body().release());
      break;

    case CASS_RESULT_KIND_SCHEMA_CHANGE: {
      SharedRefPtr<SchemaChangeHandler> schema_change_handler(
            new SchemaChangeHandler(connection_,
                                    this,
                                    response->response_body().release()));
      schema_change_handler->execute();
      break;
    }

    case CASS_RESULT_KIND_SET_KEYSPACE:
      io_worker_->broadcast_keyspace_change(result->keyspace());
      set_response(response->response_body().release());
      break;

    default:
      set_response(response->response_body().release());
      break;
  }
}

void RequestHandler::on_error_response(ResponseMessage* response) {
  ErrorResponse* error =
      static_cast<ErrorResponse*>(response->response_body().get());

  if (error->code() == CQL_ERROR_UNPREPARED) {
    ScopedRefPtr<PrepareHandler> prepare_handler(new PrepareHandler(this));

    if (prepare_handler->init(error->prepared_id())) {
      connection_->write(prepare_handler.get());
    } else {
      connection_->defunct();
      set_error(CASS_ERROR_LIB_UNEXPECTED_RESPONSE,
               "Received unprepared error for invalid "
               "request type or invalid prepared id");
    }
  } else {
    set_error(static_cast<CassError>(CASS_ERROR(
                                       CASS_ERROR_SOURCE_SERVER, error->code())),
              error->message());
  }
}

} // namespace cass
