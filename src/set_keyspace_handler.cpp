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

#include "set_keyspace_handler.hpp"

#include "connection.hpp"
#include "io_worker.hpp"
#include "prepare_request.hpp"
#include "query_request.hpp"
#include "request_handler.hpp"
#include "response.hpp"
#include "result_response.hpp"

namespace cass {

SetKeyspaceHandler::SetKeyspaceHandler(Connection* connection,
                                       const std::string& keyspace,
                                       Handler* handler)
    : connection_(connection)
    , request_(new QueryRequest())
    , handler_(handler) {
  request_->set_query("use \"" + keyspace + "\"");
}

void SetKeyspaceHandler::on_set(ResponseMessage* response) {
  switch (response->opcode()) {
    case CQL_OPCODE_RESULT:
      on_result_response(response);
      break;
    case CQL_OPCODE_ERROR:
      connection_->defunct();
      handler_->on_error(CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE,
                         "Unable to set keyspsace");
      break;
    default:
      break;
  }
}

void SetKeyspaceHandler::on_error(CassError code, const std::string& message) {
  connection_->defunct();
  handler_->on_error(CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE,
                     "Unable to set keyspsace");
}

void SetKeyspaceHandler::on_timeout() {
  // TODO(mpenick): What to do here?
  handler_->on_timeout();
}

void SetKeyspaceHandler::on_result_response(ResponseMessage* response) {
  ResultResponse* result =
      static_cast<ResultResponse*>(response->response_body().get());
  if (result->kind() == CASS_RESULT_KIND_SET_KEYSPACE) {
    connection_->write(handler_.get());
  } else {
    connection_->defunct();
    handler_->on_error(CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE,
                       "Unable to set keyspsace");
  }
}

} // namespace cass
