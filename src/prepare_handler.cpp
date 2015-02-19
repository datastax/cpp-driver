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

#include "prepare_handler.hpp"

#include "batch_request.hpp"
#include "constants.hpp"
#include "error_response.hpp"
#include "execute_request.hpp"
#include "prepare_request.hpp"
#include "request_handler.hpp"
#include "response.hpp"
#include "result_response.hpp"

namespace cass {

bool PrepareHandler::init(const std::string& prepared_id) {
  PrepareRequest* prepare =
      static_cast<PrepareRequest*>(new PrepareRequest());
  request_.reset(prepare);
  if (request_handler_->request()->opcode() == CQL_OPCODE_EXECUTE) {
    const ExecuteRequest* execute = static_cast<const ExecuteRequest*>(
        request_handler_->request());
    prepare->set_query(execute->prepared()->statement());
    return true;
  } else if (request_handler_->request()->opcode() == CQL_OPCODE_BATCH) {
    const BatchRequest* batch = static_cast<const BatchRequest*>(
        request_handler_->request());
    std::string prepared_statement;
    if (batch->prepared_statement(prepared_id, &prepared_statement)) {
      prepare->set_query(prepared_statement);
      return true;
    }
  }
  return false; // Invalid request type
}

void PrepareHandler::on_set(ResponseMessage* response) {
  switch (response->opcode()) {
    case CQL_OPCODE_RESULT: {
      ResultResponse* result =
          static_cast<ResultResponse*>(response->response_body().get());
      if (result->kind() == CASS_RESULT_KIND_PREPARED) {
        request_handler_->retry(RETRY_WITH_CURRENT_HOST);
      } else {
        request_handler_->retry(RETRY_WITH_NEXT_HOST);
      }
    } break;
    case CQL_OPCODE_ERROR:
      request_handler_->retry(RETRY_WITH_NEXT_HOST);
      break;
    default:
      break;
  }
}

void PrepareHandler::on_error(CassError code, const std::string& message) {
  request_handler_->retry(RETRY_WITH_NEXT_HOST);
}

void PrepareHandler::on_timeout() {
  request_handler_->retry(RETRY_WITH_NEXT_HOST);
}

} // namespace cass
