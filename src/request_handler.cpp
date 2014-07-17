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

#include "error_response.hpp"
#include "request_handler.hpp"
#include "response.hpp"


namespace cass {

void RequestHandler::on_set(ResponseMessage* response) {
  assert(!hosts.empty() && "Tried to set on an non-existent host");
  switch (response->opcode()) {
    case CQL_OPCODE_RESULT:
      future_->set_result(hosts.front(), response->response_body().release());
      break;
    case CQL_OPCODE_ERROR: {
      ErrorResponse* error =
          static_cast<ErrorResponse*>(response->response_body().get());
      future_->set_error_with_host(hosts.front(),
                                   static_cast<CassError>(CASS_ERROR(
                                                            CASS_ERROR_SOURCE_SERVER, error->code())),
                                   error->message());
    } break;
    default:
      // TODO(mpenick): Get the host for errors
      future_->set_error_with_host(hosts.front(),
                                   CASS_ERROR_LIB_UNEXPECTED_RESPONSE,
                                   "Unexpected response");
      break;
  }
  notify_finished();
}

void RequestHandler::on_error(CassError code, const std::string& message) {
  if (hosts.empty()) {
    future_->set_error(code, message);
  } else {
    future_->set_error_with_host(hosts.front(), code, message);
  }
  notify_finished();
}

void RequestHandler::on_timeout() {
  assert(!hosts.empty() && "Tried to timeout on an non-existent host");
  future_->set_error_with_host(hosts.front(), CASS_ERROR_LIB_REQUEST_TIMED_OUT, "Request timed out");
  notify_finished();
}

} // namespace cass
