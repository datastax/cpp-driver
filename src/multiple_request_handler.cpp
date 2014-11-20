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

#include "multiple_request_handler.hpp"

#include "connection.hpp"
#include "query_request.hpp"

namespace cass {

MultipleRequestHandler::~MultipleRequestHandler() {
  for (ResponseVec::iterator it = responses_.begin(),
       end = responses_.end(); it != end; ++it) {
    delete (*it);
  }
}

void MultipleRequestHandler::execute_query(const std::string& query) {
  if (has_errors_or_timeouts_) return;
  responses_.push_back(NULL);
  SharedRefPtr<InternalHandler> handler(new InternalHandler(this, new QueryRequest(query), remaining_++));
  if (!connection_->write(handler.get())) {
    on_error(CASS_ERROR_LIB_NO_STREAMS, "No more streams available");
  }
}

void MultipleRequestHandler::InternalHandler::on_set(ResponseMessage* response) {
  parent_->responses_[index_] = response->response_body().release();
  if (--parent_->remaining_ == 0 && !parent_->has_errors_or_timeouts_) {
    parent_->on_set(parent_->responses_);
  }
}

void MultipleRequestHandler::InternalHandler::on_error(CassError code, const std::string& message) {
  if (!parent_->has_errors_or_timeouts_) {
    parent_->on_error(code, message);
  }
  parent_->has_errors_or_timeouts_ = true;
}

void MultipleRequestHandler::InternalHandler::on_timeout() {
  if (!parent_->has_errors_or_timeouts_) {
    parent_->on_timeout();
  }
  parent_->has_errors_or_timeouts_ = true;
}

} // namespace cass
