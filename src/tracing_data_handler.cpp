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

#include "tracing_data_handler.hpp"

#include "query_request.hpp"
#include "result_iterator.hpp"

using namespace datastax;
using namespace datastax::internal::core;

TracingDataHandler::TracingDataHandler(const RequestHandler::Ptr& request_handler,
                                       const Host::Ptr& current_host, const Response::Ptr& response,
                                       CassConsistency consistency, uint64_t max_wait_time_ms,
                                       uint64_t retry_wait_time_ms)
    : WaitForHandler(request_handler, current_host, response, max_wait_time_ms, retry_wait_time_ms)
    , consistency_(consistency) {}

ChainedRequestCallback::Ptr TracingDataHandler::callback() {
  WaitforRequestVec requests;
  QueryRequest::Ptr request(new QueryRequest(SELECT_TRACES_SESSION, 1));
  request->set_request_timeout_ms(request_timeout_ms());
  request->set_consistency(consistency_);
  request->set(0, response()->tracing_id());
  requests.push_back(WaitForRequest("session", request));
  return WaitForHandler::callback(requests);
}

bool TracingDataHandler::on_set(const ChainedRequestCallback::Ptr& callback) {
  ResultResponse::Ptr result(callback->result("session"));
  if (result && result->row_count() > 0) {
    LOG_DEBUG("Found tracing data in %llu ms",
              static_cast<unsigned long long>(get_time_since_epoch_ms() - start_time_ms()));
    return true;
  } else {
    LOG_DEBUG("Tracing data still not available. Trying again in %llu ms",
              static_cast<unsigned long long>(retry_wait_time_ms()));
    return false;
  }
}

void TracingDataHandler::on_error(WaitForHandler::WaitForError code, const String& message) {
  switch (code) {
    case WAIT_FOR_ERROR_REQUEST_ERROR:
      LOG_ERROR("An error occurred waiting for tracing data to become available: %s",
                message.c_str());
      break;
    case WAIT_FOR_ERROR_REQUEST_TIMEOUT:
      LOG_WARN("A query timeout occurred waiting for tracing data to become available");
      break;
    case WAIT_FOR_ERROR_CONNECTION_CLOSED:
      LOG_WARN("Connection closed while attempting to wait for tracing data to become available");
      break;
    case WAIT_FOR_ERROR_NO_STREAMS:
      LOG_WARN("No stream available when attempting to wait for tracing data to become available");
      break;
    case WAIT_FOR_ERROR_TIMEOUT:
      LOG_WARN("Tracing data not available after %llu ms",
               static_cast<unsigned long long>(max_wait_time_ms()));
      break;
  }
}
