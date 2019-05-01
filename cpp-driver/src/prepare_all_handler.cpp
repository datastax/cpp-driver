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

#include "prepare_all_handler.hpp"

#include "connection.hpp"

using namespace datastax;
using namespace datastax::internal::core;

PrepareAllHandler::PrepareAllHandler(const Host::Ptr& current_host, const Response::Ptr& response,
                                     const RequestHandler::Ptr& request_handler, int remaining)
    : current_host_(current_host)
    , response_(response)
    , request_handler_(request_handler)
    , remaining_(remaining) {
  assert(remaining > 0);
}

void PrepareAllHandler::finish() {
  if (remaining_.fetch_sub(1) - 1 == 0) { // The last request sets the response on the future.
    request_handler_->set_response(current_host_, response_);
  }
}

PrepareAllCallback::PrepareAllCallback(const Address& address,
                                       const PrepareAllHandler::Ptr& handler)
    : SimpleRequestCallback(handler->wrapper())
    , address_(address)
    , handler_(handler)
    , is_finished_(false) {}

PrepareAllCallback::~PrepareAllCallback() { finish(); }

void PrepareAllCallback::finish() {
  // Only finish the callback one time. This can happen if the request times out.
  if (!is_finished_) {
    handler_->finish();
    is_finished_ = true;
  }
}

void PrepareAllCallback::on_internal_set(ResponseMessage* response) {
  if (!is_finished_) { // The request hasn't timed out
    LOG_DEBUG("Successfully prepared all on host %s", address_.to_string().c_str());
  }
}

void PrepareAllCallback::on_internal_error(CassError code, const String& message) {
  if (!is_finished_) { // The request hasn't timed out
    LOG_WARN("Failed to prepare all on host %s with error: '%s'", address_.to_string().c_str(),
             message.c_str());
  }
}

void PrepareAllCallback::on_internal_timeout() {
  LOG_WARN("Prepare all timed out on host %s", address_.to_string().c_str());
  finish(); // Don't wait for the request to come back
}
