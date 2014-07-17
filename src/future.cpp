/*
  Copyright (c) 2014 DataStax

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

#include "types.hpp"
#include "future.hpp"
#include "scoped_ptr.hpp"
#include "request_handler.hpp"

extern "C" {

void cass_future_free(CassFuture* future) {
  // Futures can be deleted without being waited on
  // because they'll be cleaned up by the notifying thread
  future->release();
}

cass_bool_t cass_future_ready(CassFuture* future) {
  return static_cast<cass_bool_t>(future->ready());
}

void cass_future_wait(CassFuture* future) {
  future->wait();
}

cass_bool_t cass_future_wait_timed(CassFuture* future, cass_duration_t wait) {
  return static_cast<cass_bool_t>(future->wait_for(wait));
}

CassSession* cass_future_get_session(CassFuture* future) {
  if (future->type() != cass::CASS_FUTURE_TYPE_SESSION_CONNECT) {
    return NULL;
  }
  cass::SessionConnectFuture* connect_future =
      static_cast<cass::SessionConnectFuture*>(future->from());
  if (connect_future->is_error()) {
    return NULL;
  }
  return CassSession::to(connect_future->release_result());
}

const CassResult* cass_future_get_result(CassFuture* future) {
  if (future->type() != cass::CASS_FUTURE_TYPE_RESPONSE) {
    return NULL;
  }
  cass::ResponseFuture* response_future =
      static_cast<cass::ResponseFuture*>(future->from());
  if (response_future->is_error()) {
    return NULL;
  }
  return CassResult::to(
      static_cast<cass::ResultResponse*>(response_future->release_result()));
}

const CassPrepared* cass_future_get_prepared(CassFuture* future) {
  if (future->type() != cass::CASS_FUTURE_TYPE_RESPONSE) {
    return NULL;
  }
  cass::ResponseFuture* response_future =
      static_cast<cass::ResponseFuture*>(future->from());
  if (response_future->is_error()) {
    return NULL;
  }
  cass::ScopedPtr<cass::ResultResponse> result(
      static_cast<cass::ResultResponse*>(response_future->release_result()));
  if (result && result->kind() == CASS_RESULT_KIND_PREPARED) {
    cass::Prepared* prepared =
        new cass::Prepared(result->prepared(), response_future->statement);
    return CassPrepared::to(prepared);
  }
  return NULL;
}

CassError cass_future_error_code(CassFuture* future) {
  const cass::Future::Error* error = future->get_error();
  if (error != NULL) {
    return error->code;
  } else {
    return CASS_OK;
  }
}

CassString cass_future_error_message(CassFuture* future) {
  CassString str;
  const cass::Future::Error* error = future->get_error();
  if (error != NULL) {
    const std::string& message = error->message;
    str.data = message.data();
    str.length = message.size();
  } else {
    str.data = "";
    str.length = 0;
  }
  return str;
}

} // extern "C"
