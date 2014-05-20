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

#include "cassandra.hpp"
#include "future.hpp"

extern "C" {

void
cass_future_free(
   CassFuture* future) {
  // TODO(mpenick): We can't do this because the memory could still be in use by an internal thread
  // This needs to be referenced counted
  delete future->from();
}

int
cass_future_ready(
   CassFuture* future) {
  return static_cast<int>(future->ready());
}

void
cass_future_wait(
   CassFuture* future) {
  future->wait();
}

cass_bool_t
cass_future_wait_timed(
   CassFuture* future,
   cass_duration_t wait) {
  return static_cast<cass_bool_t>(future->wait_for(wait));
}

CassSession*
cass_future_get_session(
   CassFuture* future) {
  cass::SessionFuture* session_future = static_cast<cass::SessionFuture*>(future->from());
  const cass::Future::ResultOrError* result_or_error = session_future->get();
  if(result_or_error->is_error()) {
    return nullptr;
  }
  return CassSession::to(session_future->session);
}

const CassResult*
cass_future_get_result(
   CassFuture* future) {
  cass::RequestFuture* request_future = static_cast<cass::RequestFuture*>(future->from());
  cass::Future::ResultOrError* result_or_error = request_future->get();
  if(result_or_error->is_error()) {
    return nullptr;
  }
  return CassResult::to(static_cast<cass::Result*>(result_or_error->release()));
}

const CassPrepared*
cass_future_get_prepared(
   CassFuture* future) {
  cass::RequestFuture* request_future = static_cast<cass::RequestFuture*>(future->from());
  cass::Future::ResultOrError* result_or_error = request_future->get();
  if(result_or_error->is_error()) {
    return nullptr;
  }
  cass::Result* result = static_cast<cass::Result*>(result_or_error->release());
  if(result != nullptr) {
    cass::Prepared* prepared = new cass::Prepared(std::string(result->prepared,
                                                              result->prepared_size));
    delete result;
    return CassPrepared::to(prepared);
  }
  return nullptr;
}

const char*
cass_future_error_string(CassFuture* future) {
  const cass::Future::ResultOrError* result_or_error = future->get();
  if(result_or_error->is_error()) {
    return result_or_error->error()->message.c_str();
  } else {
    return "";
  }
}


CassErrorSource
cass_future_error_source(CassFuture* future) {
  const cass::Future::ResultOrError* result_or_error = future->get();
  if(result_or_error->is_error()) {
    return result_or_error->error()->source;
  } else {
    return CASS_ERROR_SOURCE_NONE;
  }
}

CassError
cass_future_error_code(CassFuture* future) {
  const cass::Future::ResultOrError* result_or_error = future->get();
  if(result_or_error->is_error()) {
    return result_or_error->error()->code;
  } else {
    return CASS_OK;
  }
}

} // extern "C"

