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
   cass_future_t* future) {
  // TODO(mpenick): We can't do this because the memory could still be in use by an internal thread
  // This needs to be referenced counted
  delete future->from();
}

int
cass_future_ready(
   cass_future_t* future) {
  return static_cast<int>(future->ready());
}

void
cass_future_wait(
   cass_future_t* future) {
  future->wait();
}

cass_bool_t
cass_future_wait_timed(
   cass_future_t* future,
    size_t     wait) {
  return static_cast<cass_bool_t>(future->wait_for(wait));
}

cass_session_t*
cass_future_get_session(
   cass_future_t* future) {
  cass::SessionFuture* session_future = static_cast<cass::SessionFuture*>(future->from());
  const cass::Future::ResultOrError& result_or_error = session_future->get();
  if(result_or_error.is_error()) {
    return nullptr;
  }
  return cass_session_t::to(session_future->session);
}

const cass_result_t*
cass_future_get_result(
   cass_future_t* future) {
  cass::RequestFuture* request_future = static_cast<cass::RequestFuture*>(future->from());
  cass::Future::ResultOrError& result_or_error = request_future->get();
  if(result_or_error.is_error()) {
    return nullptr;
  }
  return cass_result_t::to(static_cast<cass::Result*>(result_or_error.release()));
}

const cass_prepared_t*
cass_future_get_prepared(
   cass_future_t* future) {
  cass::RequestFuture* request_future = static_cast<cass::RequestFuture*>(future->from());
  cass::Future::ResultOrError& result_or_error = request_future->get();
  if(result_or_error.is_error()) {
    return nullptr;
  }
  cass::Result* result = static_cast<cass::Result*>(result_or_error.release());
  if(result != nullptr) {
    cass::Prepared* prepared = new cass::Prepared(std::string(result->prepared,
                                                              result->prepared_size));
    delete result;
    return cass_prepared_t::to(prepared);
  }
  return nullptr;
}

const char*
cass_future_error_string(cass_future_t* future) {
  cass::Future::ResultOrError& result_or_error = future->get();
  if(result_or_error.is_error()) {
    return result_or_error.error()->message.c_str();
  } else {
    return "";
  }
}


cass_source_t
cass_future_error_source(cass_future_t* future) {
  const cass::Future::ResultOrError& result_or_error = future->get();
  if(result_or_error.is_error()) {
    return result_or_error.error()->source;
  } else {
    return CASS_ERROR_SOURCE_NONE;
  }
}

cass_code_t
cass_future_error_code(cass_future_t* future) {
  const cass::Future::ResultOrError& result_or_error = future->get();
  if(result_or_error.is_error()) {
    return result_or_error.error()->code;
  } else {
    return CASS_OK;
  }
}

} // extern "C"

