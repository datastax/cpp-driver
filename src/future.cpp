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

int
cass_future_wait_timed(
   cass_future_t* future,
    size_t     wait) {
  return static_cast<int>(future->wait_for(wait));
}

cass_session_t*
cass_future_get_session(
   cass_future_t* future) {
  cass::SessionFutureImpl* session_future = static_cast<cass::SessionFutureImpl*>(future->from());
  if(session_future->error) {
    return nullptr;
  }
  return cass_session_t::to(session_future->result);
}

const cass_result_t*
cass_future_get_result(
   cass_future_t* future) {
  cass::MessageFutureImpl* message_future = static_cast<cass::MessageFutureImpl*>(future->from());
  if(message_future->error) {
    return nullptr;
  }
  cass::Result* result = static_cast<cass::Result*>(message_future->result->body.release());
  return cass_result_t::to(result);
}

const cass_prepared_t*
cass_future_get_prepared(
   cass_future_t* future) {
  cass::MessageFutureImpl* message_future = static_cast<cass::MessageFutureImpl*>(future->from());
  if(message_future->error) {
    return nullptr;
  }
  cass::Result* result = static_cast<cass::Result*>(message_future->result->body.get());
  return cass_prepared_t::to(new cass::Prepared(std::string(result->prepared,
                                                         result->prepared_size)));
}

void
cass_future_error_string(
    cass_future_t* future,
    char*      output,
    size_t     output_len,
    size_t*    total) {
  *total = future->error->message.copy(output, output_len);
}

cass_source_t
cass_future_error_source(
    cass_future_t* future) {
  return future->error->source;
}

cass_code_t
cass_future_error_code(
    cass_future_t* future) {
  return future->error->code;
}

