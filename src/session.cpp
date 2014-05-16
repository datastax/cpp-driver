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
#include "session.hpp"

extern "C" {

cass_session_t* cass_session_new() {
  return cass_session_t::to(new cass::Session());
}

cass_session_t* cass_session_clone(const cass_session_t* session) {
  return cass_session_t::to(new cass::Session(session));
}

void
cass_session_free(
   cass_session_t* session) {
  delete session->from();
}

cass_code_t
cass_session_setopt(
    cass_session_t* session,
    cass_option_t   option,
    const void* data,
    size_t      data_length) {
  return session->config_.option(option, data, data_length);
}

cass_code_t
cass_session_getopt(
    const cass_session_t* session,
    cass_option_t   option,
    void**      data,
    size_t*     data_length)
{
  return CASS_OK;
}

cass_code_t
cass_session_connect(
   cass_session_t* session,
   cass_future_t** future) {
  cass::Future* session_future = session->connect("");
  *future = cass_future_t::to(session_future);
  return CASS_OK;
}

cass_code_t
cass_session_connect_keyspace(
   cass_session_t*  session,
    const char* keyspace,
    cass_future_t** future) {
  cass::Future* session_future = session->connect(keyspace);
  *future = cass_future_t::to(session_future);
  return CASS_OK;
}

cass_code_t
cass_session_shutdown(
    cass_session_t* session,
    cass_future_t** future) {
  cass::Future* shutdown_future = session->shutdown();
  *future = cass_future_t::to(shutdown_future);
  return CASS_OK;
}


cass_code_t
cass_session_prepare(
    cass_session_t* session,
    const char* statement,
    size_t      statement_length,
    cass_future_t** output) {
  cass::Future* future = session->prepare(statement, statement_length);
  *output = cass_future_t::to(future);
  return CASS_OK;
}

cass_code_t
cass_session_exec(
    cass_session_t*   session,
    cass_statement_t* statement,
    cass_future_t**   future) {
  *future = cass_future_t::to(session->execute(statement->from()));
  return CASS_OK;
}

} // extern "C"
