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

CassSession* cass_session_new() {
  return CassSession::to(new cass::Session());
}

CassSession* cass_session_clone(const CassSession* session) {
  return CassSession::to(new cass::Session(session));
}

void
cass_session_free(
   CassSession* session) {
  delete session->from();
}

CassError
cass_session_setopt(
    CassSession* session,
    CassOption   option,
    const void* data,
    size_t      data_length) {
  return session->config_.option(option, data, data_length);
}

CassError
cass_session_getopt(
    const CassSession* session,
    CassOption   option,
    void**      data,
    size_t*     data_length)
{
  return CASS_OK;
}

CassError
cass_session_connect(
   CassSession* session,
   CassFuture** future) {
  cass::Future* session_future = session->connect("");
  *future = CassFuture::to(session_future);
  return CASS_OK;
}

CassError
cass_session_connect_keyspace(
   CassSession*  session,
    const char* keyspace,
    CassFuture** future) {
  cass::Future* session_future = session->connect(keyspace);
  *future = CassFuture::to(session_future);
  return CASS_OK;
}

CassError
cass_session_shutdown(
    CassSession* session,
    CassFuture** future) {
  cass::Future* shutdown_future = session->shutdown();
  *future = CassFuture::to(shutdown_future);
  return CASS_OK;
}


CassError
cass_session_prepare(
    CassSession* session,
    const char* statement,
    size_t      statement_length,
    CassFuture** output) {
  cass::Future* future = session->prepare(statement, statement_length);
  *output = CassFuture::to(future);
  return CASS_OK;
}

CassError
cass_session_exec(
    CassSession*   session,
    CassStatement* statement,
    CassFuture**   future) {
  *future = CassFuture::to(session->execute(statement->from()));
  return CASS_OK;
}

} // extern "C"
