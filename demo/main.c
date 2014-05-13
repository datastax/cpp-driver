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

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <uv.h>

#include <arpa/inet.h>

#include "cassandra.h"

void print_error(const char* message, int err) {
  printf("%s: %s (%d)\n", message, cass_error_desc(err), err);
}

int
main() {
  CassSession* session = cass_session_new();
  CassFuture* session_future = NULL;
  /*CassFuture* shutdown_future = NULL;*/
  int err;

  const char* query = "SELECT * FROM system.local WHERE key = ?";
  const char* key = "local";

  CassStatement* statement = NULL;
  CassFuture* result_future = NULL;
  CassResult* result = NULL;

  const char* cp1 = "127.0.0.1";
  cass_session_setopt(session, CASS_OPTION_CONTACT_POINT_ADD, cp1, strlen(cp1));

  /*
  const char* cp2 = "127.0.0.2";
  const char* cp3 = "127.0.0.3";
  cass_session_setopt(session, CASS_OPTION_CONTACT_POINT_ADD, cp2, strlen(cp2));
  cass_session_setopt(session, CASS_OPTION_CONTACT_POINT_ADD, cp3, strlen(cp3));
  */

  err = cass_session_connect(session, &session_future);
  if (err != 0) {
    print_error("Error creating session", err);
    goto cleanup;
  }

  cass_future_wait(session_future);
  cass_future_free(session_future);

  cass_session_query(session, query, strlen(query), 1, CASS_CONSISTENCY_ONE, &statement);
  cass_statement_bind_string(statement, 0, key, strlen(key));
  /*cass_statement_bind_inet(statement, 0, (const uint8_t*)&address, 4);*/
  cass_session_exec(session, statement, &result_future);

  cass_future_wait(result_future);
  result = cass_future_get_result(result_future);

  /*
  CassIterator* iterator = NULL;

  cass_result_iterator_new(result, &iterator);

  while(cass_iterator_next(iterator)) {
    CassRow* row = NULL;
    cass_result_iterator_get(iterator, &row);
  }

  err = cass_session_shutdown(session, &shutdown_future);
  if(err != 0) {
    print_error("Error on shutdown", err);
    goto cleanup;
  }

  cass_future_wait(shutdown_future);
  cass_future_free(shutdown_future);

*/
cleanup:
  cass_session_free(session);
}
