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
  printf("%s: %s (%d)\n", message, cass_code_error_desc(err), err);
}

int
main() {
  CassSession* session = cass_session_new();
  CassFuture* session_future = NULL;
  /*CassFuture* shutdown_future = NULL;*/

  const char* cp1 = "127.0.0.1";
  cass_session_setopt(session, CASS_OPTION_CONTACT_POINT_ADD, cp1, strlen(cp1));

  session_future = cass_session_connect(session);

  cass_future_wait(session_future);
  cass_future_free(session_future);

  {
    CassString query = cass_string_init("SELECT * FROM system.local WHERE key = ?");
    CassString key = cass_string_init("local");

    CassStatement* statement = NULL;
    CassFuture* result_future = NULL;
    const CassResult* result = NULL;
    CassIterator* iterator = NULL;


    statement = cass_statement_new(query, 1, CASS_CONSISTENCY_ONE);
    cass_statement_bind_string(statement, 0, key);

    result_future = cass_session_execute(session, statement);

    cass_future_wait(result_future);
    result = cass_future_get_result(result_future);
    cass_future_free(result_future);

    iterator = cass_iterator_from_result(result);

    while(cass_iterator_next(iterator)) {
      CassIterator* tokens_iterator = NULL;
      const CassValue* tokens_value = NULL;
      const CassRow* row = cass_iterator_get_row(iterator);

      CassString key_string;
      cass_value_get_string(cass_row_get_column(row, 0), &key_string);
      printf("key: %.*s\n", (int)key_string.length, key_string.data);

      tokens_value = cass_row_get_column(row, 13);
      tokens_iterator = cass_iterator_from_collection(tokens_value);
      while(cass_iterator_next(tokens_iterator)) {
        CassString token_string;
        cass_value_get_string(cass_iterator_get_value(tokens_iterator), &token_string);
        printf("token: %.*s\n", (int)token_string.length, token_string.data);
      }
      cass_iterator_free(tokens_iterator);
    }

    cass_iterator_free(iterator);
    cass_result_free(result);
    cass_statement_free(statement);
  }

  cass_session_free(session);
}
