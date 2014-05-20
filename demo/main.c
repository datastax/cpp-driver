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
  int err;


  const char* cp1 = "127.0.0.1";
  cass_session_setopt(session, CASS_OPTION_CONTACT_POINT_ADD, cp1, strlen(cp1));

  err = cass_session_connect(session, &session_future);
  if (err != 0) {
    print_error("Error creating session", err);
    goto cleanup;
  }

  cass_future_wait(session_future);
  cass_future_free(session_future);

  {
    const char* query = "SELECT * FROM system.local WHERE key = ?";
    const char* key = "local";
    CassStatement* statement = NULL;
    CassFuture* result_future = NULL;
    const CassResult* result = NULL;
    CassIterator* iterator = NULL;

    statement = cass_statement_new(query, strlen(query), 1, CASS_CONSISTENCY_ONE);
    cass_statement_bind_string(statement, 0, key, strlen(key));
    cass_session_exec(session, statement, &result_future);

    cass_future_wait(result_future);
    result = cass_future_get_result(result_future);
    cass_future_free(result_future);

    iterator = cass_iterator_from_result(result);

    while(cass_iterator_next(iterator)) {
      CassIterator* tokens_iterator = NULL;
      const CassRow* row = cass_iterator_get_row(iterator);
      const CassValue* key_value = NULL;
      const CassValue* tokens_value = NULL;

      cass_size_t copied;
      char key_buffer[256];
      cass_row_get_column(row, 0, &key_value);
      cass_value_get_string(key_value, key_buffer, sizeof(key_buffer), &copied);
      key_buffer[copied] = '\0';
      printf("key: %s\n", key_buffer);

      cass_row_get_column(row, 13, &tokens_value);
      tokens_iterator = cass_iterator_from_collection(tokens_value);
      while(cass_iterator_next(tokens_iterator)) {
        const CassValue* token_value = cass_iterator_get_value(tokens_iterator);
        cass_size_t copied;
        char token_buffer[256];
        cass_value_get_string(token_value, token_buffer, sizeof(token_buffer), &copied);
        token_buffer[copied] = '\0';
        printf("token: %s\n", token_buffer);
      }
      cass_iterator_free(tokens_iterator);
    }

    cass_iterator_free(iterator);
    cass_result_free(result);
    cass_statement_free(statement);
  }

  /*
  {
    CassStatement* statement = NULL;
    CassFuture* result_future = NULL;
    CassResult* result = NULL;

    const char* query = "INSERT INTO test.basic (key, bln, flt, dbl, i32, i64) VALUES (?, ?, ?, ?, ?, ?);";
    const char* key = "abcdefghijklmnopqrstuvwxyz";
    cass_session_query(session, query, strlen(query), 6, CASS_CONSISTENCY_ONE, &statement);
    cass_statement_bind_string(statement, 0, key, strlen(key));
    cass_statement_bind_bool(statement, 1, cass_true);
    cass_statement_bind_float(statement, 2, 0.1);
    cass_statement_bind_double(statement, 3, 99999);
    cass_statement_bind_int(statement, 4, 1);
    cass_statement_bind_bigint(statement, 5, 2);

    cass_session_exec(session, statement, &result_future);

    cass_future_wait(result_future);
    result = cass_future_get_result(result_future);
  }

  {
    CassStatement* statement = NULL;
    CassFuture* result_future = NULL;
    CassResult* result = NULL;
    CassFuture* prepared_future;
    CassPrepared* prepared;

    const char* query = "INSERT INTO test.basic (key, bln, flt, dbl, i32, i64) VALUES (?, ?, ?, ?, ?, ?);";
    const char* key = "abcdefghijklmnopqrstuvwxyz";

    cass_session_prepare(session, query, strlen(query), &prepared_future);

    cass_future_wait(prepared_future);
    prepared = cass_future_get_prepared(prepared_future);

    cass_prepared_bind(prepared, 6, CASS_CONSISTENCY_ONE, &statement);

    cass_statement_bind_string(statement, 0, key, strlen(key));
    cass_statement_bind_bool(statement, 1, cass_true);
    cass_statement_bind_float(statement, 2, 0.1);
    cass_statement_bind_double(statement, 3, 99999);
    cass_statement_bind_int(statement, 4, 1);
    cass_statement_bind_bigint(statement, 5, 2);

    cass_session_exec(session, statement, &result_future);

    cass_future_wait(result_future);
    result = cass_future_get_result(result_future);
  }
  */

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
