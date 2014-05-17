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

struct basic_s {
  cass_bool_t bln;
  cass_float_t flt;
  cass_double_t dbl;
  cass_int32_t i32;
  cass_int64_t i64;
};

typedef struct basic_s basic_t;

void print_error(const char* message, int err) {
  printf("%s: %s (%d)\n", message, cass_code_error_desc(err), err);
}

cass_code_t connect_session(const char* contact_points[], cass_session_t** output) {
  cass_code_t rc = 0;
  const char** cp = NULL;
  cass_future_t* future = NULL;
  cass_session_t* session = cass_session_new();

  *output = NULL;

  for(cp = contact_points; *cp; cp++) {
    cass_session_setopt(session, CASS_OPTION_CONTACT_POINT_ADD, *cp, strlen(*cp));
  }

  rc = cass_session_connect(session, &future);
  if(rc != CASS_OK) {
    cass_session_free(session);
  } else {
    cass_future_wait(future);
    rc = cass_future_error_code(future);
    if(rc != CASS_OK) {
      fprintf(stderr, "Error: %s\n", cass_future_error_message(future));
      cass_session_free(session);
    } else {
      *output = session;
    }
    cass_future_free(future);
  }

  return rc;
}

cass_code_t execute_query(cass_session_t* session, const char* query) {
  cass_code_t rc = 0;
  cass_future_t* future = NULL;
  cass_statement_t* statement = cass_statement_new(query, strlen(query), 0, CASS_CONSISTENCY_ONE);

  cass_session_exec(session, statement, &future);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if(rc != CASS_OK) {
    fprintf(stderr, "Error: %s\n", cass_future_error_message(future));
  }

  cass_future_free(future);

  return rc;
}

cass_code_t insert_into_basic(cass_session_t* session, const char* key, const basic_t* basic) {
  cass_code_t rc = 0;
  cass_statement_t* statement = NULL;
  cass_future_t* future = NULL;
  const char* query = "INSERT INTO examples.basic (key, bln, flt, dbl, i32, i64) VALUES (?, ?, ?, ?, ?, ?);";

  statement = cass_statement_new(query, strlen(query), 6, CASS_CONSISTENCY_ONE);

  cass_statement_bind_string(statement, 0, key, strlen(key));
  cass_statement_bind_bool(statement, 1, basic->bln);
  cass_statement_bind_float(statement, 2, basic->flt);
  cass_statement_bind_double(statement, 3, basic->dbl);
  cass_statement_bind_int32(statement, 4, basic->i32);
  cass_statement_bind_int64(statement, 5, basic->i64);

  cass_session_exec(session, statement, &future);

  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if(rc != CASS_OK) {
    fprintf(stderr, "Error: %s\n", cass_future_error_message(future));
  }

  cass_future_free(future);

  return rc;
}

cass_code_t select_from_basic(cass_session_t* session, const char* key, basic_t* basic) {
  cass_code_t rc = 0;
  cass_statement_t* statement = NULL;
  cass_future_t* future = NULL;
  const char* query = "SELECT * FROM examples.basic WHERE key = ?";

  statement = cass_statement_new(query, strlen(query), 1, CASS_CONSISTENCY_ONE);

  cass_statement_bind_string(statement, 0, key, strlen(key));

  cass_session_exec(session, statement, &future);

  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if(rc != CASS_OK) {
    fprintf(stderr, "Error: %s\n", cass_future_error_message(future));
  } else {
    const cass_result_t* result = cass_future_get_result(future);
    cass_iterator_t* iterator = cass_iterator_from_result(result);

    if(cass_iterator_next(iterator)) {
      const cass_value_t* value;
      const cass_row_t* row = cass_iterator_get_row(iterator);

      cass_row_get_column(row, 1, &value);
      cass_value_get_bool(value, &basic->bln);

      cass_row_get_column(row, 2, &value);
      cass_value_get_double(value, &basic->dbl);

      cass_row_get_column(row, 3, &value);
      cass_value_get_float(value, &basic->flt);

      cass_row_get_column(row, 4, &value);
      cass_value_get_int32(value, &basic->i32);

      cass_row_get_column(row, 5, &value);
      cass_value_get_int64(value, &basic->i64);
    }

    cass_result_free(result);
    cass_iterator_free(iterator);
  }

  cass_future_free(future);

  return rc;
}

int
main() {
  cass_code_t rc = 0;
  cass_session_t* session = NULL;
  const char* contact_points[] = { "127.0.0.1", NULL };
  basic_t input = { cass_true, 0.001, 0.0002, 1, 2 };
  basic_t output;


  rc = connect_session(contact_points, &session);
  if(rc != CASS_OK) {
    return -1;
  }

  execute_query(session,
                "CREATE KEYSPACE examples WITH replication = { \
                           'class': 'SimpleStrategy', 'replication_factor': '1' };");

  execute_query(session,
                "CREATE TABLE examples.basic (key text, \
                                              bln boolean, \
                                              flt float, dbl double,\
                                              i32 int, i64 bigint, \
                                              PRIMARY KEY (key));");


  insert_into_basic(session, "test", &input);
  select_from_basic(session, "test", &output);

  assert(input.bln == output.bln);
  assert(input.flt == output.flt);
  assert(input.dbl == output.dbl);
  assert(input.i32 == output.i32);
  assert(input.i64 == output.i64);

  cass_session_free(session);

  return 0;
}
