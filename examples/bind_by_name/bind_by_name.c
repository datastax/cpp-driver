/*
  This is free and unencumbered software released into the public domain.

  Anyone is free to copy, modify, publish, use, compile, sell, or
  distribute this software, either in source code form or as a compiled
  binary, for any purpose, commercial or non-commercial, and by any
  means.

  In jurisdictions that recognize copyright laws, the author or authors
  of this software dedicate any and all copyright interest in the
  software to the public domain. We make this dedication for the benefit
  of the public at large and to the detriment of our heirs and
  successors. We intend this dedication to be an overt act of
  relinquishment in perpetuity of all present and future rights to this
  software under copyright law.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
  OTHER DEALINGS IN THE SOFTWARE.

  For more information, please refer to <http://unlicense.org/>
*/

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "cassandra.h"

struct Basic_ {
  cass_bool_t bln;
  cass_float_t flt;
  cass_double_t dbl;
  cass_int32_t i32;
  cass_int64_t i64;
};

typedef struct Basic_ Basic;

void print_error(CassFuture* future) {
  const char* message;
  size_t message_length;
  cass_future_error_message(future, &message, &message_length);
  fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
}

CassCluster* create_cluster() {
  CassCluster* cluster = cass_cluster_new();
  cass_cluster_set_contact_points(cluster, "127.0.0.1,127.0.0.2,127.0.0.3");
  return cluster;
}

CassError connect_session(CassSession* session, const CassCluster* cluster) {
  CassError rc = CASS_OK;
  CassFuture* future = cass_session_connect(session, cluster);

  cass_future_wait(future);
  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  }
  cass_future_free(future);

  return rc;
}

CassError execute_query(CassSession* session, const char* query) {
  CassError rc = CASS_OK;
  CassStatement* statement = cass_statement_new(query, 0);
  CassFuture* future = cass_session_execute(session, statement);

  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

CassError prepare_query(CassSession* session, const char* query, const CassPrepared** prepared) {
  CassError rc = CASS_OK;
  CassFuture* future = NULL;

  future = cass_session_prepare(session, query);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  } else {
    *prepared = cass_future_get_prepared(future);
  }

  cass_future_free(future);

  return rc;
}

CassError insert_into_basic(CassSession* session, const CassPrepared* prepared, const char* key, const Basic* basic) {
  CassError rc = CASS_OK;
  CassStatement* statement = NULL;
  CassFuture* future = NULL;

  statement = cass_prepared_bind(prepared);

  cass_statement_bind_string_by_name(statement, "key", key);
  cass_statement_bind_bool_by_name(statement, "BLN", basic->bln);
  cass_statement_bind_float_by_name(statement, "FLT", basic->flt);
  cass_statement_bind_double_by_name(statement, "\"dbl\"", basic->dbl);
  cass_statement_bind_int32_by_name(statement, "i32", basic->i32);
  cass_statement_bind_int64_by_name(statement, "I64", basic->i64);

  future = cass_session_execute(session, statement);

  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

CassError select_from_basic(CassSession* session, const CassPrepared * prepared, const char* key, Basic* basic) {
  CassError rc = CASS_OK;
  CassStatement* statement = NULL;
  CassFuture* future = NULL;

  statement = cass_prepared_bind(prepared);

  cass_statement_bind_string_by_name(statement, "key", key);

  future = cass_session_execute(session, statement);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  } else {
    const CassResult* result = cass_future_get_result(future);
    CassIterator* iterator = cass_iterator_from_result(result);

    if (cass_iterator_next(iterator)) {
      const CassRow* row = cass_iterator_get_row(iterator);

      cass_value_get_bool(cass_row_get_column_by_name(row, "BLN"), &basic->bln);
      cass_value_get_double(cass_row_get_column_by_name(row, "dbl"), &basic->dbl);
      cass_value_get_float(cass_row_get_column_by_name(row, "flt"), &basic->flt);
      cass_value_get_int32(cass_row_get_column_by_name(row, "\"i32\""), &basic->i32);
      cass_value_get_int64(cass_row_get_column_by_name(row, "i64"), &basic->i64);
    }

    cass_result_free(result);
    cass_iterator_free(iterator);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

int main() {
  CassCluster* cluster = create_cluster();
  CassSession* session = cass_session_new();
  CassFuture* close_future = NULL;
  Basic input = { cass_true, 0.001f, 0.0002, 1, 2 };
  Basic output;
  const CassPrepared* insert_prepared = NULL;
  const CassPrepared* select_prepared = NULL;

  const char* insert_query
    = "INSERT INTO examples.basic (key, bln, flt, dbl, i32, i64) VALUES (?, ?, ?, ?, ?, ?);";
  const char* select_query
    = "SELECT * FROM examples.basic WHERE key = ?";

  if (connect_session(session, cluster) != CASS_OK) {
    cass_cluster_free(cluster);
    cass_session_free(session);
    return -1;
  }

  execute_query(session,
                "CREATE KEYSPACE examples WITH replication = { \
                           'class': 'SimpleStrategy', 'replication_factor': '3' };");


  execute_query(session,
                "CREATE TABLE examples.basic (key text, \
                                              bln boolean, \
                                              flt float, dbl double,\
                                              i32 int, i64 bigint, \
                                              PRIMARY KEY (key));");


  if (prepare_query(session, insert_query, &insert_prepared) == CASS_OK) {
    insert_into_basic(session, insert_prepared, "prepared_test", &input);
    cass_prepared_free(insert_prepared);
  }

  if (prepare_query(session, select_query,  &select_prepared) == CASS_OK) {
    select_from_basic(session, select_prepared, "prepared_test", &output);

    assert(input.bln == output.bln);
    assert(input.flt == output.flt);
    assert(input.dbl == output.dbl);
    assert(input.i32 == output.i32);
    assert(input.i64 == output.i64);
    cass_prepared_free(select_prepared);
  }

  close_future = cass_session_close(session);
  cass_future_wait(close_future);
  cass_future_free(close_future);

  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;
}
