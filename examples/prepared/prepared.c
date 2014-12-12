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
  CassString message = cass_future_error_message(future);
  fprintf(stderr, "Error: %.*s\n", (int)message.length, message.data);
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
  if(rc != CASS_OK) {
    print_error(future);
  }
  cass_future_free(future);

  return rc;
}

CassError execute_query(CassSession* session, const char* query) {
  CassError rc = CASS_OK;
  CassStatement* statement = cass_statement_new(cass_string_init(query), 0);
  CassFuture* future = cass_session_execute(session, statement);

  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if(rc != CASS_OK) {
    print_error(future);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

CassError insert_into_basic(CassSession* session, const char* key, const Basic* basic) {
  CassError rc = CASS_OK;
  CassStatement* statement = NULL;
  CassFuture* future = NULL;
  CassString query = cass_string_init("INSERT INTO examples.basic (key, bln, flt, dbl, i32, i64) VALUES (?, ?, ?, ?, ?, ?);");

  statement = cass_statement_new(query, 6);

  cass_statement_bind_string(statement, 0, cass_string_init(key));
  cass_statement_bind_bool(statement, 1, basic->bln);
  cass_statement_bind_float(statement, 2, basic->flt);
  cass_statement_bind_double(statement, 3, basic->dbl);
  cass_statement_bind_int32(statement, 4, basic->i32);
  cass_statement_bind_int64(statement, 5, basic->i64);

  future = cass_session_execute(session, statement);

  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if(rc != CASS_OK) {
    print_error(future);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

CassError prepare_select_from_basic(CassSession* session, const CassPrepared** prepared) {
  CassError rc = CASS_OK;
  CassFuture* future = NULL;
  CassString query = cass_string_init("SELECT * FROM examples.basic WHERE key = ?");

  future = cass_session_prepare(session, query);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if(rc != CASS_OK) {
    print_error(future);
  } else {
    *prepared = cass_future_get_prepared(future);
  }

  cass_future_free(future);

  return rc;
}

CassError select_from_basic(CassSession* session, const CassPrepared * prepared, const char* key, Basic* basic) {
  CassError rc = CASS_OK;
  CassStatement* statement = NULL;
  CassFuture* future = NULL;

  statement = cass_prepared_bind(prepared);

  cass_statement_bind_string(statement, 0, cass_string_init(key));

  future = cass_session_execute(session, statement);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if(rc != CASS_OK) {
    print_error(future);
  } else {
    const CassResult* result = cass_future_get_result(future);
    CassIterator* iterator = cass_iterator_from_result(result);

    if(cass_iterator_next(iterator)) {
      const CassRow* row = cass_iterator_get_row(iterator);

      cass_value_get_bool(cass_row_get_column(row, 1), &basic->bln);
      cass_value_get_double(cass_row_get_column(row, 2), &basic->dbl);
      cass_value_get_float(cass_row_get_column(row, 3), &basic->flt);
      cass_value_get_int32(cass_row_get_column(row, 4), &basic->i32);
      cass_value_get_int64(cass_row_get_column(row, 5), &basic->i64);
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
  CassSession* session = NULL;
  CassFuture* close_future = NULL;
  Basic input = { cass_true, 0.001f, 0.0002, 1, 2 };
  Basic output;
  const CassPrepared* prepared = NULL;

  if(connect_session(session, cluster) != CASS_OK) {
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

  insert_into_basic(session, "prepared_test", &input);

  if(prepare_select_from_basic(session, &prepared) == CASS_OK) {
    select_from_basic(session, prepared, "prepared_test", &output);

    assert(input.bln == output.bln);
    assert(input.flt == output.flt);
    assert(input.dbl == output.dbl);
    assert(input.i32 == output.i32);
    assert(input.i64 == output.i64);
    cass_prepared_free(prepared);
  }

  close_future = cass_session_close(session);
  cass_future_wait(close_future);
  cass_future_free(close_future);

  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;
}
