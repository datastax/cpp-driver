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

#include "cassandra.h"

uv_mutex_t mutex;
uv_cond_t cond;

void on_create_keyspace(CassFuture* future, void* data);
void on_create_table(CassFuture* future, void* data);

void on_insert(CassFuture* future, void* data);
void on_select(CassFuture* future, void* data);

void on_session_connect(CassFuture* future, void* data);
void on_session_close(CassFuture* future, void* data);

void print_error(CassFuture* future) {
  CassString message = cass_future_error_message(future);
  fprintf(stderr, "Error: %.*s\n", (int)message.length, message.data);
}

CassCluster* create_cluster() {
  CassCluster* cluster = cass_cluster_new();
  CassString contact_points = cass_string_init("127.0.0.1,127.0.0.2,127.0.0.3");
  cass_cluster_set_contact_points(cluster, contact_points);
  return cluster;
}

void connect_session(CassCluster* cluster, CassFutureCallback callback) {
  CassFuture* future = cass_cluster_connect_keyspace(cluster, "examples");
  cass_future_set_callback(future, callback, NULL);
  cass_future_free(future);
}

void close_session(CassSession* session) {
  CassFuture* future = cass_session_close(session);
  cass_future_set_callback(future, on_session_close, NULL);
  cass_future_free(future);
}

void execute_query(CassSession* session, const char* query,
                   CassFutureCallback callback) {
  CassStatement* statement = cass_statement_new(cass_string_init(query), 0);
  CassFuture* future = cass_session_execute(session, statement);
  cass_future_set_callback(future, callback, session);
  cass_future_free(future);
  cass_statement_free(statement);
}

void on_create_keyspace(CassFuture* future, void* data) {
  CassError code = cass_future_error_code(future);
  if (code != CASS_OK) {
    print_error(future);
  }

  execute_query((CassSession*)data,
                "CREATE TABLE callbacks "
                "(key timeuuid PRIMARY KEY, value bigint)",
                on_create_table);
}

void on_create_table(CassFuture* future, void* data) {
  CassString insert_query
      = cass_string_init("INSERT INTO callbacks (key, value) "
                         "VALUES (?, ?)");
  CassUuid key;
  CassStatement* statement = NULL;
  CassFuture* insert_future = NULL;

  CassError code = cass_future_error_code(future);
  if (code != CASS_OK) {
    print_error(future);
  }

  statement = cass_statement_new(insert_query, 2);

  cass_uuid_generate_time(key);
  cass_statement_bind_uuid(statement, 0, key);
  cass_statement_bind_int64(statement, 1,
                            cass_uuid_timestamp(key));

  insert_future = cass_session_execute((CassSession*)data, statement);

  cass_future_set_callback(insert_future, on_insert, data);

  cass_statement_free(statement);
  cass_future_free(insert_future);
}

void on_insert(CassFuture* future, void* data) {
  CassError code = cass_future_error_code(future);
  if (code != CASS_OK) {
    print_error(future);
    close_session((CassSession*)data);
  } else {
    CassString select_query
        = cass_string_init("SELECT * FROM callbacks");
    CassStatement* statement
        = cass_statement_new(select_query, 0);
    CassFuture* select_future
        = cass_session_execute((CassSession*)data, statement);

    cass_future_set_callback(select_future, on_select, data);

    cass_statement_free(statement);
    cass_future_free(select_future);
  }
}

void on_select(CassFuture* future, void* data) {
  CassError code = cass_future_error_code(future);
  if (code != CASS_OK) {
    print_error(future);
  } else {
    const CassResult* result = cass_future_get_result(future);
    CassIterator* iterator = cass_iterator_from_result(result);

    while (cass_iterator_next(iterator)) {
      CassUuid key;
      char key_buf[CASS_UUID_STRING_LENGTH];
      cass_uint64_t value = 0;
      const CassRow* row = cass_iterator_get_row(iterator);

      cass_value_get_uuid(cass_row_get_column(row, 0), key);
      cass_uuid_string(key, key_buf);
      cass_value_get_int64(cass_row_get_column(row, 1), (cass_int64_t*)&value);

      printf("%s, %llu\n", key_buf, value);
    }
  }

  close_session((CassSession*)data);
}

void on_session_connect(CassFuture* future, void* data) {
  CassSession* session = NULL;
  CassError code = cass_future_error_code(future);

  if (code != CASS_OK) {
    print_error(future);
    uv_cond_signal(&cond);
    return;
  }

  session = cass_future_get_session(future);
  execute_query(session,
                "CREATE KEYSPACE examples WITH replication = { "
                "'class': 'SimpleStrategy', 'replication_factor': '3' };",
                on_create_keyspace);
}

void on_session_close(CassFuture *future, void *data) {
  uv_cond_signal(&cond);
}

int main() {
  CassCluster* cluster = create_cluster();

  uv_mutex_init(&mutex);
  uv_cond_init(&cond);

  connect_session(cluster, on_session_connect);

  /* Code running in parallel with queries */

  uv_mutex_lock(&mutex);
  uv_cond_wait(&cond, &mutex);
  uv_mutex_unlock(&mutex);

  uv_cond_destroy(&cond);
  uv_mutex_destroy(&mutex);

  cass_cluster_free(cluster);

  return 0;
}
