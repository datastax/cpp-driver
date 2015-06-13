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

#include <uv.h>

#include "cassandra.h"

uv_mutex_t mutex;
uv_cond_t cond;
CassFuture* close_future = NULL;
CassUuidGen* uuid_gen = NULL;

void wait_exit() {
  uv_mutex_lock(&mutex);
  while (close_future == NULL) {
    uv_cond_wait(&cond, &mutex);
  }
  uv_mutex_unlock(&mutex);
  cass_future_wait(close_future);
  cass_future_free(close_future);
}

void signal_exit(CassSession* session) {
  uv_mutex_lock(&mutex);
  close_future = cass_session_close(session);
  uv_cond_signal(&cond);
  uv_mutex_unlock(&mutex);
}

void on_create_keyspace(CassFuture* future, void* data);
void on_create_table(CassFuture* future, void* data);

void on_insert(CassFuture* future, void* data);
void on_select(CassFuture* future, void* data);

void on_session_connect(CassFuture* future, void* data);
void on_session_close(CassFuture* future, void* data);

void print_error(CassFuture* future) {
  const char* message;
  size_t message_length;
  cass_future_error_message(future, &message, &message_length);
  fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
}

CassCluster* create_cluster() {
  CassCluster* cluster = cass_cluster_new();
  cass_cluster_set_contact_points(cluster, "127.0.0.1");
  return cluster;
}

void connect_session(CassSession* session, const CassCluster* cluster, CassFutureCallback callback) {
  CassFuture* future = cass_session_connect_keyspace(session, cluster, "examples");
  cass_future_set_callback(future, callback, session);
  cass_future_free(future);
}

void execute_query(CassSession* session, const char* query,
                   CassFutureCallback callback) {
  CassStatement* statement = cass_statement_new(session, query, 0);
  CassFuture* future = cass_session_execute(session, statement);
  cass_future_set_callback(future, callback, session);
  cass_future_free(future);
  cass_statement_free(statement);
}

void on_session_connect(CassFuture* future, void* data) {
  CassSession* session = (CassSession*)data;
  CassError code = cass_future_error_code(future);

  if (code != CASS_OK) {
    print_error(future);
    uv_cond_signal(&cond);
    return;
  }

  execute_query(session,
                "CREATE KEYSPACE examples WITH replication = { "
                "'class': 'SimpleStrategy', 'replication_factor': '3' };",
                on_create_keyspace);
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
  const char* insert_query = "INSERT INTO callbacks (key, value) "
                             "VALUES (?, ?)";
  CassUuid key;
  CassStatement* statement = NULL;
  CassFuture* insert_future = NULL;

  CassError code = cass_future_error_code(future);
  if (code != CASS_OK) {
    print_error(future);
  }

  statement = cass_statement_new((CassSession*)data, insert_query, 2);

  cass_uuid_gen_time(uuid_gen, &key);
  cass_statement_bind_uuid(statement, 0, key);
  cass_statement_bind_int64(statement, 1, cass_uuid_timestamp(key));

  insert_future = cass_session_execute((CassSession*)data, statement);

  cass_future_set_callback(insert_future, on_insert, data);

  cass_statement_free(statement);
  cass_future_free(insert_future);
}

void on_insert(CassFuture* future, void* data) {
  CassError code = cass_future_error_code(future);
  if (code != CASS_OK) {
    print_error(future);
    signal_exit((CassSession*)data);
  } else {
    const char* select_query = "SELECT * FROM callbacks";
    CassStatement* statement
        = cass_statement_new((CassSession*)data, select_query, 0);
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
      char key_str[CASS_UUID_STRING_LENGTH];
      cass_uint64_t value = 0;
      const CassRow* row = cass_iterator_get_row(iterator);

      cass_value_get_uuid(cass_row_get_column(row, 0), &key);

      cass_uuid_string(key, key_str);
      cass_value_get_int64(cass_row_get_column(row, 1), (cass_int64_t*)&value);

      printf("%s, %llu\n", key_str, (unsigned long long)value);
    }
    cass_iterator_free(iterator);
    cass_result_free(result);
  }

  signal_exit((CassSession*)data);
}

int main() {
  CassCluster* cluster = create_cluster();
  CassSession* session = cass_session_new();

  uuid_gen = cass_uuid_gen_new();

  uv_mutex_init(&mutex);
  uv_cond_init(&cond);

  connect_session(session, cluster, on_session_connect);

  /* Code running in parallel with queries */

  wait_exit();

  uv_cond_destroy(&cond);
  uv_mutex_destroy(&mutex);

  cass_cluster_free(cluster);
  cass_uuid_gen_free(uuid_gen);
  cass_session_free(session);

  return 0;
}
