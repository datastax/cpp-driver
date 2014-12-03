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

CassError connect_session(CassCluster* cluster, CassSession** output) {
  CassError rc = CASS_OK;
  CassFuture* future = cass_cluster_connect(cluster);

  *output = NULL;

  cass_future_wait(future);
  rc = cass_future_error_code(future);
  if(rc != CASS_OK) {
    print_error(future);
  } else {
    *output = cass_future_get_session(future);
  }
  cass_future_free(future);

  return rc;
}

CassError execute_query(CassSession* session, const char* query) {
  CassError rc = CASS_OK;
  CassFuture* future = NULL;
  CassStatement* statement = cass_statement_new(cass_string_init(query), 0);

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

CassError insert_into_log(CassSession* session, const char* key, CassUuid time, const char* entry) {
  CassError rc = CASS_OK;
  CassStatement* statement = NULL;
  CassFuture* future = NULL;
  CassString query = cass_string_init("INSERT INTO examples.log (key, time, entry) VALUES (?, ?, ?);");

  statement = cass_statement_new(query, 3);

  cass_statement_bind_string(statement, 0, cass_string_init(key));
  cass_statement_bind_uuid(statement, 1, time);
  cass_statement_bind_string(statement, 2, cass_string_init(entry));

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

CassError select_from_log(CassSession* session, const char* key) {
  CassError rc = CASS_OK;
  CassStatement* statement = NULL;
  CassFuture* future = NULL;
  CassString query = cass_string_init("SELECT * FROM examples.log WHERE key = ?");

  statement = cass_statement_new(query, 1);

  cass_statement_bind_string(statement, 0, cass_string_init(key));

  future = cass_session_execute(session, statement);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if(rc != CASS_OK) {
    print_error(future);
  } else {
    const CassResult* result = cass_future_get_result(future);
    CassIterator* iterator = cass_iterator_from_result(result);

    while(cass_iterator_next(iterator)) {
      const CassRow* row = cass_iterator_get_row(iterator);
      CassString key;
      CassUuid time;
      CassString entry;
      char time_str[CASS_UUID_STRING_LENGTH];

      cass_value_get_string(cass_row_get_column(row, 0), &key);
      cass_value_get_uuid(cass_row_get_column(row, 1), &time);
      cass_value_get_string(cass_row_get_column(row, 2), &entry);

      cass_uuid_string(time, time_str);

      printf("%.*s %s %.*s\n", (int)key.length, key.data,
                               time_str,
                               (int)entry.length, entry.data);
    }

    cass_result_free(result);
    cass_iterator_free(iterator);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

int main() {
  CassError rc = CASS_OK;
  CassUuidGen* uuid_gen = cass_uuid_gen_new();
  CassCluster* cluster = create_cluster();
  CassSession* session = NULL;
  CassFuture* close_future = NULL;
  CassUuid uuid;

  rc = connect_session(cluster, &session);
  if(rc != CASS_OK) {
    return -1;
  }

  execute_query(session,
                "CREATE KEYSPACE examples WITH replication = { \
                           'class': 'SimpleStrategy', 'replication_factor': '3' };");


  execute_query(session,
                "CREATE TABLE examples.log (key text, time timeuuid, entry text, \
                                              PRIMARY KEY (key, time));");


  cass_uuid_gen_time(uuid_gen, &uuid);
  insert_into_log(session, "test", uuid, "Log entry #1");

  cass_uuid_gen_time(uuid_gen, &uuid);
  insert_into_log(session, "test", uuid, "Log entry #2");

  cass_uuid_gen_time(uuid_gen, &uuid);
  insert_into_log(session, "test", uuid, "Log entry #3");

  cass_uuid_gen_time(uuid_gen, &uuid);
  insert_into_log(session, "test", uuid, "Log entry #4");

  select_from_log(session, "test");

  close_future = cass_session_close(session);
  cass_future_wait(close_future);
  cass_future_free(close_future);
  cass_cluster_free(cluster);
  cass_uuid_gen_free(uuid_gen);

  return 0;
}
