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

#define NUM_CONCURRENT_REQUESTS 4096

void print_error(CassFuture* future) {
  CassString message = cass_future_error_message(future);
  fprintf(stderr, "Error: %.*s\n", (int)message.length, message.data);
}


CassCluster* create_cluster() {
  const char* contact_points[] = { "127.0.0.1", "127.0.0.2", "127.0.0.3", NULL };
  CassCluster* cluster = cass_cluster_new();
  const char** contact_point = NULL;
  for(contact_point = contact_points; *contact_point; contact_point++) {
    cass_cluster_setopt(cluster, CASS_OPTION_CONTACT_POINT_ADD, *contact_point, strlen(*contact_point));
  }
  return cluster;
}

CassError connect_session(CassCluster* cluster, CassSession** output) {
  CassError rc = 0;
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
  CassError rc = 0;
  CassFuture* future = NULL;
  CassStatement* statement = cass_statement_new(cass_string_init(query), 0, CASS_CONSISTENCY_ONE);

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

void insert_into_async(CassSession* session, const char* key) {
  CassError rc = 0;
  CassStatement* statement = NULL;
  CassString query = cass_string_init("INSERT INTO async (key, bln, flt, dbl, i32, i64) VALUES (?, ?, ?, ?, ?, ?);");

  CassFuture* futures[NUM_CONCURRENT_REQUESTS];

  size_t i;
  for(i = 0; i < NUM_CONCURRENT_REQUESTS; ++i) {
     char key_buffer[64];
    statement = cass_statement_new(query, 6, CASS_CONSISTENCY_ONE);

    snprintf(key_buffer, sizeof(key_buffer), "%s%zu", key, i);
    cass_statement_bind_string(statement, 0, cass_string_init(key_buffer));
    cass_statement_bind_bool(statement, 1, i % 2 == 0 ? cass_true : cass_false);
    cass_statement_bind_float(statement, 2, i / 2.0f);
    cass_statement_bind_double(statement, 3, i / 200.0);
    cass_statement_bind_int32(statement, 4, i * 10);
    cass_statement_bind_int64(statement, 5, i * 100);

    futures[i] = cass_session_execute(session, statement);

    cass_statement_free(statement);
  }

  for(i = 0; i < NUM_CONCURRENT_REQUESTS; ++i) {
    CassFuture* future = futures[i];

    cass_future_wait(future);

    rc = cass_future_error_code(future);
    if(rc != CASS_OK) {
      print_error(future);
    }

    cass_future_free(future);
  }
}

int main() {
  CassError rc = 0;
  CassCluster* cluster = create_cluster();
  CassSession* session = NULL;
  CassFuture* close_future = NULL;

  rc = connect_session(cluster, &session);
  if(rc != CASS_OK) {
    return -1;
  }

  execute_query(session,
                "CREATE KEYSPACE examples WITH replication = { \
                           'class': 'SimpleStrategy', 'replication_factor': '3' };");


  execute_query(session,
                "CREATE TABLE examples.async (key text, \
                                              bln boolean, \
                                              flt float, dbl double,\
                                              i32 int, i64 bigint, \
                                              PRIMARY KEY (key));");

  execute_query(session, "USE examples");

  insert_into_async(session, "test");

  close_future = cass_session_close(session);
  cass_future_wait(close_future);
  cass_cluster_free(cluster);

  return 0;
}
