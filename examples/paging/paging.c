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

#define NUM_CONCURRENT_REQUESTS 1000

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
  if (rc != CASS_OK) {
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
  if (rc != CASS_OK) {
    print_error(future);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

void insert_into_paging(CassSession* session, const char* key) {
  CassError rc = CASS_OK;
  CassString query = cass_string_init("INSERT INTO paging (key, value) VALUES (?, ?);");

  CassFuture* futures[NUM_CONCURRENT_REQUESTS];

  size_t i;
  for (i = 0; i < NUM_CONCURRENT_REQUESTS; ++i) {
    CassUuid key;
    char value_buffer[256];
    CassStatement* statement = cass_statement_new(query, 2);

    cass_uuid_generate_time(key);
    cass_statement_bind_uuid(statement, 0, key);

    sprintf(value_buffer, "%u", (unsigned int)i);
    cass_statement_bind_string(statement, 1, cass_string_init(value_buffer));

    futures[i] = cass_session_execute(session, statement);

    cass_statement_free(statement);
  }

  for (i = 0; i < NUM_CONCURRENT_REQUESTS; ++i) {
    CassFuture* future = futures[i];

    rc = cass_future_error_code(future);
    if (rc != CASS_OK) {
      print_error(future);
    }

    cass_future_free(future);
  }
}

void select_from_paging(CassSession* session) {
  cass_bool_t has_more_pages = cass_false;
  const CassResult* result = NULL;
  CassString query = cass_string_init("SELECT * FROM paging");
  CassStatement* statement = cass_statement_new(query, 0);

  cass_statement_set_paging_size(statement, 100);

  do {
    CassIterator* iterator;
    CassFuture* future = cass_session_execute(session, statement);

    if (cass_future_error_code(future) != 0) {
      print_error(future);
      break;
    }

    result = cass_future_get_result(future);
    iterator = cass_iterator_from_result(result);
    cass_future_free(future);

    while (cass_iterator_next(iterator)) {
      CassUuid key;
      char key_buffer[CASS_UUID_STRING_LENGTH];
      CassString value;
      char value_buffer[256];

      const CassRow* row = cass_iterator_get_row(iterator);
      cass_value_get_uuid(cass_row_get_column(row, 0), key);
      cass_uuid_string(key, key_buffer);

      cass_value_get_string(cass_row_get_column(row, 1), &value);
      memcpy(value_buffer, value.data, value.length);
      value_buffer[value.length] = '\0';

      printf("key: '%s' value: '%s'\n", key_buffer, value_buffer);
    }

    has_more_pages = cass_result_has_more_pages(result);

    if (has_more_pages) {
      cass_statement_set_paging_state(statement, result);
    }

    cass_iterator_free(iterator);
    cass_result_free(result);

  } while (has_more_pages);
  cass_statement_free(statement);
}

int main() {
  CassError rc = CASS_OK;
  CassCluster* cluster = create_cluster();
  CassSession* session = NULL;
  CassFuture* close_future = NULL;

  rc = connect_session(cluster, &session);
  if (rc != CASS_OK) {
    return -1;
  }

  execute_query(session,
                "CREATE KEYSPACE examples WITH replication = { \
                           'class': 'SimpleStrategy', 'replication_factor': '3' };");


  execute_query(session,
                "CREATE TABLE examples.paging (key timeuuid, \
                                               value text, \
                                               PRIMARY KEY (key));");

  execute_query(session, "USE examples");

  insert_into_paging(session, "test");
  select_from_paging(session);

  close_future = cass_session_close(session);
  cass_future_wait(close_future);
  cass_future_free(close_future);
  cass_cluster_free(cluster);

  return 0;
}
