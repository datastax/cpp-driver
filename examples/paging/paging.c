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

void insert_into_paging(CassSession* session, CassUuidGen* uuid_gen) {
  CassError rc = CASS_OK;
  CassString query = cass_string_init("INSERT INTO paging (key, value) VALUES (?, ?);");

  CassFuture* futures[NUM_CONCURRENT_REQUESTS];

  size_t i;
  for (i = 0; i < NUM_CONCURRENT_REQUESTS; ++i) {
    CassUuid key;
    char value_buffer[256];
    CassStatement* statement = cass_statement_new(query, 2);

    cass_uuid_gen_time(uuid_gen, &key);
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
      char key_str[CASS_UUID_STRING_LENGTH];
      CassString value;
      char value_buffer[256];

      const CassRow* row = cass_iterator_get_row(iterator);
      cass_value_get_uuid(cass_row_get_column(row, 0), &key);
      cass_uuid_string(key, key_str);

      cass_value_get_string(cass_row_get_column(row, 1), &value);
      memcpy(value_buffer, value.data, value.length);
      value_buffer[value.length] = '\0';

      printf("key: '%s' value: '%s'\n", key_str, value_buffer);
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
  CassUuidGen* uuid_gen = cass_uuid_gen_new();
  CassCluster* cluster = create_cluster();
  CassSession* session = cass_session_new();
  CassFuture* close_future = NULL;

  if(connect_session(session, cluster) != CASS_OK) {
    cass_cluster_free(cluster);
    cass_session_free(session);
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

  insert_into_paging(session, uuid_gen);
  select_from_paging(session);

  close_future = cass_session_close(session);
  cass_future_wait(close_future);
  cass_future_free(close_future);

  cass_uuid_gen_free(uuid_gen);
  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;
}
