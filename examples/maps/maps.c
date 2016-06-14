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

typedef struct Pair_ {
  const char* key;
  cass_int32_t value;
} Pair;

void print_error(CassFuture* future) {
  const char* message;
  size_t message_length;
  cass_future_error_message(future, &message, &message_length);
  fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
}

CassCluster* create_cluster(const char* hosts) {
  CassCluster* cluster = cass_cluster_new();
  cass_cluster_set_contact_points(cluster, hosts);
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
  CassFuture* future = NULL;
  CassStatement* statement = cass_statement_new(query, 0);

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

CassError insert_into_maps(CassSession* session, const char* key, const Pair items[]) {
  CassError rc = CASS_OK;
  CassStatement* statement = NULL;
  CassFuture* future = NULL;
  CassCollection* collection = NULL;
  const Pair* item = NULL;
  const char* query = "INSERT INTO examples.maps (key, items) VALUES (?, ?);";

  statement = cass_statement_new(query, 2);

  cass_statement_bind_string(statement, 0, key);

  collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, 5);
  for (item = items; item->key; item++) {
    cass_collection_append_string(collection, item->key);
    cass_collection_append_int32(collection, item->value);
  }
  cass_statement_bind_collection(statement, 1, collection);
  cass_collection_free(collection);

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

CassError select_from_maps(CassSession* session, const char* key) {
  CassError rc = CASS_OK;
  CassStatement* statement = NULL;
  CassFuture* future = NULL;
  const char* query = "SELECT items FROM examples.maps WHERE key = ?";

  statement = cass_statement_new(query, 1);

  cass_statement_bind_string(statement, 0, key);

  future = cass_session_execute(session, statement);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  } else {
    const CassResult* result = cass_future_get_result(future);

    if (cass_result_row_count(result) > 0) {
      const CassRow* row = cass_result_first_row(result);

      CassIterator* iterator
          = cass_iterator_from_map(
              cass_row_get_column(row, 0));

      while (cass_iterator_next(iterator)) {
        const char* key;
        size_t key_length;
        cass_int32_t value;
        cass_value_get_string(cass_iterator_get_map_key(iterator), &key, &key_length);
        cass_value_get_int32(cass_iterator_get_map_value(iterator), &value);
        printf("item: '%.*s' : %d \n", (int)key_length, key, value);
      }
      cass_iterator_free(iterator);
    }

    cass_result_free(result);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

int main(int argc, char* argv[]) {
  CassCluster* cluster = NULL;
  CassSession* session = cass_session_new();
  CassFuture* close_future = NULL;
  char* hosts = "127.0.0.1";
  if (argc > 1) {
    hosts = argv[1];
  }
  cluster = create_cluster(hosts);

  const Pair items[] = { { "apple", 1 }, { "orange", 2 }, { "banana", 3 }, { "mango", 4 }, { NULL, 0 } };

  if (connect_session(session, cluster) != CASS_OK) {
    cass_cluster_free(cluster);
    cass_session_free(session);
    return -1;
  }

  execute_query(session,
                "CREATE KEYSPACE examples WITH replication = { \
                'class': 'SimpleStrategy', 'replication_factor': '3' };");

  execute_query(session,
                "CREATE TABLE examples.maps (key text, \
                items map<text, int>, \
                PRIMARY KEY (key))");


  insert_into_maps(session, "test", items);
  select_from_maps(session, "test");

  close_future = cass_session_close(session);
  cass_future_wait(close_future);
  cass_future_free(close_future);

  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;
}
