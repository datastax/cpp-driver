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

typedef struct Pair_ {
  const char* key;
  cass_int32_t value;
} Pair;

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

CassError insert_into_maps(CassSession* session, const char* key, const Pair items[]) {
  CassError rc = CASS_OK;
  CassStatement* statement = NULL;
  CassFuture* future = NULL;
  CassCollection* collection = NULL;
  const Pair* item = NULL;
  CassString query = cass_string_init("INSERT INTO examples.maps (key, items) VALUES (?, ?);");

  statement = cass_statement_new(query, 2);

  cass_statement_bind_string(statement, 0, cass_string_init(key));

  collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, 5);
  for (item = items; item->key; item++) {
    cass_collection_append_string(collection, cass_string_init(item->key));
    cass_collection_append_int32(collection, item->value);
  }
  cass_statement_bind_collection(statement, 1, collection);
  cass_collection_free(collection);

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

CassError select_from_maps(CassSession* session, const char* key) {
  CassError rc = CASS_OK;
  CassStatement* statement = NULL;
  CassFuture* future = NULL;
  CassString query = cass_string_init("SELECT items FROM examples.maps WHERE key = ?");

  statement = cass_statement_new(query, 1);

  cass_statement_bind_string(statement, 0, cass_string_init(key));

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
        CassString key;
        cass_int32_t value;
        cass_value_get_string(cass_iterator_get_map_key(iterator), &key);
        cass_value_get_int32(cass_iterator_get_map_value(iterator), &value);
        printf("item: '%.*s' : %d \n", (int)key.length, key.data, value);
      }
      cass_iterator_free(iterator);
    }

    cass_result_free(result);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

int main() {
  CassCluster* cluster = create_cluster();
  CassSession* session = cass_session_new();
  CassFuture* close_future = NULL;

  const Pair items[] = { { "apple", 1 }, { "orange", 2 }, { "banana", 3 }, { "mango", 4 }, { NULL, 0 } };

  if(connect_session(session, cluster) != CASS_OK) {
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
