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

CassError insert_into_collections(CassSession* session, const char* key, const char* items[]) {
  CassError rc = 0;
  CassStatement* statement = NULL;
  CassFuture* future = NULL;
  CassCollection* collection = NULL;
  const char** item = NULL;
  CassString query = cass_string_init("INSERT INTO examples.collections (key, items) VALUES (?, ?);");

  statement = cass_statement_new(query, 2, CASS_CONSISTENCY_ONE);

  cass_statement_bind_string(statement, 0, cass_string_init(key));

  collection = cass_collection_new(2);
  for(item = items; *item; item++) {
    cass_collection_append_string(collection, cass_string_init(*item));
  }
  cass_statement_bind_collection(statement, 1, collection, cass_false);
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

CassError select_from_collections(CassSession* session, const char* key) {
  CassError rc = 0;
  CassStatement* statement = NULL;
  CassFuture* future = NULL;
  CassString query = cass_string_init("SELECT items FROM examples.collections WHERE key = ?");

  statement = cass_statement_new(query, 1, CASS_CONSISTENCY_ONE);

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
      const CassValue* value = NULL;
      const CassRow* row = cass_iterator_get_row(iterator);
      CassIterator* items_iterator = NULL;

      value = cass_row_get_column(row, 0);
      items_iterator = cass_iterator_from_collection(value);
      while(cass_iterator_next(items_iterator)) {
        CassString item_string;
        cass_value_get_string(cass_iterator_get_value(items_iterator), &item_string);
        printf("item: %.*s\n", (int)item_string.length, item_string.data);
      }
      cass_iterator_free(items_iterator);
    }

    cass_result_free(result);
    cass_iterator_free(iterator);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

int main() {
  CassError rc = 0;
  CassCluster* cluster = create_cluster();
  CassSession* session = NULL;
  CassFuture* close_future = NULL;

  const char* items[] = { "apple", "orange", "banana", "mango", NULL };

  rc = connect_session(cluster, &session);
  if(rc != CASS_OK) {
    return -1;
  }

  execute_query(session,
                "CREATE KEYSPACE examples WITH replication = { \
                           'class': 'SimpleStrategy', 'replication_factor': '1' };");

  execute_query(session,
                "CREATE TABLE examples.collections (key text, \
                                                    items set<text>, \
                                                    PRIMARY KEY (key))");


  insert_into_collections(session, "test", items);
  select_from_collections(session, "test");

  close_future = cass_session_close(session);
  cass_future_wait(close_future);
  cass_cluster_free(cluster);

  return 0;
}
