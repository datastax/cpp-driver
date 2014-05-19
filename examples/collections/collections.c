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

#include <arpa/inet.h>

#include "cassandra.h"

void print_error(const char* message, int err) {
  printf("%s: %s (%d)\n", message, cass_code_error_desc(err), err);
}

cass_code_t connect_session(const char* contact_points[], cass_session_t** output) {
  cass_code_t rc = 0;
  const char** cp = NULL;
  cass_future_t* future = NULL;
  cass_session_t* session = cass_session_new();

  *output = NULL;

  for(cp = contact_points; *cp; cp++) {
    cass_session_setopt(session, CASS_OPTION_CONTACT_POINT_ADD, *cp, strlen(*cp));
  }

  rc = cass_session_connect(session, &future);
  if(rc != CASS_OK) {
    cass_session_free(session);
  } else {
    cass_future_wait(future);
    rc = cass_future_error_code(future);
    if(rc != CASS_OK) {
      fprintf(stderr, "Error: %s\n", cass_future_error_string(future));
      cass_session_free(session);
    } else {
      *output = session;
    }
    cass_future_free(future);
  }

  return rc;
}

cass_code_t execute_query(cass_session_t* session, const char* query) {
  cass_code_t rc = 0;
  cass_future_t* future = NULL;
  cass_statement_t* statement = cass_statement_new(query, strlen(query), 0, CASS_CONSISTENCY_ONE);

  cass_session_exec(session, statement, &future);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if(rc != CASS_OK) {
    fprintf(stderr, "Error: %s\n", cass_future_error_string(future));
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

cass_code_t insert_into_collections(cass_session_t* session, const char* key, const char* items[]) {
  cass_code_t rc = 0;
  cass_statement_t* statement = NULL;
  cass_future_t* future = NULL;
  cass_collection_t* collection = NULL;
  const char** item = NULL;
  const char* query = "INSERT INTO examples.collections (key, items) VALUES (?, ?);";

  statement = cass_statement_new(query, strlen(query), 2, CASS_CONSISTENCY_ONE);

  cass_statement_bind_string(statement, 0, key, strlen(key));

  collection = cass_collection_new(2);
  for(item = items; *item; item++) {
    cass_collection_append_string(collection, *item, strlen(*item));
  }
  cass_statement_bind_collection(statement, 1, collection, cass_false);
  cass_collection_free(collection);

  cass_session_exec(session, statement, &future);

  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if(rc != CASS_OK) {
    fprintf(stderr, "Error: %s\n", cass_future_error_string(future));
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

cass_code_t select_from_collections(cass_session_t* session, const char* key) {
  cass_code_t rc = 0;
  cass_statement_t* statement = NULL;
  cass_future_t* future = NULL;
  const char* query = "SELECT items FROM examples.collections WHERE key = ?";

  statement = cass_statement_new(query, strlen(query), 1, CASS_CONSISTENCY_ONE);

  cass_statement_bind_string(statement, 0, key, strlen(key));

  cass_session_exec(session, statement, &future);

  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if(rc != CASS_OK) {
    fprintf(stderr, "Error: %s\n", cass_future_error_string(future));
  } else {
    const cass_result_t* result = cass_future_get_result(future);
    cass_iterator_t* iterator = cass_iterator_from_result(result);

    if(cass_iterator_next(iterator)) {
      const cass_value_t* value = NULL;
      const cass_row_t* row = cass_iterator_get_row(iterator);
      cass_iterator_t* items_iterator = NULL;

      cass_row_get_column(row, 0, &value);
      items_iterator = cass_iterator_from_collection(value);
      while(cass_iterator_next(items_iterator)) {
        const cass_value_t* item_value = cass_iterator_get_value(items_iterator);
        cass_size_t copied;
        char item_buffer[256];
        cass_value_get_string(item_value, item_buffer, sizeof(item_buffer), &copied);
        item_buffer[copied] = '\0';
        printf("item: %s\n", item_buffer);
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

int
main() {
  cass_code_t rc = 0;
  cass_session_t* session = NULL;
  const char* contact_points[] = { "127.0.0.1", NULL };
  const char* items[] = { "apple", "orange", "banana", "mango", NULL };

  rc = connect_session(contact_points, &session);
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


  cass_session_free(session);

  return 0;
}
