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

struct Pair_ {
    const char* key;
    const char* value;
};

typedef struct Pair_ Pair;

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

CassError prepare_insert_into_batch(CassSession* session, const CassPrepared** prepared) {
  CassError rc = 0;
  CassFuture* future = NULL;
  CassString query = cass_string_init("INSERT INTO examples.pairs (key, value) VALUES (?, ?)");

  future = cass_session_prepare(session, query);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if(rc != CASS_OK) {
    print_error(future);
  } else {
    *prepared = cass_future_get_prepared(future);
  }

  cass_future_free(future);

  return rc;
}

CassError insert_into_batch_with_prepared(CassSession* session, const CassPrepared* prepared, const Pair* pairs) {
  CassError rc = 0;
  CassFuture* future = NULL;
  CassBatch* batch = cass_batch_new(CASS_CONSISTENCY_ONE);

  const Pair* pair;
  for(pair = pairs; pair->key != NULL; pair++) {
    CassStatement* statement = cass_prepared_bind(prepared, 2, CASS_CONSISTENCY_ONE);
    cass_statement_bind_string(statement, 0, cass_string_init(pair->key));
    cass_statement_bind_string(statement, 1, cass_string_init(pair->value));
    cass_batch_add_statement(batch, statement);
  }

  cass_batch_add_statement(batch,
                           cass_statement_new(cass_string_init("INSERT INTO examples.pairs (key, value) VALUES ('c', '3')"),
                                              0,
                                              CASS_CONSISTENCY_ONE));

  {
    CassStatement* statement = cass_statement_new(cass_string_init("INSERT INTO examples.pairs (key, value) VALUES (?, ?)"),
                                                  2,
                                                  CASS_CONSISTENCY_ONE);
    cass_statement_bind_string(statement, 0, cass_string_init("d"));
    cass_statement_bind_string(statement, 1, cass_string_init("4"));
    cass_batch_add_statement(batch, statement);
  }

  future = cass_session_execute_batch(session, batch);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if(rc != CASS_OK) {
    print_error(future);
  }

  cass_future_free(future);
  cass_batch_free(batch);

  return rc;
}


int main() {
  CassError rc = 0;
  CassCluster* cluster = create_cluster();
  CassSession* session = NULL;
  CassFuture* close_future = NULL;
  const CassPrepared* prepared = NULL;

  Pair pairs[] = { {"a", "1"}, {"b", "2"}, { NULL, NULL} };

  rc = connect_session(cluster, &session);
  if(rc != CASS_OK) {
    return -1;
  }

  execute_query(session,
                "CREATE KEYSPACE examples WITH replication = { \
                           'class': 'SimpleStrategy', 'replication_factor': '3' };");


  execute_query(session,
                "CREATE TABLE examples.pairs (key text, \
                                              value text, \
                                              PRIMARY KEY (key));");

  if(prepare_insert_into_batch(session, &prepared) == CASS_OK) {
    insert_into_batch_with_prepared(session, prepared, pairs);
  }

  close_future = cass_session_close(session);
  cass_future_wait(close_future);
  cass_cluster_free(cluster);

  return 0;
}
