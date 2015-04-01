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

struct Pair_ {
    const char* key;
    const char* value;
};

typedef struct Pair_ Pair;

void print_error(CassFuture* future) {
  const char* message;
  size_t message_length;
  cass_future_error_message(future, &message, &message_length);
  fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
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

CassError prepare_insert_into_batch(CassSession* session, const CassPrepared** prepared) {
  CassError rc = CASS_OK;
  CassFuture* future = NULL;
  const char* query = "INSERT INTO examples.pairs (key, value) VALUES (?, ?)";

  future = cass_session_prepare(session, query);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  } else {
    *prepared = cass_future_get_prepared(future);
  }

  cass_future_free(future);

  return rc;
}

CassError insert_into_batch_with_prepared(CassSession* session, const CassPrepared* prepared, const Pair* pairs) {
  CassError rc = CASS_OK;
  CassFuture* future = NULL;
  CassBatch* batch = cass_batch_new(CASS_BATCH_TYPE_LOGGED);

  const Pair* pair;
  for (pair = pairs; pair->key != NULL; pair++) {
    CassStatement* statement = cass_prepared_bind(prepared);
    cass_statement_bind_string(statement, 0, pair->key);
    cass_statement_bind_string(statement, 1, pair->value);
    cass_batch_add_statement(batch, statement);
    cass_statement_free(statement);
  }

  {
    CassStatement* statement = cass_statement_new("INSERT INTO examples.pairs (key, value) VALUES ('c', '3')", 0);
    cass_batch_add_statement(batch, statement);
    cass_statement_free(statement);
  }

  {
    CassStatement* statement = cass_statement_new("INSERT INTO examples.pairs (key, value) VALUES (?, ?)", 2);
    cass_statement_bind_string(statement, 0, "d");
    cass_statement_bind_string(statement, 1, "4");
    cass_batch_add_statement(batch, statement);
    cass_statement_free(statement);
  }

  future = cass_session_execute_batch(session, batch);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  }

  cass_future_free(future);
  cass_batch_free(batch);

  return rc;
}


int main() {
  CassCluster* cluster = create_cluster();
  CassSession* session = cass_session_new();
  CassFuture* close_future = NULL;
  const CassPrepared* prepared = NULL;

  Pair pairs[] = { {"a", "1"}, {"b", "2"}, { NULL, NULL} };

  if (connect_session(session, cluster) != CASS_OK) {
    cass_cluster_free(cluster);
    cass_session_free(session);
    return -1;
  }

  execute_query(session,
                "CREATE KEYSPACE examples WITH replication = { \
                           'class': 'SimpleStrategy', 'replication_factor': '3' };");


  execute_query(session,
                "CREATE TABLE examples.pairs (key text, \
                                              value text, \
                                              PRIMARY KEY (key));");

  if (prepare_insert_into_batch(session, &prepared) == CASS_OK) {
    insert_into_batch_with_prepared(session, prepared, pairs);
  }

  cass_prepared_free(prepared);

  close_future = cass_session_close(session);
  cass_future_wait(close_future);
  cass_future_free(close_future);

  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;
}
