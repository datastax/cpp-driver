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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "cassandra.h"

#define NUM_CONCURRENT_REQUESTS 1000

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

void insert_into_async(CassSession* session, const char* key) {
  CassError rc = CASS_OK;
  CassStatement* statement = NULL;
  const char* query = "INSERT INTO async (key, bln, flt, dbl, i32, i64) VALUES (?, ?, ?, ?, ?, ?);";

  CassFuture* futures[NUM_CONCURRENT_REQUESTS];

  size_t i;
  for (i = 0; i < NUM_CONCURRENT_REQUESTS; ++i) {
    char key_buffer[64];
    statement = cass_statement_new(query, 6);

    sprintf(key_buffer, "%s%u", key, (unsigned int)i);
    cass_statement_bind_string(statement, 0, key_buffer);
    cass_statement_bind_bool(statement, 1, i % 2 == 0 ? cass_true : cass_false);
    cass_statement_bind_float(statement, 2, i / 2.0f);
    cass_statement_bind_double(statement, 3, i / 200.0);
    cass_statement_bind_int32(statement, 4, (cass_int32_t)(i * 10));
    cass_statement_bind_int64(statement, 5, (cass_int64_t)(i * 100));

    futures[i] = cass_session_execute(session, statement);

    cass_statement_free(statement);
  }

  for (i = 0; i < NUM_CONCURRENT_REQUESTS; ++i) {
    CassFuture* future = futures[i];

    cass_future_wait(future);

    rc = cass_future_error_code(future);
    if (rc != CASS_OK) {
      print_error(future);
    }

    cass_future_free(future);
  }
}

int main(int argc, char* argv[]) {
  CassCluster* cluster = NULL;
  CassSession* session = cass_session_new();
  char* hosts = "127.0.0.1";
  if (argc > 1) {
    hosts = argv[1];
  }
  cluster = create_cluster(hosts);

  if (connect_session(session, cluster) != CASS_OK) {
    cass_cluster_free(cluster);
    cass_session_free(session);
    return -1;
  }

  execute_query(session, "CREATE KEYSPACE examples WITH replication = { \
                           'class': 'SimpleStrategy', 'replication_factor': '3' };");

  execute_query(session, "CREATE TABLE examples.async (key text, \
                                              bln boolean, \
                                              flt float, dbl double,\
                                              i32 int, i64 bigint, \
                                              PRIMARY KEY (key));");

  execute_query(session, "USE examples");

  insert_into_async(session, "test");

  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;
}
