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

#include <stdio.h>
#include <string.h>

#include "cassandra.h"

#define CONCURRENCY_LEVEL 32
#define NUM_REQUESTS 10000

CassSession* session = NULL;
const CassPrepared* prepared = NULL;
CassUuidGen* uuid_gen = NULL;

int num_requests = 0;
int num_outstanding_requests = 0;

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
  CassStatement* statement = cass_statement_new(query, 0);

  CassFuture* future = cass_session_execute(session, statement);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  }

  cass_statement_free(statement);
  cass_future_free(future);
  return rc;
}

CassError prepare_insert(CassSession* session, const CassPrepared** prepared) {
  CassError rc = CASS_OK;
  const char* query = "INSERT INTO examples.concurrent_executions (id, value) VALUES (?, ?);";

  CassFuture* future = cass_session_prepare(session, query);
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

void insert_into_concurrent_executions() {
  CassFuture* futures[CONCURRENCY_LEVEL];
  int num_requests = NUM_REQUESTS;

  while (num_requests > 0) {
    int i;
    int num_outstanding_requests = CONCURRENCY_LEVEL;
    if (num_requests < num_outstanding_requests) {
      num_outstanding_requests = num_requests;
    }
    num_requests -= num_outstanding_requests;

    for (i = 0; i < num_outstanding_requests; ++i) {
      CassUuid uuid;
      char value_buffer[16];
      CassStatement* statement = cass_prepared_bind(prepared);
      cass_statement_set_is_idempotent(statement, cass_true);
      cass_uuid_gen_random(uuid_gen, &uuid);
      cass_statement_bind_uuid_by_name(statement, "id", uuid);
      sprintf(value_buffer, "%d", i);
      cass_statement_bind_string_by_name(statement, "value", value_buffer);

      futures[i] = cass_session_execute(session, statement);
      cass_statement_free(statement);
    }

    for (i = 0; i < num_outstanding_requests; ++i) {
      CassFuture* future = futures[i];
      CassError rc = cass_future_error_code(future);
      if (rc != CASS_OK) {
        print_error(future);
      }
      cass_future_free(future);
    }
  }
}

int main(int argc, char* argv[]) {
  CassCluster* cluster = NULL;
  char* hosts = "127.0.0.1";
  if (argc > 1) {
    hosts = argv[1];
  }
  session = cass_session_new();
  uuid_gen = cass_uuid_gen_new();
  cluster = create_cluster(hosts);

  if (connect_session(session, cluster) != CASS_OK) {
    cass_uuid_gen_free(uuid_gen);
    cass_cluster_free(cluster);
    cass_session_free(session);
    return -1;
  }

  execute_query(session, "CREATE KEYSPACE IF NOT EXISTS examples WITH replication = { \
                                                        'class': 'SimpleStrategy', \
                                                        'replication_factor': '1' }");
  execute_query(session, "CREATE TABLE IF NOT EXISTS examples.concurrent_executions ( \
                                                     id uuid, \
                                                     value text, \
                                                     PRIMARY KEY (id))");

  if (prepare_insert(session, &prepared) == CASS_OK) {
    insert_into_concurrent_executions();
    cass_prepared_free(prepared);
  }

  cass_uuid_gen_free(uuid_gen);
  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;
}
