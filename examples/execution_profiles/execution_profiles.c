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

void print_error_description(CassError rc) {
  fprintf(stderr, "Error Description: %s\n", cass_error_desc(rc));
}

void print_error(CassFuture* future) {
  const char* message;
  size_t message_length;
  cass_future_error_message(future, &message, &message_length);
  fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
}

CassExecProfile* create_quorum_execution_profile() {
  CassExecProfile* profile = cass_execution_profile_new();
  cass_execution_profile_set_consistency(profile, CASS_CONSISTENCY_QUORUM);
  cass_execution_profile_set_request_timeout(profile, 300000); /* Five minute request timeout */
  return profile;
}

CassExecProfile* create_reduced_latency_write_execution_profile() {
  CassExecProfile* profile = cass_execution_profile_new();
  cass_execution_profile_set_load_balance_round_robin(profile);
  cass_execution_profile_set_token_aware_routing(profile, cass_true);
  cass_execution_profile_set_consistency(profile, CASS_CONSISTENCY_ANY);
  return profile;
}

CassCluster* create_cluster(const char* hosts) {
  CassCluster* cluster = cass_cluster_new();
  cass_cluster_set_contact_points(cluster, hosts);
  return cluster;
}

CassError connect_session(CassSession* session, const CassCluster* cluster) {
  /* Provide the cluster object as configuration to connect the session */
  CassError rc = CASS_OK;
  CassFuture* future = cass_session_connect(session, cluster);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    /* Handle error */
    print_error(future);
  }
  cass_future_free(future);

  return rc;
}

CassError execute_query(CassSession* session, const char* query) {
  /* Build statement and execute query */
  CassError rc = CASS_OK;
  CassFuture* future = NULL;
  CassStatement* statement = cass_statement_new(query, 0);
  future = cass_session_execute(session, statement);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    /* Handle error */
    print_error(future);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

CassError insert_into_examples(CassSession* session,
                               const char* profile_name,
                               const char* key,
                               const cass_bool_t value) {
  CassError rc = CASS_OK;
  CassStatement* statement = NULL;
  CassFuture* future = NULL;
  const char* query = "INSERT INTO examples.execution_profiles \
                        (key, value) VALUES (?, ?)";

  /* Build statement and execute query */
  statement = cass_statement_new(query, 2);
  if (profile_name != NULL) {
    rc = cass_statement_set_execution_profile(statement, profile_name);
    if (rc != CASS_OK) {
      print_error_description(rc);
    }
  }
  cass_statement_set_keyspace(statement, "execution_profiles");
  cass_statement_add_key_index(statement, 0);
  cass_statement_bind_string(statement, 0, key);
  cass_statement_bind_bool(statement, 1, value);
  future = cass_session_execute(session, statement);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    /* Handle error */
    print_error(future);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

CassError select_from_examples(CassSession* session,
                               const char* profile_name,
                               const char* key,
                               cass_bool_t* return_value) {
  CassError rc = CASS_OK;
  CassStatement* statement = NULL;
  CassFuture* future = NULL;

  /* Build statement and execute query */
  const char* query = "SELECT * FROM examples.execution_profiles WHERE key = ?";
  statement = cass_statement_new(query, 1);
  if (profile_name != NULL) {
    rc = cass_statement_set_execution_profile(statement, profile_name);
    if (rc != CASS_OK) {
      print_error_description(rc);
    }
  }
  cass_statement_bind_string(statement, 0, key);
  future = cass_session_execute(session, statement);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    /* Handle error */
    print_error(future);
  } else {
    /* Retrieve result set and get the first row */
    const CassResult* result = cass_future_get_result(future);
    const CassRow* row = cass_result_first_row(result);
    if (row) {
      const CassValue* value = cass_row_get_column_by_name(row, "value");
      cass_value_get_bool(value, return_value);
      printf("SELECT: Key = %s | Value = %s\n",
             key,
             (*return_value == cass_true ? "true" : "false"));
    }

    cass_result_free(result);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

int main(int argc, char* argv[]) {
  /* Setup default objects */
  CassCluster* cluster = NULL;
  CassSession* session = cass_session_new();
  CassFuture* close_future = NULL;
  CassExecProfile* profile = NULL;
  cass_bool_t value = cass_false;
  const char* hosts = "127.0.0.1,127.0.0.2,127.0.0.3";

  /* Add contact points and create the cluster */
  if (argc > 1) {
    hosts = argv[1];
  }
  cluster = create_cluster(hosts);

  /* Create and set execution profiles; freeing once added to configuration */
  profile = create_reduced_latency_write_execution_profile();
  cass_cluster_set_execution_profile(cluster,
                                     "reduced_latency",
                                     profile);
  cass_execution_profile_free(profile);
  profile = create_quorum_execution_profile();
  cass_cluster_set_execution_profile(cluster,
                                     "quorum",
                                     profile);
  cass_execution_profile_free(profile);

  /* Provide the cluster object as configuration to connect the session */
  if (connect_session(session, cluster) != CASS_OK) {
    cass_cluster_free(cluster);
    cass_session_free(session);
    return -1;
  }

  /* Create a keyspace and table for the execution profile example  */
  execute_query(session,
                "CREATE KEYSPACE IF NOT EXISTS examples WITH replication = { \
                  'class': 'SimpleStrategy', 'replication_factor': '3' \
                }");
  execute_query(session,
                "CREATE TABLE IF NOT EXISTS examples.execution_profiles ( \
                  key text PRIMARY KEY, \
                  value boolean \
                )");

  /* Insert values using 'reduced_latency' profile */
  insert_into_examples(session, "reduced_latency", "one", cass_true);
  insert_into_examples(session, "reduced_latency", "two", cass_false);
  insert_into_examples(session, "reduced_latency", "three", cass_true);
  insert_into_examples(session, "reduced_latency", "four", cass_false);
  insert_into_examples(session, "reduced_latency", "five", cass_true);

  /* Select the values from the cluster using the 'quorum' profile */
  select_from_examples(session, "quorum", "one", &value);
  assert(value == cass_true);
  select_from_examples(session, "quorum", "two", &value);
  assert(value == cass_false);
  select_from_examples(session, "quorum", "three", &value);
  assert(value == cass_true);
  select_from_examples(session, "quorum", "four", &value);
  assert(value == cass_false);
  select_from_examples(session, "quorum", "five", &value);
  assert(value == cass_true);

  close_future = cass_session_close(session);
  cass_future_wait(close_future);
  cass_future_free(close_future);

  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;
}
