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

#include <dse.h>
#include <stdio.h>

/*
 * This example program assumes that the following setup is done in DSE apriori:
 *
 * 1. DSE is configured to authenticate with internal authentication or LDAP.
 * 2. Using cqlsh as an administrator user (e.g. cassandra), create the following
 *    objects and grant permissions for them:

      CREATE ROLE target_user WITH PASSWORD = 'target_user' and LOGIN = true;
      CREATE ROLE service_user WITH PASSWORD = 'service_user' and LOGIN = true;
      CREATE KEYSPACE examples WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor':
 1}; CREATE TABLE examples.proxy_execution (f1 int PRIMARY KEY, f2 int); INSERT INTO
 examples.proxy_execution (f1, f2) VALUES (1, 2); GRANT ALL ON examples.proxy_execution TO
 target_user;

      GRANT PROXY.EXECUTE ON ROLE 'target_user' to 'service_user';
 * 3. Verify that service_user cannot query examples.proxy_execution (in cqlsh).
 */

void print_error(CassFuture* future) {
  const char* message;
  size_t message_length;
  cass_future_error_message(future, &message, &message_length);
  fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
}

CassError select_and_dump(CassSession* session, const char* execute_as) {
  CassError rc = CASS_OK;
  CassStatement* statement = NULL;
  CassFuture* future = NULL;
  const char* query = "SELECT * FROM examples.proxy_execution";

  statement = cass_statement_new(query, 0);
  if (execute_as != NULL) {
    cass_statement_set_execute_as(statement, execute_as);
  }
  future = cass_session_execute(session, statement);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  } else {
    const CassResult* result = cass_future_get_result(future);
    CassIterator* iterator = cass_iterator_from_result(result);

    if (cass_iterator_next(iterator)) {
      int f1, f2;
      const CassRow* row = cass_iterator_get_row(iterator);
      if (cass_value_get_int32(cass_row_get_column(row, 0), &f1) != CASS_OK ||
          cass_value_get_int32(cass_row_get_column(row, 1), &f2) != CASS_OK) {
        print_error(future);
      } else {
        printf("f1: %d    f2: %d\n", f1, f2);
      }
    }

    cass_result_free(result);
    cass_iterator_free(iterator);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

CassError connect_session(CassSession* session, const CassCluster* cluster) {
  CassError rc = CASS_OK;
  CassFuture* future = cass_session_connect(session, cluster);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  }
  cass_future_free(future);

  return rc;
}

int main(int argc, char* argv[]) {
  /* Setup and connect to cluster */
  CassCluster* cluster = cass_cluster_new();
  CassSession* session = cass_session_new();
  char* hosts = "127.0.0.1";
  if (argc > 1) {
    hosts = argv[1];
  }

  /* Enable info logging if desired. */
  /* cass_log_set_level(CASS_LOG_INFO); */

  /* Add contact points */
  cass_cluster_set_contact_points(cluster, hosts);

  /* Authenticate as the service_user */
  cass_cluster_set_dse_plaintext_authenticator(cluster, "service_user", "service_user");

  if (connect_session(session, cluster) != CASS_OK) {
    cass_cluster_free(cluster);
    cass_session_free(session);
    return -1;
  }

  printf("Running a query without a proxy user should fail:\n");
  select_and_dump(session, NULL);
  printf("\nRunning a query with proxy user 'target_user' should succeed:\n");
  select_and_dump(session, "target_user");

  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;
}
