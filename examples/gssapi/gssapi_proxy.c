/*
  Copyright (c) 2017 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include <dse.h>
#include <stdio.h>

/*
 * This example program assumes that the following setup is done in DSE apriori:
 *
 * 1. DSE is configured to authenticate with Kerberos.
 * 2. Using cqlsh as an administrator user (e.g. cassandra), create the following
 *    objects and grant permissions for them:
 *
      CREATE ROLE target_user WITH PASSWORD = 'target_user' and LOGIN = true;
      CREATE KEYSPACE examples WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
      CREATE TABLE examples.gss_proxy_auth (f1 int PRIMARY KEY, f2 int);
      INSERT INTO examples.gss_proxy_auth (f1, f2) VALUES (1, 2);
      GRANT ALL ON examples.gss_proxy_auth TO target_user;

      GRANT PROXY.LOGIN ON ROLE 'target_user' to 'dseuser@DATASTAX.COM';
 *
 * Substitute your own Kerberos user for 'dseuser@DATASTAX.COM' (in the above cql and
 * the KERBEROS_USER macro below).
 *
 * Note that proxy auth can target an internal user (e.g. target_user) even if the authenticated
 * user is from Kerberos.
 */

#define KERBEROS_USER "dseuser@DATASTAX.COM"

void print_error(CassFuture* future) {
  const char* message;
  size_t message_length;
  cass_future_error_message(future, &message, &message_length);
  fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
}

CassError select_and_dump(CassSession* session) {
  CassError rc = CASS_OK;
  CassStatement* statement = NULL;
  CassFuture* future = NULL;
  const char* query = "SELECT * FROM examples.gss_proxy_auth";

  statement = cass_statement_new(query, 0);
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

void connect_and_run(const char* hosts, const char* proxy_user) {
  CassCluster* cluster = cass_cluster_new();
  CassSession* session = cass_session_new();

  /* Add contact points */
  cass_cluster_set_contact_points(cluster, hosts);

  /* Hostname resolution is typically necessary when authenticating with Kerberos. */
  cass_cluster_set_use_hostname_resolution(cluster, cass_true);

  /* Authenticate as the Kerberos user. If proxy_user is non-null,
   * declare that we want to execute all statements as proxy_user. */
  if (proxy_user == NULL) {
    cass_cluster_set_dse_gssapi_authenticator(cluster, "dse", KERBEROS_USER);
  } else {
    cass_cluster_set_dse_gssapi_authenticator_proxy(cluster, "dse", KERBEROS_USER, "target_user");
  }

  if (connect_session(session, cluster) != CASS_OK) {
    cass_cluster_free(cluster);
    cass_session_free(session);
    return;
  }

  select_and_dump(session);

  cass_cluster_free(cluster);
  cass_session_free(session);
}

int main(int argc, char* argv[]) {
  /* Setup and connect to cluster */
  CassFuture* connect_future = NULL;
  char* hosts = "127.0.0.1";
  if (argc > 1) {
    hosts = argv[1];
  }

  /* Enable info logging if desired. */
  /* cass_log_set_level(CASS_LOG_INFO); */

  printf("Running a query without a proxy user should fail:\n");
  connect_and_run(hosts, NULL);
  printf("\nRunning a query with proxy user 'target_user' should succeed:\n");
  connect_and_run(hosts, "target_user");

  return 0;
}
