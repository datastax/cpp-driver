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

#include <stdio.h>
#include <openssl/ssl.h>
#include <cassandra.h>

int load_trusted_cert_file(const char* file, CassSsl* ssl) {
  CassError rc;
  char* cert;
  long cert_size;

  FILE *in = fopen(file, "rb");
  if (in == NULL) {
    fprintf(stderr, "Error loading certificate file '%s'\n", file);
    return 0;
  }

  fseek(in, 0, SEEK_END);
  cert_size = ftell(in);
  rewind(in);

  cert = (char*)malloc(cert_size);
  fread(cert, sizeof(char), cert_size, in);
  fclose(in);

  rc = cass_ssl_add_trusted_cert(ssl, cass_string_init2(cert, cert_size));
  if (rc != CASS_OK) {
    fprintf(stderr, "Error loading SSL certificate: %s\n", cass_error_desc(rc));
    free(cert);
    return 0;
  }

  free(cert);
  return 1;
}

int main() {
  /* Setup and connect to cluster */
  CassFuture* connect_future = NULL;
  CassCluster* cluster = cass_cluster_new();
  CassSsl* ssl = cass_ssl_new();

  cass_cluster_set_contact_points(cluster, "127.0.0.1");

  /* Only verify the certification and not the identity */
  cass_ssl_set_verify_flags(ssl, CASS_SSL_VERIFY_PEER_CERT);

  if(!load_trusted_cert_file("cert.pem", ssl)) {
    fprintf(stderr, "Failed to load certificate disabling peer verification\n");
    cass_ssl_set_verify_flags(ssl, CASS_SSL_VERIFY_NONE);
  }

  cass_cluster_set_ssl(cluster, ssl);

  connect_future = cass_cluster_connect(cluster);

  if (cass_future_error_code(connect_future) == CASS_OK) {
    CassFuture* close_future = NULL;
    CassSession* session = cass_future_get_session(connect_future);

    /* Build statement and execute query */
    CassString query = cass_string_init("SELECT keyspace_name "
                                        "FROM system.schema_keyspaces;");
    CassStatement* statement = cass_statement_new(query, 0);

    CassFuture* result_future = cass_session_execute(session, statement);

    if(cass_future_error_code(result_future) == CASS_OK) {
      /* Retrieve result set and iterator over the rows */
      const CassResult* result = cass_future_get_result(result_future);
      CassIterator* rows = cass_iterator_from_result(result);

      while(cass_iterator_next(rows)) {
        const CassRow* row = cass_iterator_get_row(rows);
        const CassValue* value = cass_row_get_column_by_name(row, "keyspace_name");

        CassString keyspace_name;
        cass_value_get_string(value, &keyspace_name);
        printf("keyspace_name: '%.*s'\n", (int)keyspace_name.length,
                                               keyspace_name.data);
      }

      cass_result_free(result);
      cass_iterator_free(rows);
    } else {
      /* Handle error */
      CassString message = cass_future_error_message(result_future);
      fprintf(stderr, "Unable to run query: '%.*s'\n", (int)message.length,
                                                            message.data);
    } 

    cass_statement_free(statement);
    cass_future_free(result_future);

    /* Close the session */
    close_future = cass_session_close(session);
    cass_future_wait(close_future);
    cass_future_free(close_future);
  } else {
      /* Handle error */
      CassString message = cass_future_error_message(connect_future);
      fprintf(stderr, "Unable to connect: '%.*s'\n", (int)message.length, 
                                                          message.data);
  }

  cass_future_free(connect_future);
  cass_cluster_free(cluster);
  cass_ssl_free(ssl);

  return 0;
}
