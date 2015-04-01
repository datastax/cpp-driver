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
#include <openssl/ssl.h>
#include <cassandra.h>

int load_trusted_cert_file(const char* file, CassSsl* ssl) {
  CassError rc;
  char* cert;
  long cert_size;
  size_t bytes_read;

  FILE *in = fopen(file, "rb");
  if (in == NULL) {
    fprintf(stderr, "Error loading certificate file '%s'\n", file);
    return 0;
  }

  fseek(in, 0, SEEK_END);
  cert_size = ftell(in);
  rewind(in);

  cert = (char*)malloc(cert_size);
  bytes_read = fread(cert, 1, cert_size, in);
  fclose(in);

  if (bytes_read == (size_t) cert_size) {
    rc = cass_ssl_add_trusted_cert_n(ssl, cert, cert_size);
    if (rc != CASS_OK) {
      fprintf(stderr, "Error loading SSL certificate: %s\n", cass_error_desc(rc));
      free(cert);
      return 0;
    }
  }

  free(cert);
  return 1;
}

int main() {
  /* Setup and connect to cluster */
  CassFuture* connect_future = NULL;
  CassCluster* cluster = cass_cluster_new();
  CassSession* session = cass_session_new();
  CassSsl* ssl = cass_ssl_new();

  cass_cluster_set_contact_points(cluster, "127.0.0.1");

  /* Only verify the certification and not the identity */
  cass_ssl_set_verify_flags(ssl, CASS_SSL_VERIFY_PEER_CERT);

  if (!load_trusted_cert_file("cert.pem", ssl)) {
    fprintf(stderr, "Failed to load certificate disabling peer verification\n");
    cass_ssl_set_verify_flags(ssl, CASS_SSL_VERIFY_NONE);
  }

  cass_cluster_set_ssl(cluster, ssl);

  connect_future = cass_session_connect(session, cluster);

  if (cass_future_error_code(connect_future) == CASS_OK) {
    CassFuture* close_future = NULL;

    /* Build statement and execute query */
    const char* query = "SELECT keyspace_name "
                        "FROM system.schema_keyspaces;";
    CassStatement* statement = cass_statement_new(query, 0);

    CassFuture* result_future = cass_session_execute(session, statement);

    if (cass_future_error_code(result_future) == CASS_OK) {
      /* Retrieve result set and iterator over the rows */
      const CassResult* result = cass_future_get_result(result_future);
      CassIterator* rows = cass_iterator_from_result(result);

      while (cass_iterator_next(rows)) {
        const CassRow* row = cass_iterator_get_row(rows);
        const CassValue* value = cass_row_get_column_by_name(row, "keyspace_name");

        const char* keyspace_name;
        size_t keyspace_name_length;
        cass_value_get_string(value, &keyspace_name, &keyspace_name_length);
        printf("keyspace_name: '%.*s'\n", (int)keyspace_name_length,
                                               keyspace_name);
      }

      cass_result_free(result);
      cass_iterator_free(rows);
    } else {
      /* Handle error */
      const char* message;
      size_t message_length;
      cass_future_error_message(result_future, &message, &message_length);
      fprintf(stderr, "Unable to run query: '%.*s'\n", (int)message_length,
                                                            message);
    } 

    cass_statement_free(statement);
    cass_future_free(result_future);

    /* Close the session */
    close_future = cass_session_close(session);
    cass_future_wait(close_future);
    cass_future_free(close_future);
  } else {
      /* Handle error */
      const char* message;
      size_t message_length;
      cass_future_error_message(connect_future, &message, &message_length);
      fprintf(stderr, "Unable to connect: '%.*s'\n", (int)message_length,
                                                          message);
  }

  cass_future_free(connect_future);
  cass_cluster_free(cluster);
  cass_session_free(session);
  cass_ssl_free(ssl);

  return 0;
}
