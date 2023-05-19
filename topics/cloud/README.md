# Cloud

## Connecting to [DataStax Astra Database-as-a-Service] using a secure connection bundle

   Use the following code snippet to connect your database:

   ```c
   #include <cassandra.h>
   #include <stdio.h>

   int main(int argc, char* argv[]) {
     /* Setup and connect to cluster */
     CassCluster* cluster = cass_cluster_new();
     CassSession* session = cass_session_new();

     /* Setup driver to connect to the cloud using the secure connection bundle */
     const char* secure_connect_bundle = "/path/to/secure-connect-database_name.zip";
     if (cass_cluster_set_cloud_secure_connection_bundle(cluster, secure_connect_bundle) != CASS_OK) {
       fprintf(stderr, "Unable to configure cloud using the secure connection bundle: %s\n",
               secure_connect_bundle);
       return 1;
     }

     /* Set credentials provided when creating your database */
     cass_cluster_set_credentials(cluster, "username", "password");

     CassFuture* connect_future = cass_session_connect(session, cluster);

     if (cass_future_error_code(connect_future) == CASS_OK) {
       /* Use the session to run queries */
     } else {
       /* Handle error */
     }

     cass_future_free(connect_future);
     cass_cluster_free(cluster);
     cass_session_free(session);

     return 0;
   }
   ```

  **Note:** `cass_cluster_set_contact_points()` and `cass_cluster_set_ssl()` should not used
  in conjunction with `cass_cluster_set_cloud_secure_connection_bundle()`.

[DataStax Astra Database-as-a-Service]: https://astra.datastax.com/
