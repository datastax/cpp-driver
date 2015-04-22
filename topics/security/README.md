# Security

The driver currently supports plain text authentication and SSL (via OpenSSL).

## Authentication

Authentication allows an application to supply credentials that can be used to control access to Cassandra resources. The driver currently supports plain text authentication so it is best used in conjunction with SSL. Future release may relax this constraint with the use of SASL authentication. Credentials can be passed using the following:

```c
CassCluster* cluster = cass_cluster_new();

const char* username = "username1";
const char* password = "password1";

cass_cluster_set_credentials(cluster, username, password);

/* Connect session object */

cass_cluster_free(cluster);
```
