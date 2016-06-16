# Authentication

Clients that require authentication when connecting to a secured DSE cluster
(using `com.datastax.bdp.cassandra.auth.DseAuthenticator`) should use the
following functions:

* Plain-text authentication: `cass_cluster_set_dse_plaintext_authenticator()`
* GSSAPI authentication: `cass_cluster_set_dse_gssapi_authenticator()`

```c
/* A DSE cluster using plain-text authentication would use: */
cass_cluster_set_dse_plaintext_authenticator(cluster, "cassandra", "cassandra");

/* A DSE cluster using GSSAPI authentication would use: */
cass_cluster_set_dse_gssapi_authenticator(cluster, "dse", "cassandra@DATASTAX.COM");
```
