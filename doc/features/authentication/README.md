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

## Proxy Execution
Proxy execution allows a client to connect to a node as one user but run queries as a different user.

For example, if a webapp accesses DSE as the 'service' user, but needs to issue some queries as end user 'bob',
a DSE admin would first set up permissions in DSE to allow the 'service' user to execute queries as 'bob':

```
GRANT PROXY.EXECUTE ON ROLE bob TO server;
```

To run a statement as 'bob', the client simply sets the "execute-as" attribute on the statement and executes as usual:

```c
CassStatement* statement; // Previously defined statement.
cass_statement_set_execute_as(statement, "bob");
future = cass_session_execute(session, statement);
```

## Proxy Authorization
Proxy authorization allows a client to connect to a node as one user but declare that all actions of the client should
actually run as a different user (without needing credentials of that second user).

For example, if a webapp accesses DSE as the 'service' user, but needs to issue all queries as end user 'bob',
a DSE admin would first set up permissions in DSE to allow the 'service' user to proxy login as 'bob':

```
GRANT PROXY.LOGIN ON ROLE bob TO server;
```

Then the client authenticates with DSE as follows:
```c
/* A DSE cluster using plain-text authentication would use: */
cass_cluster_set_dse_plaintext_authenticator_proxy(cluster, "server", "server-password", "bob");

/* A DSE cluster using GSSAPI authentication would use: */
cass_cluster_set_dse_gssapi_authenticator_proxy(cluster, "dse", "server@DATASTAX.COM", "bob");
```

Note that if DSE is set up to leverage multiple authentication systems, the authenticated user may come from one system
(e.g. Kerberos) and the proxied user from another (e.g. internal).