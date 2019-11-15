# Authentication

Clients that require authentication when connecting to a secured DSE cluster
(using `com.datastax.bdp.cassandra.auth.DseAuthenticator`) should use the
following functions:

* Plain-text authentication: `cass_cluster_set_dse_plaintext_authenticator()`
* GSSAPI authentication: `cass_cluster_set_dse_gssapi_authenticator()`

```c
CassCluster* cluster = cass_cluster_new();

/* A DSE cluster using plain-text authentication would use: */
cass_cluster_set_dse_plaintext_authenticator(cluster, "cassandra", "cassandra");

/* A DSE cluster using GSSAPI authentication would use: */
cass_cluster_set_dse_gssapi_authenticator(cluster, "dse", "cassandra@DATASTAX.COM");

/* ... */

cass_cluster_free(cluster);
```

## Proxy Execution
Proxy execution allows a client to connect to a node as one user but run queries as a different user.

For example, if a webapp accesses DSE as the 'service' user, but needs to issue some queries as end user 'bob',
a DSE admin would first set up permissions in DSE to allow the 'service' user to execute queries as 'bob':

```
GRANT PROXY.EXECUTE ON ROLE bob TO service;
```

To run a statement as 'bob', the client simply sets the "execute-as" attribute on the statement and executes as usual:

```c
void execute_as(CassSession* session) {
  CassStatement* statement = cass_statement_new("SELECT * FROM ...", 0);

  cass_statement_set_execute_as(statement, "bob");

  CassFuture* future = cass_session_execute(session, statement);

  /* ... */

  cass_future_free(future);
  cass_statement_free(statement);
}
```

## Proxy Authentication
Proxy authentication allows a client to connect to a node as one user but declare that all actions of the client should
actually run as a different user (without needing credentials of that second user).

For example, if a webapp accesses DSE as the 'service' user, but needs to issue all queries as end user 'bob',
a DSE admin would first set up permissions in DSE to allow the 'service' user to proxy login as 'bob':

```
GRANT PROXY.LOGIN ON ROLE bob TO service;
```

Then the client authenticates with DSE as follows:

```c
CassCluster* cluster = cass_cluster_new();

/* A DSE cluster using plain-text authentication would use: */
cass_cluster_set_dse_plaintext_authenticator_proxy(cluster, "service", "service-password", "bob");

/* A DSE cluster using GSSAPI authentication would use: */
cass_cluster_set_dse_gssapi_authenticator_proxy(cluster, "dse", "service@DATASTAX.COM", "bob");

/* ... */

cass_cluster_free(cluster);
```

Note that if DSE is set up to leverage multiple authentication systems, the authenticated user may come from one system
(e.g. Kerberos) and the proxied user from another (e.g. internal).


## Kerberos Implementations

The driver uses the GSSAPI interface to interact with Kerberos and has been
tested using the following GSSAPI Kerberos implementations:

* MIT Kerberos - https://web.mit.edu/kerberos/ (includes Kerberos for Windows)
* Heimdal Kerberos - https://www.h5l.org/

Although the driver uses a common interface (GSSAPI) to interface with Kerberos,
each implementation varies slightly in its configuration and credentials
management. Please refer to the documentation of your specific implementation.

## Kerberos Configuration

MIT and Heimdal Kerberos use a configuration file (usually named `krb5.conf`) to
specify the settings for your application's specific Kerberos setup. This file
can either reside in the default location for your Kerberos implementation or it
can be specified using the `KRB5_CONFIG` environment variable.

## Kerberos Credential Cache

Kerberos credentials can reside in either a credential cache or a keytab (see
below for more information). The management of the credential cache is handled
by utilities and libraries provided by a specific Kerberos implementation and
is not handled by the C/C++ driver. MIT and Heimdal provide similar utilities for
credential cache management. The following examples work for both
implementations.

A credential can be added to the cache using `kinit`. `KRB5_CONFIG` will refer
to the path of your application's `krb5.conf` file and `cassandra@DATASTAX.COM`
should be replaced with your application's specific principal name.

```
$ KRB5_CONFIG=/path/to/krb5.conf kinit cassandra@DATASTAX.COM
Password for cassandra@DATASTAX.COM: <enter password>
```

After a credential is added to the credential cache it can be viewed using
`klist`. The output may look different on your platform.

```
$ klist
Credentials cache: API:501
        Principal: cassandra@DATASTAX.COM

  Issued                Expires               Principal
Jul  5 15:55:18 2017  Jul  6 15:55:15 2017  krbtgt/DATASTAX.COM@DATASTAX.COM
```

The environment variable `KRB5CCNAME` can be used to change the type and value of
your credential cache. This variable uses the format `<type>:<value>` where the
type of the credential cache and the value are separated using a colon e.g
`FILE:/path/to/credcache`, `DIR:/some/dir`, etc. Please refer to your Kerberos
implementation's documentation for more information on the supported types and
values.

## Kerberos Client Keytabs

A client-side keytab can be used to authenticate with Kerberos without having to
populate the credential cache and without requiring a password. A keytab is
specified by using an environment variable. The name of the environment variable
varies by implementation.  Heimdal uses the environment variable `KRB5_KTNAME`
to specify the keytab and MIT uses `KRB5_CLIENT_KTNAME`. Like `KRB5CCNAME` it
uses the form `<type>:<value>` where the type and the value of the keytab are
separated by a colon e.g. `FILE:/path/to/keytab`, `DIR:/some/dir`, etc. Please
refer to your Kerberos implementation's documentation for more information on
the supported types and values.

When using MIT Kerberos your application could use the following to specify a
client-side keytab:

```
$ KRB5_CONFIG=/path/to/krb5.conf KRB5_CLIENT_KTNAME=/path/to/keytab /your/application/exe
```

Or for Heimdal your application would use:

```
$ KRB5_CONFIG=/path/to/krb5.conf KRB5_KTNAME=/path/to/keytab /your/application/exe
```

*Important*: A keytab can be used to authenticate with Kerberos without
requiring any additional credentials or a password therefore it is important
that a keytab have its permissions set properly to restrict access.
