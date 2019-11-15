# Client Configuration

Client configuration allows an application to provide additional metadata to
the cluster which can be useful for troubleshooting and performing diagnostics.
In addition to the optional application metadata the cluster will automatically
be provided with the driver's name, driver's version, and a unique session
identifier.

## Application Options (Optional)

Application name and version metadata can be provided to the cluster during
configuration. This information can be used to isolate specific applications on
the server-side when troubleshooting or performing diagnostics on clusters that
support multiple applications.

```c
CassCluster* cluster = cass_cluster_new();

/* Assign a name for the application connecting to the cluster */
cass_cluster_set_application_name(cluster, "Application Name");

/* Assign a version for the application connecting to the cluster */
cass_cluster_set_application_version(cluster, "1.0.0");

/* ... */

cass_cluster_free(cluster);
```

## Client Identification

Each session is assigned a unique identifier (UUID) which can be used to
identify specific client connections server-side. The identifier can also be
retrieved client-side using the following function:

```c
CassSession* session = cass_session_new();

/* Retrieve the session's unique identifier */
CassUuid client_id = cass_session_get_client_id(session);

/* ... */

cass_session_free(session);
```

**Note**: A session's unique identifier is constant for its lifetime and does
          not change when re-establishing connection to a cluster.
