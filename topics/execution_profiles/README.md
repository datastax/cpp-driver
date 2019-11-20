# Execution Profiles

Execution profiles provide a mechanism to group together a set of configuration
options and reuse them across different query executions. These options include:

* Request timeout
* Consistency and serial consistency level
* Load balancing policy (*)
* Retry Policy

__*__ - Load balancing policies are disabled by default and must be explicity
        enabled for child policy settings to be applied (e.g. Token aware,
        latency aware, and filtering options)

Execution profiles are being introduced to help deal with the exploding number
of configuration options, especially as the database platform evolves into more
complex workloads. The number of options being introduced with the execution
profiles is limited and may be expanded based on feedback from the community.

## Creating Execution Profiles

An execution profile must be associated with a cluster and will be made
available for that session connection to attach to any statement before query
execution.

```c
/* Create a cluster object */
CassCluster* cluster = cass_cluster_new();

/* Create a new execution profile */
CassExecProfile* exec_profile = cass_execution_profile_new();

/* Set execution profile options */
cass_execution_profile_set_request_timeout(exec_profile,
                                           120000); /* 2 min timeout */
cass_execution_profile_set_consistency(exec_profile,
                                       CASS_CONSISTENCY_ALL);

/* Associate the execution profile with the cluster configuration */
cass_cluster_set_execution_profile(cluster,
                                   "long_query",
                                   exec_profile);

/* Execution profile may be freed once added to cluster configuration */
cass_execution_profile_free(exec_profile);

/* Provide the cluster object as configuration to connect the session */
```

The cluster configuration options will be used in place of any unassigned
options after a connection is established. Once the execution profile is added
to a cluster configuration it is immutable and any changes made will require
the execution profile to be re-added before a session is connected in order for
those settings to be available during query execution.

__Note__: There is no limit on how many execution profiles can be associated
          with a cluster/session; however the control connection may require
          more time to update the additional load balancing policies.

## Using Execution Profiles

Execution profiles are copied from the cluster object to the session object
during the session connection process.. To use an execution profile the name
must be assigned to a statement.

```c
void execute_with_a_profile(CassSession* session) {
  CassStatement* statement = cass_statement_new("SELECT * FROM ...", 0);

  /* OR create a prepared statement */

  /* Assign the execution profile to the statement */
  cass_statement_set_execution_profile(statement, "long_query");

  /* Execute the statement */
  CassFuture* query_future = cass_session_execute(session, statement);

  /* ... */

  cass_future_free(query_future);
  cass_statement_free(statement);
}
```


__Note__: Use `cass_batch_set_execution_profile(batch, "name")` for batch
          statements.

## Using the Default Cluster Configuration Options

For statements that do not have an assigned execution profile, the default
cluster configuration options will be used. Those statements that have already
defined an execution profile and are being re-used can pass a `NULL` or empty
string `""` when assigning the execution profile.

```c
CassStatement* statement = cass_statement_new("SELECT * FROM ...", 0);

/* Remove the assigned execution profile */
cass_statement_set_execution_profile(statement, NULL);

/* ... */

cass_statement_free(statement);
```
