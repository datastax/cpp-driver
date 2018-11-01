# Retry policies

Retry polices allow the driver to automatically handle server-side failures when
Cassandra is unable to fulfill the consistency requirement of a request.

**Important**: Retry policies do not handle client-side failures such as
client-side timeouts or client-side connection issues. In these cases
application code must handle the failure and retry the request. The driver will
automatically recover requests that haven't been written, but once a request is
written the driver will return an error for in-flight requests and will not try
to automatically recover. This is done because not all operations are idempotent
and the driver is unable to distinguish which requests can automatically retried
without side effect. It's up to application code to make this distinction.

## Setting Retry Policy

By default, the driver uses the `default retry policy` for all requests unless
it is overridden. The retry policy can be set globally using
[`cass_cluster_set_retry_policy()`] or it can be set per statement or batch
using [`cass_statement_set_retry_policy()`] or
[`cass_batch_set_retry_policy()`], respectively.

## Default Retry Policy

The default retry policy will only retry a request when it is safe to do so
while preserving the consistency level of the request and it is likely to
succeed. In all other cases, this policy will return an error.

<table class="table table-striped table-hover table-condensed">
  <thead>
  <tr>
   <th>Failure Type</th>
   <th>Action</th>
  </tr>
  </thead>

  <tbody>
  <tr>
   <td>Read Timeout</td>
   <td>Retry if the number of received responses is greater than or equal to the
       number of required responses, but the data was not received. Returns and
       error in all other cases.</td>
  </tr>
  <tr>
   <td>Write Timeout</td>
   <td>Retry only if the request is a logged batch request and the request failed to
       write the batch log. Returns an error in all other cases.</td>
  </tr>
  <tr>
   <td>Unavailable</td>
   <td>Retries the request using the next host in the query plan.</td>
  </tr>
  </tbody>
</table>

```c
CassRetryPolicy* default_policy = cass_retry_policy_default_new();

/* ... */

/* Retry policies must be freed */
cass_retry_policy_free(default_policy);
```

## Downgrading Consistency Retry Policy

**Deprecated:** Please do not use this policy in new applications. The use of
this policy can lead to unexpected behavior. Application errors can happen when
the consistency level is unexpectedly changed because the cluster is in a
degraded state. The assumptions made at the normal operating consistency level
may no longer apply when the consistency level is downgraded. Instead,
applications should always use the lowest consistency that can be tolerated by a
specific use case. The consistency level can be set per cluster using
`cass_cluster_set_consistency()`, per execution profile using
`cass_execution_profile_set_consistency(), or per statement using
`cass_statement_set_consistency()`.

## Fallthrough Retry Policy

This policy never retries or ignores a server-side failures. Errors are always
returned. This policy is useful for application that want to handle retries
directly.

<table class="table table-striped table-hover table-condensed">
  <thead>
   <tr>
   <th>Failure Type</th>
   <th>Action</th>
   </tr>
  </thead>

  <tbody>
  <tr>
   <td>Read Timeout</td>
   <td>Return error</td>
  </tr>
  <tr>
   <td>Write Timeout</td>
   <td>Return error</td>
  </tr>
  <tr>
   <td>Unavailable</td>
   <td>Return error</td>
  </tr>
  </tbody>
</table>

```c
CassRetryPolicy* fallthrough_policy =
cass_retry_policy_fallthrough_new();

/* ... */

/* Retry policies must be freed */
cass_retry_policy_free(fallthrough_policy);
```

## Logging

This policy can be added as a parent policy to all the other polices. It logs
the retry decision of its child policy. The log messages created by this policy
are done using the [`CASS_LOG_INFO`] level.

```c
CassCluster* cluster = cass_cluster_new();

CassRetryPolicy* default_policy = cass_retry_policy_default_new();
CassRetryPolicy* logging_policy = cass_retry_policy_logging_new(default_policy);

cass_cluster_set_retry_policy(cluster, logging_policy);

/* ... */

/* Retry policies must be freed */
cass_retry_policy_free(default_policy);
cass_retry_policy_free(logging_policy);

cass_cluster_free(cluster);
```
[`cass_cluster_set_retry_policy()`]: http://datastax.github.io/cpp-driver/api/struct.CassCluster/#cass-cluster-set-retry-policy
[`cass_statement_set_retry_policy()`]: http://datastax.github.io/cpp-driver/api/struct.CassStatement/#cass-statement-set-retry-policy
[`cass_batch_set_retry_policy()`]: http://datastax.github.io/cpp-driver/api/struct.CassBatch/#cass-batch-set-retry-policy
[`CASS_LOG_INFO`]: http://datastax.github.io/cpp-driver/api/cassandra.h/#cass-log-level
