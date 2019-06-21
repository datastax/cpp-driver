# Configuration

## Load balancing

Load balancing controls how queries are distributed to nodes in a Cassandra
cluster.

Without additional configuration the C/C++ driver defaults to using Datacenter-aware
load balancing with token-aware routing. This means that driver will only send
queries to nodes in the local datacenter (for local consistency levels) and
it will use the primary key of queries to route them directly to the nodes where the
corresponding data is located.

### Round-robin Load Balancing

This load balancing policy equally distributes queries across cluster without
consideration of datacenter locality. This should only be used with
Cassandra clusters where all nodes are located in the same datacenter.

### Datacenter-aware Load Balancing

This load balancing policy equally distributes queries to nodes in the local
datacenter. Nodes in remote datacenters are only used when all local nodes are
unavailable. Additionally, remote nodes are only considered when non-local
consistency levels are used or if the driver is configured to use remote nodes
with the [`allow_remote_dcs_for_local_cl`] setting.

```c
CassCluster* cluster = cass_cluster_new();

const char* local_dc = "dc1"; /* Local datacenter name */

/*
 * Use up to 2 remote datacenter nodes for remote consistency levels
 * or when `allow_remote_dcs_for_local_cl` is enabled.
 */
unsigned used_hosts_per_remote_dc = 2;

/* Don't use remote datacenter nodes for local consistency levels */
cass_bool_t allow_remote_dcs_for_local_cl = cass_false;

cass_cluster_set_load_balance_dc_aware(cluster,
                                       local_dc,
                                       used_hosts_per_remote_dc,
                                       allow_remote_dcs_for_local_cl);

/* ... */

cass_cluster_free(cluster);
```

### Token-aware Routing

Token-aware routing uses the primary key of queries to route requests directly to
the Cassandra nodes where the data is located. Using this policy avoids having
to route requests through an extra coordinator node in the Cassandra cluster. This
can improve query latency and reduce load on the Cassandra nodes. It can be used
in conjunction with other load balancing and routing policies.

```c
CassCluster* cluster = cass_cluster_new();

/* Enable token-aware routing (this is the default setting) */
cass_cluster_set_token_aware_routing(cluster, cass_true);

/* Disable token-aware routing */
cass_cluster_set_token_aware_routing(cluster, cass_false);

/* ... */

cass_cluster_free(cluster);
```

### Latency-aware Routing

Latency-aware routing tracks the latency of queries to avoid sending new queries
to poorly performing Cassandra nodes. It can be used in conjunction with other
load balancing and routing policies.

```c
CassCluster* cluster = cass_cluster_new();

/* Disable latency-aware routing (this is the default setting) */
cass_cluster_set_latency_aware_routing(cluster, cass_false);

/* Enable latency-aware routing */
cass_cluster_set_latency_aware_routing(cluster, cass_true);

/*
 * Configure latency-aware routing settings
 */

/* Up to 2 times the best performing latency is okay */
cass_double_t exclusion_threshold = 2.0;

 /* Use the default scale */
cass_uint64_t scale_ms = 100;

/* Retry a node after 10 seconds even if it was performing poorly before */
cass_uint64_t retry_period_ms = 10000;

/* Find the best performing latency every 100 milliseconds */
cass_uint64_t update_rate_ms = 100;

/* Only consider the average latency of a node after it's been queried 50 times */
cass_uint64_t min_measured = 50;

cass_cluster_set_latency_aware_routing_settings(cluster,
                                                exclusion_threshold,
                                                scale_ms,
                                                retry_period_ms,
                                                update_rate_ms,
                                                min_measured);

/* ... */

cass_cluster_free(cluster);
```

### Filtering policies

#### Whitelist

This policy ensures that only hosts from the provided whitelist filter will
ever be used. Any host that is not contained within the whitelist will be
considered ignored and a connection will not be established.  It can be used in
conjunction with other load balancing and routing policies.

NOTE: Using this policy to limit the connections of the driver to a predefined
      set of hosts will defeat the auto-detection features of the driver. If
      the goal is to limit connections to hosts in a local datacenter use
      DC aware in conjunction with the round robin load balancing policy.

```c
CassCluster* cluster = cass_cluster_new();

/* Set the list of predefined hosts the driver is allowed to connect to */
cass_cluster_set_whitelist_filtering(cluster,
                                     "127.0.0.1, 127.0.0.3, 127.0.0.5");

/* The whitelist can be cleared (and disabled) by using an empty string */
cass_cluster_set_whitelist_filtering(cluster, "");

/* ... */

cass_cluster_free(cluster);
```

#### Blacklist

This policy is the inverse of the whitelist policy where hosts provided in the
blacklist filter will be ignored and a connection will not be established.

```c
CassCluster* cluster = cass_cluster_new();

/* Set the list of predefined hosts the driver is NOT allowed to connect to */
cass_cluster_set_blacklist_filtering(cluster,
                                     "127.0.0.1, 127.0.0.3, 127.0.0.5");

/* The blacklist can be cleared (and disabled) by using an empty string */
cass_cluster_set_blacklist_filtering(cluster, "");

/* ... */

cass_cluster_free(cluster);
```

#### Datacenter

Filtering can also be performed on all hosts in a datacenter or multiple
datacenters when using the whitelist/blacklist datacenter filtering polices.

```c
CassCluster* cluster = cass_cluster_new();

/* Set the list of predefined datacenters the driver is allowed to connect to */
cass_cluster_set_whitelist_dc_filtering(cluster, "dc2, dc4");

/* The datacenter whitelist can be cleared/disabled by using an empty string */
cass_cluster_set_whitelist_dc_filtering(cluster, "");

/* ... */

cass_cluster_free(cluster);
```

```c
CassCluster* cluster = cass_cluster_new();


/* Set the list of predefined datacenters the driver is NOT allowed to connect to */
cass_cluster_set_blacklist_dc_filtering(cluster, "dc2, dc4");

/* The datacenter blacklist can be cleared/disabled by using an empty string */
cass_cluster_set_blacklist_dc_filtering(cluster, "");

/* ... */

cass_cluster_free(cluster);
```

## Speculative Execution

For certain applications it is of the utmost importance to minimize latency.
Speculative execution is a way to minimize latency by preemptively executing
several instances of the same query against different nodes. The fastest
response is then returned to the client application and the other requests are
cancelled. Speculative execution is <b>disabled</b> by default.

### Query Idempotence

Speculative execution will result in executing the same query several times.
Therefore, it is important that queries are idempotent i.e. a query can be
applied multiple times without changing the result beyond the initial
application. <b>Queries that are not explicitly marked as idempotent will not be
scheduled for speculative executions.</b>

The following types of queries are <b>not</b> idempotent:

* Mutation of `counter` columns
* Prepending or appending to a `list` column
* Use of non-idempotent CQL function e.g. `now()` or `uuid()`

The driver is unable to determine if a query is idempotent therefore it is up to
an application to explicitly mark a statement as being idempotent.

```c
CassStatement* statement = cass_statement_new( "SELECT * FROM table1", 0);

/* Make the statement idempotent */
cass_statement_set_is_idempotent(statement, cass_true);

cass_statement_free(statement);
```

### Enabling speculative execution

Speculative execution is enabled by connecting a `CassSession` with a
`CassCluster` that has a speculative execution policy enabled. The driver
currently only supports a constant policy, but may support more in the future.

#### Constant speculative execution policy

The following will start up to 2 more executions after the initial execution
with the subsequent executions being created 500 milliseconds apart.

```c
CassCluster* cluster = cass_cluster_new();

cass_int64_t constant_delay_ms = 500; /* Delay before a new execution is created */
int max_speculative_executions = 2;   /* Number of executions */

cass_cluster_set_constant_speculative_execution_policy(cluster,
                                                       constant_delay_ms,
                                                       max_speculative_executions);

/* ... */

cass_cluster_free(cluster);
```

### Connection Heartbeats

To prevent intermediate network devices (routers, switches, etc.) from
disconnecting pooled connections the driver sends a lightweight heartbeat
request (using an [`OPTIONS`] protocol request) periodically. By default the
driver sends a heartbeat every 30 seconds. This can be changed or disabled (0
second interval) using the following:

```c
CassCluster* cluster = cass_cluster_new();

/* Change the heartbeat interval to 1 minute */
cass_cluster_set_connection_heartbeat_interval(cluster, 60);

/* Disable heartbeat requests */
cass_cluster_set_connection_heartbeat_interval(cluster, 0);

/* ... */

cass_cluster_free(cluster);
```
Heartbeats are also used to detect unresponsive connections. An idle timeout
setting controls the amount of time a connection is allowed to be without a
successful heartbeat before being terminated and scheduled for reconnection. This
interval can be changed from the default of 60 seconds:

```c
CassCluster* cluster = cass_cluster_new();

/* Change the idle timeout to 2 minute */
cass_cluster_set_connection_idle_timeout(cluster, 120);

/* ... */

cass_cluster_free(cluster);
```

It can be disabled by setting the value to a very long timeout or by disabling
heartbeats.

### Host State Changes

The status and membership of a node can change within the life-cycle of the
cluster. A host listener callback can be used to detect these changes.

**Important**: The driver runs the host listener callback on a thread that is
               different from the application. Any data accessed in the
               callback must be immutable or synchronized with a mutex,
               semaphore, etc.

```c
void on_host_listener(CassHostListenerEvent event, CassInet inet, void* data) {
  /* Get the string representation of the inet address */
  char address[CASS_INET_STRING_LENGTH];
  cass_inet_string(inet, address);

  /* Perform application logic for host listener event */
  if (event == CASS_HOST_LISTENER_EVENT_ADD) {
    printf("Host %s has been ADDED\n", address);
   } else if (event == CASS_HOST_LISTENER_EVENT_REMOVE) {
    printf("Host %s has been REMOVED\n", address);
   } else if (event == CASS_HOST_LISTENER_EVENT_UP) {
    printf("Host %s is UP\n", address);
   } else if (event == CASS_HOST_LISTENER_EVENT_DOWN) {
    printf("Host %s is DOWN\n", address);
   }
}

int main() {
  CassCluster* cluster = cass_cluster_new();

  /* Register the host listener callback */
  cass_cluster_set_host_listener_callback(cluster, on_host_listener, NULL);

  /* ... */

  cass_cluster_free(cluster);
}
```

**Note**: Expensive (e.g. slow) operations should not be performed in host
          listener callbacks. Performing expensive operations in a callback
          will block or slow the driver's normal operation.

### Reconnection Policy

The reconnection policy controls the interval between each attempt for a given
connection.

#### Exponential Reconnection Policy

The exponential reconnection policy is the default reconnection policy. It
starts by using a base delay in milliseconds which is then exponentially
increased (doubled) during each reconnection attempt; up to the defined maximum
delay.

**Note**: Once the connection is re-established, this policy will restart using
          base delay if a reconnection occurs.

#### Constant Reconnection Policy

The constant reconnection policy is a fixed delay for each reconnection
attempt.

### Performance Tips

#### Use a single persistent session

Sessions are expensive objects to create in both time and resources because they
maintain a pool of connections to your Cassandra cluster. An application should
create a minimal number of sessions and maintain them for the lifetime of an
application.

#### Use token-aware and latency-aware policies

The token-aware load balancing can reduce the latency of requests by avoiding an
extra network hop through a coordinator node. When using the token-aware policy
requests are sent to one of the nodes which will retrieved or stored instead of
routing the request through a proxy node (coordinator node).

The latency-aware load balancing policy can also reduce the latency of requests
by routing requests to nodes that historical performing with the lowest latency.
This can prevent requests from being sent to nodes that are underperforming.

Both [latency-aware] and [token-aware] can be use together to obtain the benefits of
both.

#### Use [paging] when retrieving large result sets

Using a large page size or a very high `LIMIT` clause can cause your application
to delay for each individual request. The driver's paging mechanism can be used
to decrease the latency of individual requests.

#### Choose a lower consistency level

Ultimately, choosing a consistency level is a trade-off between consistency and
availability. Performance should not be a large deciding factor when choosing a
consistency level. However, it can affect high-percentile latency numbers
because requests with consistency levels greater than `ONE` can cause requests
to wait for one or more nodes to respond back to the coordinator node before a
request can complete. In multi-datacenter configurations, consistency levels such as
`EACH_QUORUM` can cause a request to wait for replication across a slower cross
datacenter network link.  More information about setting the consistency level
can be found [here](http://datastax.github.io/cpp-driver/topics/basics/consistency/).

### Driver Tuning

Beyond the performance tips and best practices considered in the previous
section your application might consider tuning the more fine-grain driver
settings in this section to achieve optimal performance for your application's
specific workload.

#### Increasing core connections

In some workloads, throughput can be increased by increasing the number of core
connections. By default, the driver uses a single core connection per host. It's
recommended that you try increasing the core connections to two and slowly
increase this number while doing performance testing. Two core connections is
often a good setting and increasing the core connections too high will decrease
performance because having multiple connections to a single host inhibits the
driver's ability to coalesce multiple requests into a fewer number of system
calls.

#### Coalesce delay

The coalesce delay is an optimization to reduce the number of system calls
required to process requests. This setting controls how long the driver's I/O
threads wait for requests to accumulate before flushing them on to the wire.
Larger values for coalesce delay are preferred for throughput-based workloads as
it can significantly reduce the number of system calls required to process
requests.

In general, the coalesce delay should be increased for throughput-based
workloads and can be decreased for latency-based workloads. Most importantly,
the delay should consider the responsiveness guarantees of your application.

Note: Single, sporadic requests are not generally affected by this delay and
are processed immediately.

#### New request ratio

The new request ratio controls how much time an I/O thread spends processing new
requests versus handling outstanding requests. This value is a percentage (with
a value from 1 to 100), where larger values will dedicate more time to
processing new requests and less time on outstanding requests. The goal of this
setting is to balance the time spent processing new/outstanding requests and
prevent either from fully monopolizing the I/O thread's processing time. It's
recommended that your application decrease this value if computationally
expensive or long-running future callbacks are used (via
`cass_future_set_callback()`), otherwise this can be left unchanged.

[`allow_remote_dcs_for_local_cl`]: http://datastax.github.io/cpp-driver/api/struct.CassCluster/#1a46b9816129aaa5ab61a1363489dccfd0
[`OPTIONS`]: https://github.com/apache/cassandra/blob/cassandra-3.0/doc/native_protocol_v3.spec
[token-aware]: http://datastax.github.io/cpp-driver/topics/configuration/#latency-aware-routing
[latency-aware]: http://datastax.github.io/cpp-driver/topics/configuration/#token-aware-routing
[paging]: http://datastax.github.io/cpp-driver/topics/basics/handling_results/#paging
