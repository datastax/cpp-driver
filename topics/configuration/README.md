# Configuration

## Load balancing

Load balancing controls how queries are distributed to nodes in a Cassandra
cluster.

Without additional configuration the C/C++ driver defaults to using Datacenter-aware
load balancing with token-aware routing. Meaning the driver will only send
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
/*
 * Use up to 2 remote datacenter nodes for remote consistency levels
 * or when `allow_remote_dcs_for_local_cl` is enabled.
 */
unsigned used_hosts_per_remote_dc = 2;

/* Don't use remote datacenter nodes for local consistency levels */
cass_bool_t allow_remote_dcs_for_local_cl = cass_false;

cass_cluster_set_load_balance_dc_aware(cluster,
                                       used_hosts_per_remote_dc,
                                       allow_remote_dcs_for_local_cl);
```

### Token-aware Routing

Token-aware routing uses the primary key of queries to route requests directly to
the Cassandra nodes where the data is located. Using this policy avoids having
to route requests through an extra coordinator node in the Cassandra cluster. This
can improve query latency and reduce load on the Cassandra nodes. It can be used
in conjunction with other load balancing and routing policies.

```c
/* Enable token-aware routing (this is the default setting) */
cass_cluster_set_token_aware_routing(cluster, cass_true);

/* Disable token-aware routing */
cass_cluster_set_token_aware_routing(cluster, cass_true);
```

### Latency-aware Routing

Latency-aware routing tracks the latency of queries to avoid sending new queries
to poorly performing Cassandra nodes. It can be used in conjunction with other
load balancing and routing policies.

```c
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
```

[`allow_remote_dcs_for_local_cl`]: http://datastax.github.io/cpp-driver/api/struct_cass_cluster/#1a46b9816129aaa5ab61a1363489dccfd0
