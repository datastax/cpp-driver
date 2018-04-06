# Schema Metadata

The driver provides access to keyspace and table metadata. This schema metadata
is monitored by the control connection and automatically kept up-to-date.

```c
void get_schema_metadata(CassSession* session) {
  /* Get snapshot of the schema */
  const CassSchemaMeta* schema_meta = cass_session_get_schema_meta(session);

  /* Get information about the "keyspace1" schema */
  const CassKeyspaceMeta* keyspace1_meta
    = cass_schema_meta_keyspace_by_name(schema_meta, "keyspace1");

  if (keyspace1_meta == NULL) {
    /* Handle error */
  }

  /* Get the key-value filed for "strategy_class" */
  const CassValue* strategy_class_value
    = cass_keyspace_meta_field_by_name(keyspace1_meta, "strategy_class");

  if (strategy_class_value == NULL) {
    /* Handle error */
  }

  /* Fields values use the existing cass_value*() API */
  const char* strategy_class;
  size_t strategy_class_length;
  cass_value_get_string(strategy_class_value,
                        &strategy_class,
                        &strategy_class_length);

  /* Do something with strategy_class */

  /* All values derived from the schema are cleaned up */
  cass_schema_meta_free(schema_meta);
}
```

The snapshot obtained by [`cass_session_get_schema_meta()`] will not see schema changes
that happened after the call. A new snapshot needs to be obtained to see
subsequent updates to the schema.

## Enabling/Disabling Schema Metadata

Retrieving and updating schema metadata can be enabled or disabled. It is
enabled by default. However, some application might wish to reduce this
overhead. This can be useful to improve the startup performance of the
short-lived sessions or an environment where up-to-date schema metadata is
unnecessary.

```c
CassCluster* cluster = cass_cluster_new();

/* Disable schema metdata */
cass_cluster_set_use_schema(cluster, cass_false);

cass_cluster_free(cluster);
```
[`cass_session_get_schema_meta()`]: http://datastax.github.io/cpp-driver/api/struct.CassSession/#cass-session-get-schema-meta
