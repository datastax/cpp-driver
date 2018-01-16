# UUIDs

UUIDs are 128-bit identifiers that can be used to uniquely identify information
without requiring central coordination. These are often used in Cassandra
for primary and clustering keys. There are two types of UUIDs supported by
the driver (and Cassandra), version 1 which is time-based and version 4 which
is randomly generated. Version 1 can be used with Cassandra'a `timeuuid` type
and can be used as a timestamp for data.  Timestamp information can be
extracted from the time part of a version 1 UUID using [`cass_uuid_timestamp()`].
Version 4 can be used with Cassandra's `uuid` type for unique identification.

## Generator

A UUID generator object is used to create new UUIDs. The [`CassUuidGen`] object
is thread-safe. It should only be created once per application and reused.

```c
CassUuidGen* uuid_gen = cass_uuid_gen_new();

CassUuid uuid;

/* Generate a version 1 UUID */
cass_uuid_gen_time(uuid_gen, &uuid);

/* Generate a version 1 UUID from an existing timestamp */
cass_uuid_gen_from_time(uuid_gen, 1234, &uuid);

/* Generate a version 4 UUID */
cass_uuid_gen_random(uuid_gen, &uuid);

cass_uuid_gen_free(uuid_gen);
```

A [`CassUuidGen`] can also be created with user provided information for the
node part of the UUID. This only affects version 1 UUIDs.

```c
/* Only the 48 least signficant bits of the node are considered */
cass_uint64_t node = 0x0000AAAABBBBCCCC;

CassUuidGen* uuid_gen = cass_uuid_gen_new_with_node(node);

/* Generate UUIDs */

cass_uuid_gen_free(uuid_gen);
```

## Extracting information

Information such as the timestamp (for version 1 only) and the version can be
extracted from UUIDs. They can also be converted to and created from the their
hexadecimal string representation e.g. "550e8400-e29b-41d4-a716-446655440000".

```c
CassUuid uuid;
cass_uuid_from_string("550e8400-e29b-41d4-a716-446655440000", &uuid);

/* Extract timestamp and version */
cass_uint64_t timestamp = cass_uuid_timestamp(uuid);
cass_uint8_t version = cass_uuid_version(uuid);

/* Get string representation of the UUID */
char uuid_str[CASS_UUID_STRING_LENGTH];
cass_uuid_string(uuid, uuid_str);
```
[`cass_uuid_timestamp()`]: http://datastax.github.io/cpp-driver/api/struct.CassUuid/#1a3980467a0bb6642054ecf37d49aebf1a
[`CassUuidGen`]: http://datastax.github.io/cpp-driver/api/struct.CassUuidGen/
