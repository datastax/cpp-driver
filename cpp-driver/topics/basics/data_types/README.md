# Data Types

[`CassDataType`] objects are useful for describing the different values that can
be stored in Cassandra, from primitive types to more complex composite types,
such as, UDTs (user-defined types), tuples and collections. Data types can be retrieved from existing
metadata found in schema, results, values or prepared statements, or they can be
constructed programmatically.

The following code snippets use the following type schema:

```cql
CREATE TYPE person (name text,
                    // Street address, zip code, state/province, and country
                    address frozen<tuple<text, int, text, text>>,
                    // Type and number
                    phone_numbers frozen<map<text, int>>);
```

## Retrieving an Existing Data Type

**Important**: Any `const CassDataType*` object doesn't need to be freed. Its
lifetime is bound to the object it came from.

UDT data types can be retrieved using a [`CassSchemaMeta`] object. The resulting
data type object can be used to construct a new [`CassUserType`] object using
[`cass_user_type_new_from_data_type()`].

```c
void get_person_data_type_from_keyspace(CassSession* session) {
  /* Get schema object (this should be cached) */
  const CassSchemaMeta* schema_meta = cass_session_get_schema_meta(session);

  /* Get the keyspace for the user-defined type. It doesn't need to be freed */
  const CassKeyspaceMeta* keyspace_meta =
    cass_schema_meta_keyspace_by_name(schema_meta, "examples");

  /* This data type object doesn't need to be freed */
  const CassDataType* person_data_type =
    cass_keyspace_meta_user_type_by_name(keyspace_meta, "person");

  /* ... */

  /* Schema object must be freed */
  cass_schema_meta_free(schema_meta);
}
```

Data types can also be retrieved from [`CassResult`], [`CassPrepared`], and
[`CassValue`] objects.

* [`cass_result_column_data_type()`] can be used to get the
  data type of a column for a [`CassResult`].
  * [`cass_prepared_parameter_data_type()`] can be used to get the data type of
  the parameters for a [`CassPrepared`] object. There are also functions to get
  the data type of a prepared parameter by name.
* [`cass_value_data_type()`] can be used to get the data type represented by a
  [`CassValue`] object.

## Building a Data Type Programmatically

Data types could be constructed programmatically. This is useful for application that may
have schema metatdata disabled.

```c
CassDataType* address_data_type = cass_data_type_new_type(4);
CassDataType* phone_numbers_data_type = cass_data_type_new(2);
CassDataType* person_data_type = cass_data_type_new_udt(3);

/* Street address, zip code, state/province, and country */
cass_data_type_add_sub_value_type(address_data_type, CASS_VALUE_TYPE_TEXT);
cass_data_type_add_sub_value_type(address_data_type, CASS_VALUE_TYPE_INT);
cass_data_type_add_sub_value_type(address_data_type, CASS_VALUE_TYPE_TEXT);
cass_data_type_add_sub_value_type(address_data_type, CASS_VALUE_TYPE_TEXT);

/* Phone type and number*/
cass_data_type_add_sub_value_type(phone_numbers_data_type, CASS_VALUE_TYPE_TEXT);
cass_data_type_add_sub_value_type(phone_numbers_data_type, CASS_VALUE_TYPE_INT);

/* Add fields to the person data type */
cass_data_type_add_sub_value_type_by_name(person_data_type, "name", CASS_VALUE_TYPE_TEXT);
cass_data_type_add_sub_data_type_by_name(person_data_type, "address", address_data_type);
cass_data_type_add_sub_value_type_by_name(person_data_type, "phone_numbers", phone_numbers_data_type);

/* ... */

/* Data types must be freed */
cass_data_type_free(address_data_type);
cass_data_type_free(phone_numbers_data_type);
cass_data_type_free(person_data_type);
```

## Creating UDTs, Tuples and Collections Using Data Types

After the user type object is retrieved or created manually, it can be used to
construct composite data types. The subtypes of a data type can be used to
construct other nested types.

```c
CassDataType* person_data_type = NULL;

/* Construct or lookup data type */

/* Construct a new UDT from a data type */
CassUserType* person = cass_user_type_new_from_data_type(person_data_type);

/* ... */

/* Construct a new tuple from a nested data type */
CassTuple* address =
  cass_tuple_new_from_data_type(
     cass_data_type_sub_data_type_by_name(person_data_type, "address"));

/* ... */

/* Construct a new map collection from a nested data type */
CassCollection* phone_numbers =
  cass_collection_new_from_data_type(
    cass_data_type_sub_data_type_by_name(person_data_type, "phone_numbers"), 2);

/* ... */

/* Add fields to the UDT */
cass_user_type_set_string_byte_by_name(person, "name", "Bob");
cass_user_type_set_user_type_by_name(person, "address", address);
cass_user_type_set_collection_by_name(person, "phone_numbers", phone_numbers);

/* ... */

/* UDT, tuple, and collection objects must be freed */
cass_user_type_free(person);
cass_tuple_free(address);
cass_collection_free(phone_numbers);
```

[`CassDataType`]: http://datastax.github.io/cpp-driver/api/struct.CassDataType/
[`CassUserType`]: http://datastax.github.io/cpp-driver/api/struct.CassUserType/
[`CassPrepared`]: http://datastax.github.io/cpp-driver/api/struct.CassPrepared/
[`CassResult`]: http://datastax.github.io/cpp-driver/api/struct.CassResult/
[`CassValue`]: http://datastax.github.io/cpp-driver/api/struct.CassValue/
[`CassSchemaMeta`]: http://datastax.github.io/cpp-driver/api/struct.CassSchemaMeta/
[`cass_user_type_new_from_data_type()`]: http://datastax.github.io/cpp-driver/api/struct.CassUserType/#cass-user-type-new-from-data-type
[`cass_result_column_data_type()`]: http://datastax.github.io/cpp-driver/api/struct.CassResult/#cass-result-column-data-type
[`cass_prepared_parameter_data_type()`]: http://datastax.github.io/cpp-driver/api/struct.CassPrepared/#cass-prepared-parameter-data-type
[`cass_value_data_type()`]: http://datastax.github.io/cpp-driver/api/struct.CassValue/#cass-value-data-type
