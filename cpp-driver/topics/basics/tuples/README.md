# Tuples

**Note**: Cassandra 2.1+ is required.

Tuples are fixed-length sets of values. They are similar to UDTs in that they
can contain different types of values, but unlike UDTs tuples can only be
accessed by position and not by name.

## Creating a Tuple

Creating a [`CassTuple`] is done by allocating a new tuple object with the
number of items that will be contained in it. Items can the be set in the tuple
using their position.

```c
/* The number of items must be set properly */
CassTuple* tuple = cass_tuple_new(2);

/* Items are set by position */
cass_tuple_set_string(tuple, 0, "abc");
cass_tuple_set_int64(tuple, 1, 123);

/* ... */

/* Tuples must be freed */
cass_tuple_free(tuple);
```

## Create a Tuple using a Data Type

A tuple can also be created using a [`CassDataType`] that comes from schema
metadata or is manually constructed. However, this is not a necessary step as
a tuple can be created without a data type. A typed tuple will not allow invalid
type to be added to it. [`cass_tuple_set_*()`] functions will return an error
code if the incorrect type is added to a position.

```c
/* Creata new tuple data type */
CassDataType* data_type = cass_data_type_new_tuple(2);

/* Add a string at position 0 and an 64-bit integer at position 1 */
cass_data_type_add_sub_value_type(data_type, CASS_VALUE_TYPE_TEXT);
cass_data_type_add_sub_value_type(data_type, CASS_VALUE_TYPE_BIGINT);

/* Create a new tuple using data type */
CassTuple* tuple = cass_tuple_new_from_data_type(data_type);

/* This will now return an error because the data type of the first item is
 * a string not an integer
 */
CassError rc = cass_tuple_set_int32(tuple, 0, 123);

assert(rc != CASS_OK);

/* These are the correct types */
cass_tuple_set_string(tuple, 0, "abc");
cass_tuple_set_int64(tuple, 1, 123);

/* ... */

/* Constructed data types must be freed */
cass_data_type_free(data_type);

/* Tuples must be freed */
cass_tuple_free(tuple);
```

## Consuming values from a Tuple result

[`CassTuple`]s are consumed using an iterator.

```c
void iterate_tuple(const CassRow* row) {
  /* Retrieve tuple value from column */
  const CassValue* tuple_value = cass_row_get_column_by_name(row, "value1");

  /* Create an iterator for the UDT value */
  CassIterator* tuple_iterator = cass_iterator_from_tuple(tuple_value);

  /* Iterate over the tuple fields */
  while (cass_iterator_next(tuple_iterator)) {
    const char* field_name;
    size_t field_name_length;
    /* Get tuple value */
    const CassValue* value = cass_iterator_get_value(tuple_iterator);

    /* ... */
  }

  /* The tuple iterator needs to be freed */
  cass_iterator_free(tuple_iterator);
}
```

[`CassTuple`]: http://datastax.github.io/cpp-driver/api/struct.CassTuple/
[`CassUserType`]: http://datastax.github.io/cpp-driver/api/struct.CassUserType/
[`cass_tuple_set_*()`]: http://datastax.github.io/cpp-driver/api/struct.CassTuple/#cass-tuple-set-null
