/*
  This is free and unencumbered software released into the public domain.

  Anyone is free to copy, modify, publish, use, compile, sell, or
  distribute this software, either in source code form or as a compiled
  binary, for any purpose, commercial or non-commercial, and by any
  means.

  In jurisdictions that recognize copyright laws, the author or authors
  of this software dedicate any and all copyright interest in the
  software to the public domain. We make this dedication for the benefit
  of the public at large and to the detriment of our heirs and
  successors. We intend this dedication to be an overt act of
  relinquishment in perpetuity of all present and future rights to this
  software under copyright law.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
  OTHER DEALINGS IN THE SOFTWARE.

  For more information, please refer to <http://unlicense.org/>
*/

#include <stdio.h>
#include <cassandra.h>

void print_keyspace_meta(const CassKeyspaceMeta* meta, int indent);
void print_table_meta(const CassTableMeta* meta, int indent);
void print_function_meta(const CassFunctionMeta* meta, int indent);
void print_aggregate_meta(const CassAggregateMeta* meta, int indent);

void print_keyspace(CassSession* session, const char* keyspace) {
  const CassSchemaMeta* schema_meta = cass_session_get_schema_meta(session);
  const CassKeyspaceMeta* keyspace_meta = cass_schema_meta_keyspace_by_name(schema_meta, keyspace);

  if (keyspace_meta != NULL) {
    print_keyspace_meta(keyspace_meta, 0);
  } else {
    fprintf(stderr, "Unable to find \"%s\" keyspace in the schema metadata\n", keyspace);
  }

  cass_schema_meta_free(schema_meta);
}

void print_table(CassSession* session, const char* keyspace, const char* table) {
  const CassSchemaMeta* schema_meta = cass_session_get_schema_meta(session);
  const CassKeyspaceMeta* keyspace_meta = cass_schema_meta_keyspace_by_name(schema_meta, keyspace);

  if (keyspace_meta != NULL) {
    const CassTableMeta* table_meta = cass_keyspace_meta_table_by_name(keyspace_meta, table);
    if (table_meta != NULL) {
      print_table_meta(table_meta, 0);
    } else {
      fprintf(stderr, "Unable to find \"%s\" table in the schema metadata\n", table);
    }
  } else {
    fprintf(stderr, "Unable to find \"%s\" keyspace in the schema metadata\n", keyspace);
  }

  cass_schema_meta_free(schema_meta);
}

void print_function(CassSession* session, const char* keyspace, const char* function, const char* arguments) {
  const CassSchemaMeta* schema_meta = cass_session_get_schema_meta(session);
  const CassKeyspaceMeta* keyspace_meta = cass_schema_meta_keyspace_by_name(schema_meta, keyspace);

  if (keyspace_meta != NULL) {
    const CassFunctionMeta* function_meta = cass_keyspace_meta_function_by_name(keyspace_meta, function, arguments);
    if (function_meta != NULL) {
      print_function_meta(function_meta, 0);
    } else {
      fprintf(stderr, "Unable to find \"%s\" function in the schema metadata\n", function);
    }
  } else {
    fprintf(stderr, "Unable to find \"%s\" keyspace in the schema metadata\n", keyspace);
  }

  cass_schema_meta_free(schema_meta);
}

void print_aggregate(CassSession* session, const char* keyspace, const char* aggregate, const char* arguments) {
  const CassSchemaMeta* schema_meta = cass_session_get_schema_meta(session);
  const CassKeyspaceMeta* keyspace_meta = cass_schema_meta_keyspace_by_name(schema_meta, keyspace);

  if (keyspace_meta != NULL) {
    const CassAggregateMeta* aggregate_meta = cass_keyspace_meta_aggregate_by_name(keyspace_meta, aggregate, arguments);
    if (aggregate_meta != NULL) {
      print_aggregate_meta(aggregate_meta, 0);
    } else {
      fprintf(stderr, "Unable to find \"%s\" aggregate in the schema metadata\n", aggregate);
    }
  } else {
    fprintf(stderr, "Unable to find \"%s\" keyspace in the schema metadata\n", keyspace);
  }

  cass_schema_meta_free(schema_meta);
}

void print_error(CassFuture* future) {
  const char* message;
  size_t message_length;
  cass_future_error_message(future, &message, &message_length);
  fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
}

CassError execute_query(CassSession* session, const char* query) {
  CassError rc = CASS_OK;
  CassFuture* future = NULL;
  CassStatement* statement = cass_statement_new(query, 0);

  future = cass_session_execute(session, statement);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

int main(int argc, char* argv[]) {
  CassFuture* connect_future = NULL;
  CassCluster* cluster = cass_cluster_new();
  CassSession* session = cass_session_new();
  char* hosts = "127.0.0.1";
  if (argc > 1) {
    hosts = argv[1];
  }
  cass_cluster_set_contact_points(cluster, hosts);

  connect_future = cass_session_connect(session, cluster);

  if (cass_future_error_code(connect_future) == CASS_OK) {
    const CassSchemaMeta* schema_meta = cass_session_get_schema_meta(session);
    CassVersion version = cass_schema_meta_version(schema_meta);

    execute_query(session, "DROP KEYSPACE IF EXISTS examples;");

    execute_query(session,
                  "CREATE KEYSPACE examples WITH replication = { \
                  'class': 'SimpleStrategy', 'replication_factor': '3' }");

    print_keyspace(session, "examples");

    execute_query(session,
                  "CREATE TABLE examples.schema_meta (key text, \
                  value bigint, \
                  PRIMARY KEY (key))");

    execute_query(session,
                  "CREATE INDEX schema_meta_idx \
                    ON examples.schema_meta (value)");

    execute_query(session,
                  "CREATE FUNCTION \
                     examples.avg_state(state tuple<int, bigint>, val int) \
                  CALLED ON NULL INPUT RETURNS tuple<int, bigint> \
                  LANGUAGE java AS \
                    'if (val != null) { \
                      state.setInt(0, state.getInt(0) + 1); \
                      state.setLong(1, state.getLong(1) + val.intValue()); \
                    } \
                    return state;'");
    execute_query(session,
                  "CREATE FUNCTION \
                     examples.avg_final (state tuple<int, bigint>) \
                  CALLED ON NULL INPUT RETURNS double \
                  LANGUAGE java AS \
                    'double r = 0; \
                    if (state.getInt(0) == 0) return null; \
                    r = state.getLong(1); \
                    r /= state.getInt(0); \
                    return Double.valueOf(r);'");

    execute_query(session,
                  "CREATE AGGREGATE examples.average(int) \
                  SFUNC avg_state STYPE tuple<int, bigint> FINALFUNC avg_final \
                  INITCOND(0, 0)");

    print_table(session, "examples", "schema_meta");
    if (version.major_version >= 3) {
      /* Collection types are marked as frozen in Cassandra 3.x and later. */
      print_function(session, "examples", "avg_state", "frozen<tuple<int,bigint>>,int");
      print_function(session, "examples", "avg_final", "frozen<tuple<int,bigint>>");
    } else {
      print_function(session, "examples", "avg_state", "tuple<int,bigint>,int");
      print_function(session, "examples", "avg_final", "tuple<int,bigint>");
    }
    print_aggregate(session, "examples", "average", "int");

    cass_schema_meta_free(schema_meta);
  } else {
    print_error(connect_future);
  }

  cass_future_free(connect_future);
  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;
}

void print_schema_value(const CassValue* value);
void print_schema_bytes(const CassValue* value);
void print_schema_list(const CassValue* value);
void print_schema_map(const CassValue* value);
void print_meta_field(const CassIterator* iterator, int indent);
void print_meta_fields(CassIterator* iterator, int indent);
void print_column_meta(const CassColumnMeta* meta, int indent);
void print_index_meta(const CassIndexMeta* meta, int indent);

void print_indent(int indent) {
  int i;
  for (i = 0; i < indent; ++i) {
    printf("\t");
  }
}

void print_schema_value(const CassValue* value) {
  cass_int32_t i;
  cass_bool_t b;
  cass_double_t d;
  const char* s;
  size_t s_length;
  CassUuid u;
  char us[CASS_UUID_STRING_LENGTH];

  CassValueType type = cass_value_type(value);
  switch (type) {
    case CASS_VALUE_TYPE_INT:
      cass_value_get_int32(value, &i);
      printf("%d", i);
      break;

    case CASS_VALUE_TYPE_BOOLEAN:
      cass_value_get_bool(value, &b);
      printf("%s", b ? "true" : "false");
      break;

    case CASS_VALUE_TYPE_DOUBLE:
      cass_value_get_double(value, &d);
      printf("%f", d);
      break;

    case CASS_VALUE_TYPE_TEXT:
    case CASS_VALUE_TYPE_ASCII:
    case CASS_VALUE_TYPE_VARCHAR:
      cass_value_get_string(value, &s, &s_length);
      printf("\"%.*s\"", (int)s_length, s);
      break;

    case CASS_VALUE_TYPE_UUID:
      cass_value_get_uuid(value, &u);
      cass_uuid_string(u, us);
      printf("%s", us);
      break;

    case CASS_VALUE_TYPE_LIST:
      print_schema_list(value);
      break;

    case CASS_VALUE_TYPE_MAP:
      print_schema_map(value);
      break;

    case CASS_VALUE_TYPE_BLOB:
      print_schema_bytes(value);
      break;

    default:
      if (cass_value_is_null(value)) {
        printf("null");
      } else {
        printf("<unhandled type>");
      }
      break;
  }
}

void print_schema_bytes(const CassValue* value) {
  const cass_byte_t* bytes;
  size_t b_length;
  size_t i;

  cass_value_get_bytes(value, &bytes, &b_length);
  printf("0x");
  for (i = 0; i < b_length; ++i) printf("%02x", bytes[i]);
}

void print_schema_list(const CassValue* value) {
  CassIterator* iterator = cass_iterator_from_collection(value);
  cass_bool_t is_first = cass_true;

  printf("[ ");
  while (cass_iterator_next(iterator)) {
    if (!is_first) printf(", ");
    print_schema_value(cass_iterator_get_value(iterator));
    is_first = cass_false;
  }
  printf(" ]");
  cass_iterator_free(iterator);
}

void print_schema_map(const CassValue* value) {
  CassIterator* iterator = cass_iterator_from_map(value);
  cass_bool_t is_first = cass_true;

  printf("{ ");
  while (cass_iterator_next(iterator)) {
    if (!is_first) printf(", ");
    print_schema_value(cass_iterator_get_map_key(iterator));
    printf(" : ");
    print_schema_value(cass_iterator_get_map_value(iterator));
    is_first = cass_false;
  }
  printf(" }");
  cass_iterator_free(iterator);
}

void print_meta_field(const CassIterator* iterator, int indent) {
  const char* name;
  size_t name_length;
  const CassValue* value;

  cass_iterator_get_meta_field_name(iterator, &name, &name_length);
  value = cass_iterator_get_meta_field_value(iterator);

  print_indent(indent);
  printf("%.*s: ", (int)name_length, name);
  print_schema_value(value);
  printf("\n");
}

void print_meta_fields(CassIterator* iterator, int indent) {
  while (cass_iterator_next(iterator)) {
    print_meta_field(iterator, indent);
  }
  cass_iterator_free(iterator);
}

void print_keyspace_meta(const CassKeyspaceMeta* meta, int indent) {
  const char* name;
  size_t name_length;
  CassIterator* iterator;

  print_indent(indent);
  cass_keyspace_meta_name(meta, &name, &name_length);
  printf("Keyspace \"%.*s\":\n", (int)name_length, name);

  print_meta_fields(cass_iterator_fields_from_keyspace_meta(meta), indent + 1);
  printf("\n");

  iterator = cass_iterator_tables_from_keyspace_meta(meta);
  while (cass_iterator_next(iterator)) {
    print_table_meta(cass_iterator_get_table_meta(iterator), indent + 1);
  }
  printf("\n");

  cass_iterator_free(iterator);
}

void print_table_meta(const CassTableMeta* meta, int indent) {
  const char* name;
  size_t name_length;
  CassIterator* iterator;

  print_indent(indent);
  cass_table_meta_name(meta, &name, &name_length);
  printf("Table \"%.*s\":\n", (int)name_length, name);

  print_meta_fields(cass_iterator_fields_from_table_meta(meta), indent + 1);
  printf("\n");

  iterator = cass_iterator_columns_from_table_meta(meta);
  while (cass_iterator_next(iterator)) {
    print_column_meta(cass_iterator_get_column_meta(iterator), indent + 1);
  }
  printf("\n");

  cass_iterator_free(iterator);

  iterator = cass_iterator_indexes_from_table_meta(meta);
  while (cass_iterator_next(iterator)) {
    print_index_meta(cass_iterator_get_index_meta(iterator), indent + 1);
  }
  printf("\n");

  cass_iterator_free(iterator);
}

void print_function_meta(const CassFunctionMeta* meta, int indent) {
  const char* name;
  size_t name_length;

  print_indent(indent);
  cass_function_meta_name(meta, &name, &name_length);
  printf("Function \"%.*s\":\n", (int)name_length, name);

  print_meta_fields(cass_iterator_fields_from_function_meta(meta), indent + 1);
  printf("\n");
}

void print_aggregate_meta(const CassAggregateMeta* meta, int indent) {
  const char* name;
  size_t name_length;

  print_indent(indent);
  cass_aggregate_meta_name(meta, &name, &name_length);
  printf("Aggregate \"%.*s\":\n", (int)name_length, name);

  print_meta_fields(cass_iterator_fields_from_aggregate_meta(meta), indent + 1);
  printf("\n");
}

void print_column_meta(const CassColumnMeta* meta, int indent) {
  const char* name;
  size_t name_length;

  print_indent(indent);
  cass_column_meta_name(meta, &name, &name_length);
  printf("Column \"%.*s\":\n", (int)name_length, name);
  print_meta_fields(cass_iterator_fields_from_column_meta(meta), indent + 1);
  printf("\n");
}

void print_index_meta(const CassIndexMeta* meta, int indent) {
  const char* name;
  size_t name_length;

  print_indent(indent);
  cass_index_meta_name(meta, &name, &name_length);
  printf("Index \"%.*s\":\n", (int)name_length, name);
  print_meta_fields(cass_iterator_fields_from_index_meta(meta), indent + 1);
  printf("\n");
}
