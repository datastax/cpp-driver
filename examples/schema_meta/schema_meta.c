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

void print_schema_meta(const CassSchemaMeta* meta, int indent);

void print_keyspace(CassSession* session, const char* keyspace) {
  const CassSchema* schema = cass_session_get_schema(session);
  const CassSchemaMeta* keyspace_meta = cass_schema_get_keyspace(schema, keyspace);

  if (keyspace_meta != NULL) {
    print_schema_meta(keyspace_meta, 0);
  } else {
    fprintf(stderr, "Unable to find \"%s\" keyspace in the schema metadata\n", keyspace);
  }

  cass_schema_free(schema);
}

void print_table(CassSession* session, const char* keyspace, const char* table) {
  const CassSchema* schema = cass_session_get_schema(session);
  const CassSchemaMeta* keyspace_meta = cass_schema_get_keyspace(schema, keyspace);

  if (keyspace_meta != NULL) {
    const CassSchemaMeta* table_meta = cass_schema_meta_get_entry(keyspace_meta, table);
    if (table_meta != NULL) {
      print_schema_meta(table_meta, 0);
    } else {
      fprintf(stderr, "Unable to find \"%s\" table in the schema metadata\n", table);
    }
  } else {
    fprintf(stderr, "Unable to find \"%s\" keyspace in the schema metadata\n", keyspace);
  }

  cass_schema_free(schema);
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

int main() {
  CassFuture* connect_future = NULL;
  CassCluster* cluster = cass_cluster_new();
  CassSession* session = cass_session_new();
  cass_cluster_set_contact_points(cluster, "127.0.0.1");

  connect_future = cass_session_connect(session, cluster);

  if (cass_future_error_code(connect_future) == CASS_OK) {
    CassFuture* close_future = NULL;

    execute_query(session,
                  "CREATE KEYSPACE examples WITH replication = { \
                  'class': 'SimpleStrategy', 'replication_factor': '3' };");

    print_keyspace(session, "examples");

    execute_query(session,
                  "CREATE TABLE examples.schema_meta (key text, \
                  value bigint, \
                  PRIMARY KEY (key));");

    print_table(session, "examples", "schema_meta");

    /* Close the session */
    close_future = cass_session_close(session);
    cass_future_wait(close_future);
    cass_future_free(close_future);
  } else {
    print_error(connect_future);
  }

  cass_future_free(connect_future);
  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;
}

void print_schema_value(const CassValue* value);
void print_schema_list(const CassValue* value);
void print_schema_map(const CassValue* value);
void print_schema_meta_field(const CassSchemaMetaField* field, int indent);
void print_schema_meta_fields(const CassSchemaMeta* meta, int indent);
void print_schema_meta_entries(const CassSchemaMeta* meta, int indent);

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

    default:
      if (cass_value_is_null(value)) {
        printf("null");
      } else {
        printf("<unhandled type>");
      }
      break;
  }
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

void print_schema_meta_field(const CassSchemaMetaField* field, int indent) {
  const char* name;
  size_t name_length;
  const CassValue* value;

  cass_schema_meta_field_name(field, &name, &name_length);
  value = cass_schema_meta_field_value(field);

  print_indent(indent);
  printf("%.*s: ", (int)name_length, name);
  print_schema_value(value);
  printf("\n");
}

void print_schema_meta_fields(const CassSchemaMeta* meta, int indent) {
  CassIterator* fields = cass_iterator_fields_from_schema_meta(meta);

  while (cass_iterator_next(fields)) {
    print_schema_meta_field(cass_iterator_get_schema_meta_field(fields), indent);
  }
  cass_iterator_free(fields);
}

void print_schema_meta_entries(const CassSchemaMeta* meta, int indent) {
  CassIterator* entries = cass_iterator_from_schema_meta(meta);

  while (cass_iterator_next(entries)) {
    print_schema_meta(cass_iterator_get_schema_meta(entries), indent);
  }
  cass_iterator_free(entries);
}

void print_schema_meta(const CassSchemaMeta* meta, int indent) {
  const CassSchemaMetaField* field = NULL;
  const char* name;
  size_t name_length;
  print_indent(indent);

  switch (cass_schema_meta_type(meta)) {
    case CASS_SCHEMA_META_TYPE_KEYSPACE:
      field = cass_schema_meta_get_field(meta, "keyspace_name");
      cass_value_get_string(cass_schema_meta_field_value(field), &name, &name_length);
      printf("Keyspace \"%.*s\":\n", (int)name_length, name);
      print_schema_meta_fields(meta, indent + 1);
      printf("\n");
      print_schema_meta_entries(meta, indent + 1);
      break;

    case CASS_SCHEMA_META_TYPE_TABLE:
      field = cass_schema_meta_get_field(meta, "columnfamily_name");
      cass_value_get_string(cass_schema_meta_field_value(field), &name, &name_length);
      printf("Table \"%.*s\":\n", (int)name_length, name);
      print_schema_meta_fields(meta, indent + 1);
      printf("\n");
      print_schema_meta_entries(meta, indent + 1);
      break;

    case CASS_SCHEMA_META_TYPE_COLUMN:
      field = cass_schema_meta_get_field(meta, "column_name");
      cass_value_get_string(cass_schema_meta_field_value(field), &name, &name_length);
      printf("Column \"%.*s\":\n", (int)name_length, name);
      print_schema_meta_fields(meta, indent + 1);
      printf("\n");
      break;
  }
}
