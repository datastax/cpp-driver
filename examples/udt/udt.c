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

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "cassandra.h"

CassUuidGen* uuid_gen;
const CassSchema* schema;

void print_error(CassFuture* future) {
  const char* message;
  size_t message_length;
  cass_future_error_message(future, &message, &message_length);
  fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
}

CassCluster* create_cluster() {
  CassCluster* cluster = cass_cluster_new();
  cass_cluster_set_contact_points(cluster, "127.0.0.1");
  return cluster;
}

CassError connect_session(CassSession* session, const CassCluster* cluster) {
  CassError rc = CASS_OK;
  CassFuture* future = cass_session_connect(session, cluster);

  cass_future_wait(future);
  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  }
  cass_future_free(future);

  return rc;
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

CassError insert_into_udt(CassSession* session) {
  CassError rc = CASS_OK;
  CassStatement* statement = NULL;
  CassFuture* future = NULL;

  CassUuid id;
  char id_str[CASS_UUID_STRING_LENGTH];
  const CassDataType* udt_address = NULL;
  const CassDataType* udt_phone = NULL;

  const char* query = "INSERT INTO examples.udt (id, address) VALUES (?, ?)";

  statement = cass_statement_new(query, 2);

  cass_uuid_gen_time(uuid_gen, &id);
  cass_uuid_string(id, id_str);

  udt_address = cass_schema_get_udt(schema, "examples", "address");
  udt_phone = cass_schema_get_udt(schema, "examples", "phone_numbers");

  if (udt_address != NULL && udt_phone != NULL) {
    int i;
    CassUserType* address = cass_user_type_new_from_data_type(udt_address);
    CassCollection* phone = cass_collection_new(CASS_COLLECTION_TYPE_SET, 2);

    for (i = 0; i < 2; ++i) {
      CassUserType* phone_numbers = cass_user_type_new_from_data_type(udt_phone);
      cass_user_type_set_int32_by_name(phone_numbers, "phone1", i + 1);
      cass_user_type_set_int32_by_name(phone_numbers, "phone2", i + 2);
      cass_collection_append_user_type(phone, phone_numbers);
      cass_user_type_free(phone_numbers);
    }

    cass_user_type_set_string_by_name(address, "street", id_str);
    cass_user_type_set_string_by_name(address, "city", id_str);
    cass_user_type_set_int32_by_name(address, "zip", (cass_int32_t)id.time_and_version);
    cass_user_type_set_collection_by_name(address, "phone", phone);

    cass_statement_bind_uuid(statement, 0, id);
    cass_statement_bind_user_type(statement, 1, address);

    future = cass_session_execute(session, statement);
    cass_future_wait(future);

    rc = cass_future_error_code(future);
    if (rc != CASS_OK) {
      print_error(future);
    }

    cass_future_free(future);
    cass_user_type_free(address);
    cass_collection_free(phone);
  }

  cass_statement_free(statement);

  return rc;
}

CassError select_from_udt(CassSession* session) {
  CassError rc = CASS_OK;
  CassStatement* statement = NULL;
  CassFuture* future = NULL;

  const char* query = "SELECT * FROM examples.udt";

  statement = cass_statement_new(query, 0);

  future = cass_session_execute(session, statement);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  } else {
    const CassResult* result = NULL;
    CassIterator* rows = NULL;

    result = cass_future_get_result(future);
    rows = cass_iterator_from_result(result);

    while(cass_iterator_next(rows)) {
      CassUuid id;
      char id_str[CASS_UUID_STRING_LENGTH];
      const CassRow* row = cass_iterator_get_row(rows);
      const CassValue* id_value = cass_row_get_column_by_name(row, "id");
      const CassValue* address_value = cass_row_get_column_by_name(row, "address");
      CassIterator* fields = cass_iterator_from_user_type(address_value);

      cass_value_get_uuid(id_value, &id);
      cass_uuid_string(id, id_str);

      printf("id %s ", id_str);

      while(fields != NULL && cass_iterator_next(fields)) {
        const char* field_name;
        size_t field_name_length;
        const CassValue* field_value = NULL;
        cass_iterator_get_user_type_field_name(fields, &field_name, &field_name_length);
        field_value = cass_iterator_get_user_type_field_value(fields);
        printf("%.*s ", (int)field_name_length, field_name);

        if (!cass_value_is_null(field_value)) {
          if (cass_value_type(field_value) == CASS_VALUE_TYPE_VARCHAR) {
            const char* text;
            size_t text_length;
            cass_value_get_string(field_value, &text, &text_length);
            printf("\"%.*s\" ", (int)text_length, text);
          } else if (cass_value_type(field_value) == CASS_VALUE_TYPE_INT) {
            cass_int32_t i;
            cass_value_get_int32(field_value, &i);
            printf("%d ", i);
          } else if (cass_value_type(field_value) == CASS_VALUE_TYPE_SET) {
            CassIterator* phone_numbers = cass_iterator_from_collection(field_value);
            while (cass_iterator_next(phone_numbers)) {
              const CassValue* phone_value = cass_iterator_get_value(phone_numbers);
              CassIterator* phone_fields = cass_iterator_from_user_type(phone_value);
              assert(cass_value_type(phone_value) == CASS_VALUE_TYPE_UDT);
              while (cass_iterator_next(phone_fields)) {
                const CassValue* phone_number_value = cass_iterator_get_user_type_field_value(phone_fields);
                cass_int32_t i;
                cass_value_get_int32(phone_number_value, &i);
                printf("%d ", i);
              }
            }
          } else {
            printf("<invalid> ");
          }
        } else {
          printf("<null> ");
        }
      }

      printf("\n");
    }

    cass_result_free(result);
    cass_iterator_free(rows);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

int main() {
  CassCluster* cluster = create_cluster();
  CassSession* session = cass_session_new();
  CassFuture* close_future = NULL;

  uuid_gen = cass_uuid_gen_new();

  if (connect_session(session, cluster) != CASS_OK) {
    cass_cluster_free(cluster);
    cass_session_free(session);
    return -1;
  }

  schema = cass_session_get_schema(session);

  execute_query(session,
                "CREATE KEYSPACE examples WITH replication = { \
                           'class': 'SimpleStrategy', 'replication_factor': '3' }");

  execute_query(session,
                "CREATE TYPE examples.phone_numbers (phone1 int, phone2 int)");

  execute_query(session,
                "CREATE TYPE examples.address (street text, city text, zip int, phone set<frozen<phone_numbers>>)");

  execute_query(session,
                "CREATE TABLE examples.udt (id timeuuid, address frozen<address>, PRIMARY KEY(id))");

  insert_into_udt(session);
  select_from_udt(session);

  close_future = cass_session_close(session);
  cass_future_wait(close_future);
  cass_future_free(close_future);

  cass_cluster_free(cluster);
  cass_session_free(session);

  cass_uuid_gen_free(uuid_gen);
  cass_schema_free(schema);

  return 0;
}
