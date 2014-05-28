/*
  Copyright (c) 2014 DataStax

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/


#include <stdio.h>
#include <string.h>
#include <cassandra.h>

int main() {
  CassError rc = 0;
  CassSession* session = cass_session_new();
  CassFuture* session_future = NULL;
  const char* contact_points[] = { "127.0.0.1",  NULL };
  const char** contact_point = NULL;

  for(contact_point = contact_points; *contact_point; contact_point++) {
    cass_session_setopt(session, CASS_OPTION_CONTACT_POINT_ADD, *contact_point, strlen(*contact_point));
  }

  session_future = cass_session_connect(session);
  cass_future_wait(session_future);
  rc = cass_future_error_code(session_future);

  if(rc == CASS_OK) {
    CassFuture* result_future = NULL;
    CassFuture* shutdown_future = NULL;
    CassString query = cass_string_init("SELECT * FROM system.schema_keyspaces;");
    CassStatement* statement = cass_statement_new(query, 0, CASS_CONSISTENCY_ONE);

    cass_session_execute(session, statement, &result_future);
    cass_future_wait(result_future);

    rc = cass_future_error_code(result_future);
    if(rc == CASS_OK) {
      const CassResult* result = cass_future_get_result(result_future);
      CassIterator* rows = cass_iterator_from_result(result);

      while(cass_iterator_next(rows)) {
        const CassRow* row = cass_iterator_get_row(rows);
        CassString keyspace_name;
        cass_bool_t durable_writes;
        CassString strategy_class;
        CassString strategy_options;

        cass_value_get_string(cass_row_get_column(row, 0), &keyspace_name);
        cass_value_get_bool(cass_row_get_column(row, 1), &durable_writes);
        cass_value_get_string(cass_row_get_column(row, 2), &strategy_class);
        cass_value_get_string(cass_row_get_column(row, 3), &strategy_options);

        printf("keyspace_name: '%.*s', durable_writes: %s, strategy_class: '%.*s', strategy_options: %.*s\n",
               (int)keyspace_name.length, keyspace_name.data,
               durable_writes ? "true" : "false",
               (int)strategy_class.length, strategy_class.data,
               (int)strategy_options.length, strategy_options.data);
      }

      cass_result_free(result);
      cass_iterator_free(rows);
    } else {
      CassString message = cass_future_error_message(result_future);
      fprintf(stderr, "Error: %.*s\n", (int)message.length, message.data);
    }

    cass_future_free(result_future);
    shutdown_future = cass_session_shutdown(session);
    cass_future_wait(shutdown_future);
    cass_future_free(shutdown_future);
  } else {
    CassString message = cass_future_error_message(session_future);
    fprintf(stderr, "Error: %.*s\n", (int)message.length, message.data);
  }

  cass_future_free(session_future);
  cass_session_free(session);

  return 0;
}
