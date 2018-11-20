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

void print_error(const char* context, CassFuture* future) {
  const char* message;
  size_t message_length;
  cass_future_error_message(future, &message, &message_length);
  fprintf(stderr, "%s: %.*s\n", context, (int)message_length, message);
}

const CassResult* run_tracing_query(CassSession* session,
                                    const char* query,
                                    CassUuid tracing_id) {
  CassError rc = CASS_OK;
  CassFuture* future = NULL;
  const CassResult* result = NULL;
  CassStatement* statement = cass_statement_new(query, 1);

  cass_statement_bind_uuid(statement, 0, tracing_id);

  future = cass_session_execute(session, statement);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error("Unable to run tracing query", future);
    return NULL;
  }

  result = cass_future_get_result(future);
  cass_future_free(future);
  cass_statement_free(statement);

  return result;
}

void print_tracing_data(CassSession* session, CassFuture* future) {
  CassUuid tracing_id;
  const CassResult* result = NULL;

  /* Get the tracing ID from the future object */
  if (cass_future_tracing_id(future, &tracing_id) != CASS_OK) {
    fprintf(stderr, "Unable to get tracing id\n");
    return;
  }

  /* Query the tracing tables using the retrieved tracing ID */

  /* Get information for the tracing session */
  result = run_tracing_query(session,
                             "SELECT * FROM system_traces.sessions WHERE session_id = ?",
                             tracing_id);

  if (result) {
    if (cass_result_row_count(result) > 0) {
      const char* command;
      size_t command_length;
      cass_int32_t duration;
      const CassRow* row = cass_result_first_row(result);

      /* Get the command type that was run and how long it took */
      cass_value_get_string(cass_row_get_column_by_name(row, "command"),
                            &command, &command_length);
      cass_value_get_int32(cass_row_get_column_by_name(row, "duration"),
                           &duration);

      printf("Request command \"%.*s\" took %f milliseconds:\n",
             (int)command_length, command,
             duration / 1000.0);
    }

    cass_result_free(result);
  }

  /* Get the events that happened during the tracing session */
  result = run_tracing_query(session,
                             "SELECT * FROM system_traces.events WHERE session_id = ?",
                             tracing_id);

  if (result) {
    /* Iterate over the tracing events */
    CassIterator* iterator = cass_iterator_from_result(result);
    if (iterator) {
      int event_count = 1;
      while (cass_iterator_next(iterator)) {
        const char* activity;
        size_t activity_length;
        CassInet source;
        cass_int32_t source_elapsed;
        char source_str[CASS_INET_STRING_LENGTH];
        const CassRow* row = cass_iterator_get_row(iterator);

        /* Get the activity for the event which host happened on, and how long
         * it took.
         */
        cass_value_get_string(cass_row_get_column_by_name(row, "activity"),
                              &activity, &activity_length);
        cass_value_get_inet(cass_row_get_column_by_name(row, "source"),
                            &source);
        cass_value_get_int32(cass_row_get_column_by_name(row, "source_elapsed"),
                             &source_elapsed);

        cass_inet_string(source, source_str);
        printf("%2d) Event on host %s (%f milliseconds): \"%.*s\"\n",
               event_count++,
               source_str, source_elapsed / 1000.0,
               (int)activity_length, activity);
      }

      cass_iterator_free(iterator);
    }

    cass_result_free(result);
  }
}

int main(int argc, char* argv[]) {
  /* Setup and connect to cluster */
  CassFuture* connect_future = NULL;
  CassCluster* cluster = cass_cluster_new();
  CassSession* session = cass_session_new();
  char* hosts = "127.0.0.1";
  if (argc > 1) {
    hosts = argv[1];
  }

  /* Add contact points */
  cass_cluster_set_contact_points(cluster, hosts);

  /* Provide the cluster object as configuration to connect the session */
  connect_future = cass_session_connect(session, cluster);

  if (cass_future_error_code(connect_future) == CASS_OK) {
    /* Build statement and execute query */
    CassFuture* result_future = NULL;
    const char* query = "SELECT release_version FROM system.local";
    CassStatement* statement = cass_statement_new(query, 0);

    /* Enable tracing on the statement */
    cass_statement_set_tracing(statement, cass_true);

    result_future = cass_session_execute(session, statement);

    if (cass_future_error_code(result_future) == CASS_OK) {
      /* Retrieve result set and get the first row */
      const CassResult* result = cass_future_get_result(result_future);
      const CassRow* row = cass_result_first_row(result);

      /* Print result of query */
      printf("Query result:\n");
      if (row) {
        const CassValue* value = cass_row_get_column_by_name(row, "release_version");

        const char* release_version;
        size_t release_version_length;
        cass_value_get_string(value, &release_version, &release_version_length);
        printf("release_version: '%.*s'\n", (int)release_version_length,
               release_version);
      } else {
        printf("No rows returned\n");
      }

      /* Print out basic tracing information */
      printf("\nTracing data:\n");
      print_tracing_data(session, result_future);

      cass_result_free(result);
    } else {
      /* Handle error */
      print_error("Unable to run query", result_future);
    }

    cass_statement_free(statement);
    cass_future_free(result_future);
  } else {
    /* Handle error */
    print_error("Unable to connect", connect_future);
  }

  cass_future_free(connect_future);
  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;
}
