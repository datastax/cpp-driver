/*
  Copyright (c) 2016 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include <dse.h>

#include <stdarg.h>
#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
  #include <windows.h>
#else
  #include <unistd.h>
#endif
#define GRAPH_ALLOW_SCANS \
  "schema.config().option('graph.allow_scan').set('true')"

#define GRAPH_MAKE_STRICT \
  "schema.config().option('graph.schema_mode').set(com.datastax.bdp.graph.api.model.Schema.Mode.Production)"

#define GRAPH_SCHEMA \
  "schema.propertyKey('name').Text().ifNotExists().create();" \
  "schema.propertyKey('age').Int().ifNotExists().create();" \
  "schema.propertyKey('lang').Text().ifNotExists().create();" \
  "schema.propertyKey('weight').Float().ifNotExists().create();" \
  "schema.vertexLabel('person').properties('name', 'age').ifNotExists().create();" \
  "schema.vertexLabel('software').properties('name', 'lang').ifNotExists().create();" \
  "schema.edgeLabel('created').properties('weight').connection('person', 'software').ifNotExists().create();" \
  "schema.edgeLabel('created').connection('software', 'software').add();" \
  "schema.edgeLabel('knows').properties('weight').connection('person', 'person').ifNotExists().create();"

#define GRAPH_DATA \
  "Vertex marko = graph.addVertex(label, 'person', 'name', 'marko', 'age', 29);" \
  "Vertex vadas = graph.addVertex(label, 'person', 'name', 'vadas', 'age', 27);" \
  "Vertex lop = graph.addVertex(label, 'software', 'name', 'lop', 'lang', 'java');" \
  "Vertex josh = graph.addVertex(label, 'person', 'name', 'josh', 'age', 32);" \
  "Vertex ripple = graph.addVertex(label, 'software', 'name', 'ripple', 'lang', 'java');" \
  "Vertex peter = graph.addVertex(label, 'person', 'name', 'peter', 'age', 35);" \
  "marko.addEdge('knows', vadas, 'weight', 0.5f);" \
  "marko.addEdge('knows', josh, 'weight', 1.0f);" \
  "marko.addEdge('created', lop, 'weight', 0.4f);" \
  "josh.addEdge('created', ripple, 'weight', 1.0f);" \
  "josh.addEdge('created', lop, 'weight', 0.4f);" \
  "peter.addEdge('created', lop, 'weight', 0.2f);"

void print_error(CassFuture* future) {
  const char* message;
  size_t message_length;
  cass_future_error_message(future, &message, &message_length);
  fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
}

void print_indented(int indent, const char* format, ...) {
  va_list args;

  printf("%*s", indent, "");
  va_start(args, format);
  vprintf(format, args);
  va_end(args);
}

void print_graph_result(int indent, const DseGraphResult* result) {
  size_t i, count;

  switch(dse_graph_result_type(result)) {
    case DSE_GRAPH_RESULT_TYPE_NULL:
      print_indented(indent, "null");
      break;
    case DSE_GRAPH_RESULT_TYPE_BOOL:
      print_indented(indent, "%s",
                     dse_graph_result_get_bool(result) ? "true" : "false");
      break;
    case DSE_GRAPH_RESULT_TYPE_NUMBER:
      if (dse_graph_result_is_int32(result)) {
        print_indented(indent, "%d", dse_graph_result_get_int32(result));
      } else if (dse_graph_result_is_int64(result)) {
        print_indented(indent, "%lld", dse_graph_result_get_int64(result));
      } else {
        print_indented(indent, "%f", dse_graph_result_get_double(result));
      }
      break;
    case DSE_GRAPH_RESULT_TYPE_STRING:
      print_indented(indent, "\"%s\"", dse_graph_result_get_string(result, NULL));
      break;
    case DSE_GRAPH_RESULT_TYPE_OBJECT:
      count = dse_graph_result_member_count(result);
      print_indented(indent, "{");
      for (i = 0; i < count; ++i) {
        const char* key = dse_graph_result_member_key(result, i, NULL);
        const DseGraphResult* value = dse_graph_result_member_value(result, i);
        DseGraphResultType type = dse_graph_result_type(value);
        printf("\n");
        print_indented(indent + 4, "\"%s\": ", key);
        if (type == DSE_GRAPH_RESULT_TYPE_OBJECT ||
            type == DSE_GRAPH_RESULT_TYPE_ARRAY) {
          printf("\n");
          print_graph_result(indent + 4, value);
        } else {
          print_graph_result(0, value);
        }
        printf(",");
      }
      printf("\n");
      print_indented(indent, "}");
      break;
    case DSE_GRAPH_RESULT_TYPE_ARRAY:
      count = dse_graph_result_element_count(result);
      print_indented(indent, "[");
      for (i = 0; i < count; ++i) {
        const DseGraphResult* element = dse_graph_result_element(result, i);
        printf("\n");
        print_graph_result(indent + 4, element);
        printf(",");
      }
      printf("\n");
      print_indented(indent, "]");
      break;
  }
}

void print_graph_resultset(DseGraphResultSet* resultset) {
  size_t i, count = dse_graph_resultset_count(resultset);
  printf("[\n");
  for (i = 0; i < count; ++i) {
    print_graph_result(4, dse_graph_resultset_next(resultset));
    printf(",\n");
  }
  printf("]\n");
}

cass_bool_t execute_graph_query(CassSession* session,
                                const char* query,
                                const DseGraphOptions* options,
                                const DseGraphObject* values,
                                DseGraphResultSet** resultset) {
  cass_bool_t is_success = cass_false;
  CassFuture* future;
  DseGraphStatement* statement = dse_graph_statement_new(query, options);

  dse_graph_statement_bind_values(statement, values);

  future = cass_session_execute_dse_graph(session, statement);

  if (cass_future_error_code(future) == CASS_OK) {
    DseGraphResultSet* rs = cass_future_get_dse_graph_resultset(future);
    if (resultset) {
      *resultset = rs;
    } else {
      dse_graph_resultset_free(rs);
    }
    is_success = cass_true;
  } else {
    print_error(future);
  }

  cass_future_free(future);
  dse_graph_statement_free(statement);
  return is_success;
}

cass_bool_t create_graph(CassSession* session, const char* name) {
  size_t i;
  cass_bool_t is_success = cass_false;
  DseGraphObject* values = dse_graph_object_new();

  dse_graph_object_add_string(values, "name", name);
  dse_graph_object_finish(values);

  if (execute_graph_query(session,
                          "graph = system.graph(name);" \
                          "if (graph.exists()) graph.drop();" \
                          "graph.create();",
                          NULL, values, NULL)) {
    for  (i = 0; i < 10; ++i) {
      DseGraphResultSet* resultset;
      if (execute_graph_query(session,
                              "system.graph(name).exists()",
                              NULL, values, &resultset)) {
        if (dse_graph_resultset_count(resultset) > 0) {
          const DseGraphResult* result = dse_graph_resultset_next(resultset);
          if (dse_graph_result_is_bool(result) && dse_graph_result_get_bool(result)) {
            is_success = cass_true;
            dse_graph_resultset_free(resultset);
            break;
          }
        }
#ifndef _WIN32
        sleep(1);
#else
        Sleep(1000);
#endif
        dse_graph_resultset_free(resultset);
      }
    }
  }

  dse_graph_object_free(values);
  return is_success;
}

void execute_graph_query_and_print(CassSession* session,
                                   const char* query,
                                   const DseGraphOptions* options,
                                   const DseGraphObject* values) {
  DseGraphResultSet* resultset;
  if (execute_graph_query(session, query, options, values, &resultset)) {
    print_graph_resultset(resultset);
    dse_graph_resultset_free(resultset);
  }
}

int main() {
  /* Setup and connect to cluster */
  CassFuture* connect_future = NULL;
  CassCluster* cluster = cass_cluster_new();
  CassSession* session = cass_session_new();

  cass_log_set_level(CASS_LOG_INFO);

  /* Add contact points */
  cass_cluster_set_contact_points(cluster, "127.0.0.1");

  /* Provide the cluster object as configuration to connect the session */
  connect_future = cass_session_connect(session, cluster);

  if (cass_future_error_code(connect_future) == CASS_OK) {
    CassFuture* close_future;
    DseGraphOptions* options = dse_graph_options_new();

    dse_graph_options_set_graph_name(options, "classic");

    if (create_graph(session, "classic")) {
      execute_graph_query(session, GRAPH_ALLOW_SCANS, options, NULL, NULL);
      execute_graph_query(session, GRAPH_MAKE_STRICT, options, NULL, NULL);
      execute_graph_query(session, GRAPH_SCHEMA, options, NULL, NULL);
      execute_graph_query(session, GRAPH_DATA, options, NULL, NULL);

      printf("Who does 'marko' know?\n");
      execute_graph_query_and_print(session, "g.V().has('name','marko').out('knows').values('name')", options, NULL);

      printf("What vertices are connected to 'marko'?\n");
      execute_graph_query_and_print(session, "g.V().has('name', 'marko').out('knows')", options, NULL);
    }

    dse_graph_options_free(options);

    /* Close the session */
    close_future = cass_session_close(session);
    cass_future_wait(close_future);
    cass_future_free(close_future);
  } else {
    /* Handle error */
    const char* message;
    size_t message_length;
    cass_future_error_message(connect_future, &message, &message_length);
    fprintf(stderr, "Unable to connect: '%.*s'\n", (int)message_length,
                                                        message);
  }

  cass_future_free(connect_future);
  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;
}
