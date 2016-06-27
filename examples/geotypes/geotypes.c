/*
  Copyright (c) 2016 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include <dse.h>

#include <stdarg.h>
#include <stdio.h>
#include <string.h>

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

void insert_point(CassSession* session, const char* key,
                  cass_double_t x, cass_double_t y) {
  CassFuture* future = NULL;
  CassStatement* statement
      = cass_statement_new("INSERT INTO examples.geotypes "
                           "(key, point) VALUES (?, ?)", 2);

  cass_statement_bind_string(statement, 0, key);
  cass_statement_bind_dse_point(statement, 1, x, y);

  future = cass_session_execute(session, statement);

  if (cass_future_error_code(future) != CASS_OK) {
    print_error(future);
  }

  cass_future_free(future);
  cass_statement_free(statement);
}

void select_point(CassSession* session, const char* key) {
  CassFuture* future = NULL;
  CassStatement* statement
      = cass_statement_new("SELECT point FROM examples.geotypes WHERE key = ?", 1);

  cass_statement_bind_string(statement, 0, key);

  future = cass_session_execute(session, statement);

  if (cass_future_error_code(future) == CASS_OK) {
    cass_double_t x = 0.0, y = 0.0;
    const CassResult* result = cass_future_get_result(future);
    if (result && cass_result_column_count(result) > 0) {
      const CassRow* row = cass_result_first_row(result);
      const CassValue* value = cass_row_get_column_by_name(row, "point");

      cass_value_get_dse_point(value, &x, &y);
      printf("%s : POINT(%.1f %.1f)\n", key, x, y);

      cass_result_free(result);
    }
  } else {
    print_error(future);
  }

  cass_future_free(future);
  cass_statement_free(statement);
}

void insert_line_string(CassSession* session, const char* key, int num_points, ...) {
  int i;
  va_list args;
  CassFuture* future = NULL;
  CassStatement* statement
      = cass_statement_new("INSERT INTO examples.geotypes "
                           "(key, linestring) VALUES (?, ?)", 2);
  DseLineString* line_string = dse_line_string_new();

  dse_line_string_reserve(line_string, num_points);

  va_start(args, num_points);
  for (i = 0; i < num_points; ++i) {
    cass_double_t x = va_arg(args, cass_double_t);
    cass_double_t y = va_arg(args, cass_double_t);
    dse_line_string_add_point(line_string, x, y);
  }
  va_end(args);

  dse_line_string_finish(line_string);

  cass_statement_bind_string(statement, 0, key);
  cass_statement_bind_dse_line_string(statement, 1, line_string);

  future = cass_session_execute(session, statement);

  if (cass_future_error_code(future) != CASS_OK) {
    print_error(future);
  }

  cass_future_free(future);
  dse_line_string_free(line_string);
  cass_statement_free(statement);
}

void select_line_string(CassSession* session, const char* key) {
  CassFuture* future = NULL;
  CassStatement* statement
      = cass_statement_new("SELECT linestring FROM examples.geotypes WHERE key = ?", 1);

  cass_statement_bind_string(statement, 0, key);

  future = cass_session_execute(session, statement);

  if (cass_future_error_code(future) == CASS_OK) {
    const CassResult* result = cass_future_get_result(future);
    if (result && cass_result_column_count(result) > 0) {
      cass_uint32_t i, num_points;
      const CassRow* row = cass_result_first_row(result);
      const CassValue* value = cass_row_get_column_by_name(row, "linestring");

      DseLineStringIterator* iterator = dse_line_string_iterator_new();

      dse_line_string_iterator_reset(iterator, value);

      num_points = dse_line_string_iterator_num_points(iterator);

      printf("%s : LINESTRING(", key);
      for (i = 0; i < num_points; ++i) {
        cass_double_t x = 0.0, y = 0.0;
        dse_line_string_iterator_next_point(iterator, &x, &y);
        if (i > 0) printf(", ");
        printf("%.1f %.1f", x, y);
      }
      printf(")\n");

      dse_line_string_iterator_free(iterator);
      cass_result_free(result);
    }
  } else {
    print_error(future);
  }

  cass_future_free(future);
  cass_statement_free(statement);
}

void insert_polygon(CassSession* session, const char* key, int num_rings, ...) {
  int i, j;
  va_list args;
  CassFuture* future = NULL;
  CassStatement* statement
      = cass_statement_new("INSERT INTO examples.geotypes "
                           "(key, polygon) VALUES (?, ?)", 2);

  DsePolygon* polygon = dse_polygon_new();

  va_start(args, num_rings);
  for (i = 0; i < num_rings; ++i) {
    int num_points = va_arg(args, int);
    dse_polygon_start_ring(polygon);
    for (j = 0; j < num_points; ++j) {
      cass_double_t x = va_arg(args, cass_double_t);
      cass_double_t y = va_arg(args, cass_double_t);
      dse_polygon_add_point(polygon, x, y);
    }
  }
  va_end(args);

  dse_polygon_finish(polygon);

  cass_statement_bind_string(statement, 0, key);
  cass_statement_bind_dse_polygon(statement, 1, polygon);

  future = cass_session_execute(session, statement);

  if (cass_future_error_code(future) != CASS_OK) {
    print_error(future);
  }

  cass_future_free(future);
  dse_polygon_free(polygon);
  cass_statement_free(statement);
}

void select_polygon(CassSession* session, const char* key) {
  CassFuture* future = NULL;
  CassStatement* statement
      = cass_statement_new("SELECT polygon FROM examples.geotypes WHERE key = ?", 1);

  cass_statement_bind_string(statement, 0, key);

  future = cass_session_execute(session, statement);

  if (cass_future_error_code(future) == CASS_OK) {
    const CassResult* result = cass_future_get_result(future);
    if (result && cass_result_column_count(result) > 0) {
      cass_uint32_t i, j, num_rings;
      const CassRow* row = cass_result_first_row(result);
      const CassValue* value = cass_row_get_column_by_name(row, "polygon");

      DsePolygonIterator* iterator = dse_polygon_iterator_new();

      dse_polygon_iterator_reset(iterator, value);

      num_rings = dse_polygon_iterator_num_rings(iterator);

      printf("%s : POLYGON(", key);
      for (i = 0; i < num_rings; ++i) {
        cass_uint32_t num_points = 0;
        dse_polygon_iterator_next_num_points(iterator, &num_points);
        printf("(");
        for (j = 0; j < num_points; ++j)  {
          cass_double_t x = 0.0, y;
          dse_polygon_iterator_next_point(iterator, &x, &y);
          if (j > 0) printf(", ");
          printf("%.1f %.1f", x, y);
        }
        printf(")");
      }
      printf(")\n");

      dse_polygon_iterator_free(iterator);
      cass_result_free(result);
    }
  } else {
    print_error(future);
  }

  cass_future_free(future);
  cass_statement_free(statement);
}

int main() {
  CassCluster* cluster = create_cluster();
  CassSession* session = cass_session_new();
  CassFuture* close_future = NULL;

  if (connect_session(session, cluster) != CASS_OK) {
    cass_cluster_free(cluster);
    cass_session_free(session);
    return -1;
  }

  execute_query(session,
                "CREATE KEYSPACE IF NOT EXISTS examples "
                "WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '3' };");

  execute_query(session, "CREATE TABLE IF NOT EXISTS examples.geotypes ("
                         "key text PRIMARY KEY, "
                         "point 'PointType', "
                         "linestring 'LineStringType', "
                         "polygon 'PolygonType')");

  insert_point(session, "pnt1", 0.1, 0.1);
  select_point(session, "pnt1");

  insert_line_string(session, "lnstr1", 0);
  select_line_string(session, "lnstr1");

  insert_line_string(session, "lnstr2", 2,
                     0.0, 0.0, 1.0, 1.0);
  select_line_string(session, "lnstr2");

  insert_line_string(session, "lnstr3", 3,
                     0.0, 0.0, 1.0, 0.0,
                     2.0, 0.0);
  select_line_string(session, "lnstr3");

  insert_line_string(session, "lnstr4", 4,
                     0.0, 0.0, 1.0, 0.0,
                     2.0, 0.0, 3.0, 0.0);
  select_line_string(session, "lnstr4");

  insert_polygon(session, "poly1", 0);
  select_polygon(session, "poly1");

  insert_polygon(session, "poly2", 1, 0);
  select_polygon(session, "poly2");

  insert_polygon(session, "poly3", 1,
                 3,
                 0.0, 0.0, 0.5, 0.0,
                 1.0, 1.0);
  select_polygon(session, "poly3");

  insert_polygon(session, "poly4", 2,
                 5,
                 35.0, 10.0, 45.0, 45.0,
                 15.0, 40.0, 10.0, 20.0,
                 35.0, 10.0,
                 4,
                 20.0, 30.0, 35.0, 35.0,
                 30.0, 20.0, 20.0, 30.0);
  select_polygon(session, "poly4");

  close_future = cass_session_close(session);
  cass_future_wait(close_future);
  cass_future_free(close_future);

  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;
}

