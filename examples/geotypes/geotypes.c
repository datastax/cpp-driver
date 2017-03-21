/*
  Copyright (c) 2016 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include <dse.h>

#include <stdarg.h>
#include <stdio.h>
#include <string.h>

CassDataType* point_user_type;
CassDataType* line_string_user_type;
CassDataType* polygon_user_type;

void print_error(CassFuture* future) {
  const char* message;
  size_t message_length;
  cass_future_error_message(future, &message, &message_length);
  fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
}

CassCluster* create_cluster(const char* hosts) {
  CassCluster* cluster = cass_cluster_new();
  cass_cluster_set_contact_points(cluster, hosts);
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

void print_point(cass_double_t x, cass_double_t y) {
  printf("POINT(%.1f %.1f)", x, y);
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
    if (result && cass_result_row_count(result) > 0) {
      const CassRow* row = cass_result_first_row(result);
      const CassValue* value = cass_row_get_column_by_name(row, "point");

      cass_value_get_dse_point(value, &x, &y);
      printf("%s: ", key);
      print_point(x, y);
      printf("\n");

      cass_result_free(result);
    }
  } else {
    print_error(future);
  }

  cass_future_free(future);
  cass_statement_free(statement);
}

void insert_point_collections(CassSession* session, const char* key) {
  CassFuture* future = NULL;
  CassStatement* statement
      = cass_statement_new("INSERT INTO examples.geotypes_collections "
                           "(key, point_list, point_tuple, point_udt) VALUES (?, ?, ?, ?)", 4);

  cass_statement_bind_string(statement, 0, key);

  { /* Bind point list */
    CassCollection* point_list = cass_collection_new(CASS_COLLECTION_TYPE_LIST, 2);
    cass_collection_append_dse_point(point_list, 1.0, 2.0);
    cass_collection_append_dse_point(point_list, 2.0, 3.0);
    cass_statement_bind_collection(statement, 1, point_list);
    cass_collection_free(point_list);
  }

  { /* Bind point tuple */
    CassTuple* point_tuple = cass_tuple_new(2);
    cass_tuple_set_dse_point(point_tuple, 0, 3.0, 4.0);
    cass_tuple_set_dse_point(point_tuple, 1, 4.0, 5.0);
    cass_statement_bind_tuple(statement, 2, point_tuple);
    cass_tuple_free(point_tuple);
  }

  { /* Bind point UDT */
    CassUserType* point_udt = cass_user_type_new_from_data_type(point_user_type);

    /* Set using a name */
    cass_user_type_set_dse_point_by_name(point_udt, "point1", 5.0, 6.0);

    /* Set using an index */
    cass_user_type_set_dse_point(point_udt, 1, 6.0, 7.0);

    cass_statement_bind_user_type(statement, 3, point_udt);
    cass_user_type_free(point_udt);
  }

  future = cass_session_execute(session, statement);

  if (cass_future_error_code(future) != CASS_OK) {
    print_error(future);
  }

  cass_future_free(future);
  cass_statement_free(statement);
}

void select_point_collections(CassSession* session, const char* key) {
  CassFuture* future = NULL;
  CassStatement* statement
      = cass_statement_new("SELECT point_list, point_tuple, point_udt "
                           "FROM examples.geotypes_collections WHERE key = ?", 1);

  cass_statement_bind_string(statement, 0, key);

  future = cass_session_execute(session, statement);

  if (cass_future_error_code(future) == CASS_OK) {
    const CassResult* result = cass_future_get_result(future);
    if (result && cass_result_row_count(result) > 0) {
      const CassRow* row = cass_result_first_row(result);

      { /* Get point list */
        int i = 0;
        const CassValue* point_list = cass_row_get_column_by_name(row, "point_list");
        CassIterator* iterator = cass_iterator_from_collection(point_list);
        printf("point_list: [");
        while (cass_iterator_next(iterator)) {
          cass_double_t x, y;
          const CassValue* point = cass_iterator_get_value(iterator);
          cass_value_get_dse_point(point, &x, &y);
          if (i++ > 0) printf(", ");
          print_point(x, y);
        }
        printf("]\n");
        cass_iterator_free(iterator);
      }

      { /* Get point tuple */
        int i = 0;
        const CassValue* point_tuple = cass_row_get_column_by_name(row, "point_tuple");
        CassIterator* iterator = cass_iterator_from_tuple(point_tuple);
        printf("point_tuple: (");
        while (cass_iterator_next(iterator)) {
          cass_double_t x, y;
          const CassValue* point = cass_iterator_get_value(iterator);
          cass_value_get_dse_point(point, &x, &y);
          if (i++ > 0) printf(", ");
          print_point(x, y);
        }
        printf(")\n");
        cass_iterator_free(iterator);
      }

      { /* Get point UDT */
        int i = 0;
        const CassValue* point_udt = cass_row_get_column_by_name(row, "point_udt");
        CassIterator* iterator = cass_iterator_fields_from_user_type(point_udt);
        printf("point_udt: {");
        while (cass_iterator_next(iterator)) {
          const char* field_name;
          size_t field_name_length;
          cass_double_t x, y;
          const CassValue* point = cass_iterator_get_user_type_field_value(iterator);
          cass_iterator_get_user_type_field_name(iterator, &field_name, &field_name_length);
          cass_value_get_dse_point(point, &x, &y);
          if (i++ > 0) printf(", ");
          printf("%.*s: ", (int)field_name_length, field_name);
          print_point(x, y);
        }
        printf("}\n");
      }

      cass_result_free(result);
    }
  } else {
    print_error(future);
  }

  cass_future_free(future);
  cass_statement_free(statement);
}

void print_line_string(DseLineStringIterator* iterator) {
  cass_int32_t i, num_points = dse_line_string_iterator_num_points(iterator) ;
  printf("LINESTRING(");
  for (i = 0; i < num_points; ++i) {
    cass_double_t x, y;
    dse_line_string_iterator_next_point(iterator, &x, &y);
    if (i > 0) printf(", ");
    printf("%.1f %.1f", x, y);
  }
  printf(")");
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
    if (result && cass_result_row_count(result) > 0) {
      cass_uint32_t i, num_points;
      const CassRow* row = cass_result_first_row(result);
      const CassValue* value = cass_row_get_column_by_name(row, "linestring");

      DseLineStringIterator* iterator = dse_line_string_iterator_new();

      dse_line_string_iterator_reset(iterator, value);

      num_points = dse_line_string_iterator_num_points(iterator);


      printf("%s: ", key);
      print_line_string(iterator);
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

void insert_line_string_collections(CassSession* session, const char* key) {
  CassFuture* future = NULL;
  CassStatement* statement
      = cass_statement_new("INSERT INTO examples.geotypes_collections "
                           "(key, linestring_set, linestring_tuple, linestring_udt) VALUES (?, ?, ?, ?)", 4);
  DseLineString* line_string = dse_line_string_new();

  cass_statement_bind_string(statement, 0, key);

  { /* Bind line string set */
    CassCollection* line_string_set = cass_collection_new(CASS_COLLECTION_TYPE_SET, 2);

    dse_line_string_reset(line_string);
    dse_line_string_add_point(line_string, 0.0, 0.0);
    dse_line_string_add_point(line_string, 1.0, 1.0);
    dse_line_string_finish(line_string);
    cass_collection_append_dse_line_string(line_string_set, line_string);

    dse_line_string_reset(line_string);
    dse_line_string_add_point(line_string, 1.0, 1.0);
    dse_line_string_add_point(line_string, 2.0, 2.0);
    dse_line_string_finish(line_string);
    cass_collection_append_dse_line_string(line_string_set, line_string);

    cass_statement_bind_collection(statement, 1, line_string_set);
    cass_collection_free(line_string_set);
  }

  { /* Bind line string tuple */
    CassTuple* line_string_tuple = cass_tuple_new(2);

    dse_line_string_reset(line_string);
    dse_line_string_add_point(line_string, 1.0, 1.0);
    dse_line_string_add_point(line_string, 2.0, 2.0);
    dse_line_string_add_point(line_string, 3.0, 3.0);
    dse_line_string_finish(line_string);
    cass_tuple_set_dse_line_string(line_string_tuple, 0, line_string);

    dse_line_string_reset(line_string);
    dse_line_string_add_point(line_string, 4.0, 4.0);
    dse_line_string_add_point(line_string, 5.0, 5.0);
    dse_line_string_add_point(line_string, 6.0, 6.0);
    dse_line_string_finish(line_string);
    cass_tuple_set_dse_line_string(line_string_tuple, 1, line_string);

    cass_statement_bind_tuple(statement, 2, line_string_tuple);
    cass_tuple_free(line_string_tuple);
  }

  { /* Bind line string UDT */
    CassUserType* line_string_udt = cass_user_type_new_from_data_type(line_string_user_type);

    /* Set using a name */
    dse_line_string_reset(line_string);
    dse_line_string_add_point(line_string, 0.0, 0.0);
    dse_line_string_add_point(line_string, 0.0, 1.0);
    dse_line_string_add_point(line_string, 1.0, 2.0);
    dse_line_string_add_point(line_string, 2.0, 3.0);
    dse_line_string_finish(line_string);
    cass_user_type_set_dse_line_string_by_name(line_string_udt, "linestring1", line_string);

    /* Set using an index */
    dse_line_string_reset(line_string);
    dse_line_string_add_point(line_string, 2.0, 3.0);
    dse_line_string_add_point(line_string, 3.0, 5.0);
    dse_line_string_add_point(line_string, 5.0, 7.0);
    dse_line_string_add_point(line_string, 7.0, 9.0);
    dse_line_string_finish(line_string);
    cass_user_type_set_dse_line_string(line_string_udt, 1, line_string);

    cass_statement_bind_user_type(statement, 3, line_string_udt);
    cass_user_type_free(line_string_udt);
  }

  future = cass_session_execute(session, statement);

  if (cass_future_error_code(future) != CASS_OK) {
    print_error(future);
  }

  dse_line_string_free(line_string);
  cass_future_free(future);
  cass_statement_free(statement);
}

void select_line_string_collections(CassSession* session, const char* key) {
  CassFuture* future = NULL;
  CassStatement* statement
      = cass_statement_new("SELECT linestring_set, linestring_tuple, linestring_udt "
                           "FROM examples.geotypes_collections WHERE key = ?", 1);
  DseLineStringIterator* line_string_iterator = dse_line_string_iterator_new();

  cass_statement_bind_string(statement, 0, key);

  future = cass_session_execute(session, statement);

  if (cass_future_error_code(future) == CASS_OK) {
    const CassResult* result = cass_future_get_result(future);
    if (result && cass_result_row_count(result) > 0) {
      const CassRow* row = cass_result_first_row(result);

      { /* Get line string set */
        int i = 0;
        const CassValue* line_string_set = cass_row_get_column_by_name(row, "linestring_set");
        CassIterator* iterator = cass_iterator_from_collection(line_string_set);
        printf("linestring_set: [");
        while (cass_iterator_next(iterator)) {
          const CassValue* line_string = cass_iterator_get_value(iterator);
          dse_line_string_iterator_reset(line_string_iterator, line_string);
          if (i++ > 0) printf(", ");
          print_line_string(line_string_iterator);
        }
        printf("]\n");
        cass_iterator_free(iterator);
      }

      { /* Get line string tuple */
        int i = 0;
        const CassValue* line_string_tuple = cass_row_get_column_by_name(row, "linestring_tuple");
        CassIterator* iterator = cass_iterator_from_tuple(line_string_tuple);
        printf("linestring_tuple: (");
        while (cass_iterator_next(iterator)) {
          const CassValue* line_string = cass_iterator_get_value(iterator);
          dse_line_string_iterator_reset(line_string_iterator, line_string);
          if (i++ > 0) printf(", ");
          print_line_string(line_string_iterator);
        }
        printf(")\n");
        cass_iterator_free(iterator);
      }

      { /* Get line string UDT */
        int i = 0;
        const CassValue* line_string_udt = cass_row_get_column_by_name(row, "linestring_udt");
        CassIterator* iterator = cass_iterator_fields_from_user_type(line_string_udt);
        printf("linestring_udt: {");
        while (cass_iterator_next(iterator)) {
          const char* field_name;
          size_t field_name_length;
          const CassValue* line_string = cass_iterator_get_user_type_field_value(iterator);
          dse_line_string_iterator_reset(line_string_iterator, line_string);
          cass_iterator_get_user_type_field_name(iterator, &field_name, &field_name_length);
          if (i++ > 0) printf(", ");
          printf("%.*s: ", (int)field_name_length, field_name);
          print_line_string(line_string_iterator);
        }
        printf("}\n");
      }

      cass_result_free(result);
    }
  } else {
    print_error(future);
  }

  dse_line_string_iterator_free(line_string_iterator);
  cass_future_free(future);
  cass_statement_free(statement);
}

void print_polygon(DsePolygonIterator* iterator) {
  cass_int32_t i, num_rings = dse_polygon_iterator_num_rings(iterator);
  printf("POLYGON(");
  for (i = 0; i < num_rings; ++i) {
    cass_uint32_t j, num_points = 0;
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
  printf(")");
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
    if (result && cass_result_row_count(result) > 0) {
      const CassRow* row = cass_result_first_row(result);
      const CassValue* value = cass_row_get_column_by_name(row, "polygon");

      DsePolygonIterator* iterator = dse_polygon_iterator_new();

      dse_polygon_iterator_reset(iterator, value);

      printf("%s: ", key);
      print_polygon(iterator);
      printf("\n");

      dse_polygon_iterator_free(iterator);
      cass_result_free(result);
    }
  } else {
    print_error(future);
  }

  cass_future_free(future);
  cass_statement_free(statement);
}

void insert_polygon_collections(CassSession* session, const char* key) {
  CassFuture* future = NULL;
  CassStatement* statement
      = cass_statement_new("INSERT INTO examples.geotypes_collections "
                           "(key, polygon_map, polygon_tuple, polygon_udt) VALUES (?, ?, ?, ?)", 4);
  DsePolygon* polygon = dse_polygon_new();

  cass_statement_bind_string(statement, 0, key);

  { /* Bind polygon map */
    CassCollection* polygon_map = cass_collection_new(CASS_COLLECTION_TYPE_MAP, 2);

    cass_collection_append_string(polygon_map, "poly1");
    dse_polygon_reset(polygon);
    dse_polygon_start_ring(polygon);
    dse_polygon_add_point(polygon, 0.0, 0.0);
    dse_polygon_add_point(polygon, 1.0, 0.0);
    dse_polygon_add_point(polygon, 1.0, 1.0);
    dse_polygon_add_point(polygon, 0.0, 0.0);
    dse_polygon_finish(polygon);
    cass_collection_append_dse_polygon(polygon_map, polygon);

    cass_collection_append_string(polygon_map, "poly2");
    dse_polygon_reset(polygon);
    dse_polygon_start_ring(polygon);
    dse_polygon_add_point(polygon, 0.0, 0.0);
    dse_polygon_add_point(polygon, 1.0, 0.0);
    dse_polygon_add_point(polygon, 1.0, 1.0);
    dse_polygon_add_point(polygon, 0.0, 1.0);
    dse_polygon_add_point(polygon, 0.0, 0.0);
    dse_polygon_finish(polygon);
    cass_collection_append_dse_polygon(polygon_map, polygon);

    cass_statement_bind_collection(statement, 1, polygon_map);
    cass_collection_free(polygon_map);
  }

  { /* Bind polygon tuple */
    CassTuple* polygon_tuple = cass_tuple_new(2);

    dse_polygon_reset(polygon);
    dse_polygon_start_ring(polygon);
    dse_polygon_add_point(polygon, 0.0, 0.0);
    dse_polygon_add_point(polygon, 2.0, 0.0);
    dse_polygon_add_point(polygon, 2.0, 2.0);
    dse_polygon_add_point(polygon, 0.0, 0.0);
    dse_polygon_finish(polygon);
    cass_tuple_set_dse_polygon(polygon_tuple, 0, polygon);

    dse_polygon_reset(polygon);
    dse_polygon_start_ring(polygon);
    dse_polygon_add_point(polygon, 0.0, 0.0);
    dse_polygon_add_point(polygon, 2.0, 0.0);
    dse_polygon_add_point(polygon, 2.0, 2.0);
    dse_polygon_add_point(polygon, 0.0, 2.0);
    dse_polygon_add_point(polygon, 0.0, 0.0);
    dse_polygon_finish(polygon);
    cass_tuple_set_dse_polygon(polygon_tuple, 1, polygon);

    cass_statement_bind_tuple(statement, 2, polygon_tuple);
    cass_tuple_free(polygon_tuple);
  }

  { /* Bind polygon UDT */
    CassUserType* polygon_udt = cass_user_type_new_from_data_type(polygon_user_type);

    /* Set using a name */
    dse_polygon_reset(polygon);
    dse_polygon_start_ring(polygon);
    dse_polygon_add_point(polygon, 0.0, 0.0);
    dse_polygon_add_point(polygon, 3.0, 0.0);
    dse_polygon_add_point(polygon, 3.0, 3.0);
    dse_polygon_add_point(polygon, 0.0, 3.0);
    dse_polygon_add_point(polygon, 0.0, 0.0);
    dse_polygon_finish(polygon);
    cass_user_type_set_dse_polygon_by_name(polygon_udt, "polygon1", polygon);

    /* Set using a index */
    dse_polygon_reset(polygon);
    dse_polygon_start_ring(polygon);
    dse_polygon_add_point(polygon, 0.0, 0.0);
    dse_polygon_add_point(polygon, 4.0, 0.0);
    dse_polygon_add_point(polygon, 4.0, 4.0);
    dse_polygon_add_point(polygon, 0.0, 4.0);
    dse_polygon_add_point(polygon, 0.0, 0.0);
    dse_polygon_finish(polygon);
    cass_user_type_set_dse_polygon(polygon_udt, 1, polygon);

    cass_statement_bind_user_type(statement, 3, polygon_udt);
    cass_user_type_free(polygon_udt);
  }

  future = cass_session_execute(session, statement);

  if (cass_future_error_code(future) != CASS_OK) {
    print_error(future);
  }

  dse_polygon_free(polygon);
  cass_future_free(future);
  cass_statement_free(statement);
}

void select_polygon_collections(CassSession* session, const char* key) {
  CassFuture* future = NULL;
  CassStatement* statement
      = cass_statement_new("SELECT polygon_map, polygon_tuple, polygon_udt "
                           "FROM examples.geotypes_collections WHERE key = ?", 1);
  DsePolygonIterator* polygon_iterator = dse_polygon_iterator_new();

  cass_statement_bind_string(statement, 0, key);

  future = cass_session_execute(session, statement);

  if (cass_future_error_code(future) == CASS_OK) {
    const CassResult* result = cass_future_get_result(future);
    if (result && cass_result_row_count(result) > 0) {
      const CassRow* row = cass_result_first_row(result);

      { /* Get polygon map */
        int i = 0;
        const CassValue* polygon_list = cass_row_get_column_by_name(row, "polygon_map");
        CassIterator* iterator = cass_iterator_from_map(polygon_list);
        printf("polygon_map: {");
        while (cass_iterator_next(iterator)) {
          const char* map_name;
          size_t map_name_length;
          const CassValue* name = cass_iterator_get_map_key(iterator);
          const CassValue* polygon = cass_iterator_get_map_value(iterator);
          cass_value_get_string(name, &map_name, &map_name_length);
          dse_polygon_iterator_reset(polygon_iterator, polygon);
          if (i++ > 0) printf(", ");
          printf("%.*s: ", (int)map_name_length, map_name);
          print_polygon(polygon_iterator);
        }
        printf("}\n");
        cass_iterator_free(iterator);
      }

      { /* Get polygon tuple */
        int i = 0;
        const CassValue* polygon_tuple = cass_row_get_column_by_name(row, "polygon_tuple");
        CassIterator* iterator = cass_iterator_from_tuple(polygon_tuple);
        printf("polygon_tuple: (");
        while (cass_iterator_next(iterator)) {
          const CassValue* polygon = cass_iterator_get_value(iterator);
          dse_polygon_iterator_reset(polygon_iterator, polygon);
          if (i++ > 0) printf(", ");
          print_polygon(polygon_iterator);
        }
        printf(")\n");
        cass_iterator_free(iterator);
      }

      { /* Get polygon UDT */
        int i = 0;
        const CassValue* polygon_udt = cass_row_get_column_by_name(row, "polygon_udt");
        CassIterator* iterator = cass_iterator_fields_from_user_type(polygon_udt);
        printf("polygon_udt: {");
        while (cass_iterator_next(iterator)) {
          const char* field_name;
          size_t field_name_length;
          const CassValue* polygon = cass_iterator_get_user_type_field_value(iterator);
          dse_polygon_iterator_reset(polygon_iterator, polygon);
          cass_iterator_get_user_type_field_name(iterator, &field_name, &field_name_length);
          if (i++ > 0) printf(", ");
          printf("%.*s: ", (int)field_name_length, field_name);
          print_polygon(polygon_iterator);
        }
        printf("}\n");
      }

      cass_result_free(result);
    }
  } else {
    print_error(future);
  }

  dse_polygon_iterator_free(polygon_iterator);
  cass_future_free(future);
  cass_statement_free(statement);
}

int main(int argc, char* argv[]) {
  CassCluster* cluster = NULL;
  CassSession* session = cass_session_new();
  CassFuture* close_future = NULL;
  char* hosts = "127.0.0.1";
  if (argc > 1) {
    hosts = argv[1];
  }
  cluster = create_cluster(hosts);

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

  execute_query(session,
                "CREATE TYPE IF NOT EXISTS examples.point_user_type "
                "(point1 'PointType', point2 'PointType')");

  execute_query(session,
                "CREATE TYPE IF NOT EXISTS examples.linestring_user_type "
                "(linestring1 'LineStringType', linestring2 'LineStringType')");

  execute_query(session,
                "CREATE TYPE IF NOT EXISTS examples.polygon_user_type "
                "(polygon1 'PolygonType', polygon2 'PolygonType')");

  execute_query(session, "CREATE TABLE IF NOT EXISTS examples.geotypes_collections ("
                         "key text PRIMARY KEY, "
                         "point_list list<'PointType'>, "
                         "point_tuple tuple<'PointType', 'PointType'>, "
                         "point_udt point_user_type, "
                         "linestring_set set<'LineStringType'>, "
                         "linestring_tuple tuple<'LineStringType', 'LineStringType'>, "
                         "linestring_udt linestring_user_type, "
                         "polygon_map map<text, 'PolygonType'>, "
                         "polygon_tuple tuple<'PolygonType', 'PolygonType'>, "
                         "polygon_udt polygon_user_type)");

  { /* Get geospatial user types from schema metadata */
    const CassSchemaMeta* schema = cass_session_get_schema_meta(session);
    const CassKeyspaceMeta* keyspace = cass_schema_meta_keyspace_by_name(schema, "examples");

    point_user_type = cass_data_type_new_from_existing(
                        cass_keyspace_meta_user_type_by_name(keyspace, "point_user_type"));

    line_string_user_type = cass_data_type_new_from_existing(
                              cass_keyspace_meta_user_type_by_name(keyspace, "linestring_user_type"));

    polygon_user_type = cass_data_type_new_from_existing(
                          cass_keyspace_meta_user_type_by_name(keyspace, "polygon_user_type"));

    cass_schema_meta_free(schema);
  }

  printf("examples.geotypes (Point):\n");
  insert_point(session, "pnt1", 0.1, 0.1);
  select_point(session, "pnt1");

  printf("\nexamples.geotypes (LineString):\n");
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

  printf("\nexamples.geotypes (Polygon):\n");
  insert_polygon(session, "poly1", 0);
  select_polygon(session, "poly1");

  insert_polygon(session, "poly2", 2,
                 5,
                 35.0, 10.0, 45.0, 45.0,
                 15.0, 40.0, 10.0, 20.0,
                 35.0, 10.0,
                 4,
                 20.0, 30.0, 35.0, 35.0,
                 30.0, 20.0, 20.0, 30.0);
  select_polygon(session, "poly2");

  printf("\nexamples.geotypes_collections (Point):\n");
  insert_point_collections(session, "pntcoll1");
  select_point_collections(session, "pntcoll1");

  printf("\nexamples.geotypes_collections (LineString):\n");
  insert_line_string_collections(session, "lnstrcoll1");
  select_line_string_collections(session, "lnstrcoll1");

  printf("\nexamples.geotypes_collections (Polygon):\n");
  insert_polygon_collections(session, "polycoll1");
  select_polygon_collections(session, "polycoll1");

  close_future = cass_session_close(session);
  cass_future_wait(close_future);
  cass_future_free(close_future);

  cass_data_type_free(point_user_type);
  cass_data_type_free(line_string_user_type);
  cass_data_type_free(polygon_user_type);

  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;
}

