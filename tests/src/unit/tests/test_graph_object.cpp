/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include <gtest/gtest.h>

#include "dse.h"
#include "json.hpp"
#include "graph.hpp"

using namespace datastax;
using namespace datastax::internal;

class GraphObjectUnitTest : public testing::Test {
public:
  void SetUp() {
    graph_object = dse_graph_object_new();
    line_string = NULL;
    line_string_iterator = NULL;
    polygon = NULL;
    polygon_iterator = NULL;
  }

  void TearDown() {
    dse_graph_object_free(graph_object);
    if (line_string) {
      dse_line_string_free(line_string);
    }
    if (line_string_iterator) {
      dse_line_string_iterator_free(line_string_iterator);
    }
    if (polygon) {
      dse_polygon_free(polygon);
    }
    if (polygon_iterator) {
      dse_polygon_iterator_free(polygon_iterator);
    }
  }

  const DseGraphResult* to_graph_result() {
    if (document.Parse(graph_object->data()).HasParseError()) {
      return NULL;
    }
    return DseGraphResult::to(&document);
  }

  DseGraphObject* graph_object;
  json::Document document;
  DseLineString* line_string;
  DseLineStringIterator* line_string_iterator;
  DsePolygon* polygon;
  DsePolygonIterator* polygon_iterator;
};

TEST_F(GraphObjectUnitTest, PrimitiveTypes) {
  ASSERT_EQ(CASS_OK, dse_graph_object_add_null(graph_object, "null"));
  ASSERT_EQ(CASS_OK, dse_graph_object_add_bool(graph_object, "bool", cass_true));
  ASSERT_EQ(CASS_OK, dse_graph_object_add_int32(graph_object, "int32", 1));
  ASSERT_EQ(CASS_OK, dse_graph_object_add_int64(graph_object, "int64", 2));
  ASSERT_EQ(CASS_OK, dse_graph_object_add_double(graph_object, "double", 1.2));
  ASSERT_EQ(CASS_OK, dse_graph_object_add_string(graph_object, "string", "abc"));
  dse_graph_object_finish(graph_object);

  const DseGraphResult* graph_result = to_graph_result();
  ASSERT_TRUE(graph_result != NULL);
  ASSERT_TRUE(dse_graph_result_is_object(graph_result));

  ASSERT_EQ(6u, dse_graph_result_member_count(graph_result));

  const DseGraphResult* value;
  ASSERT_EQ("null", String(dse_graph_result_member_key(graph_result, 0, NULL)));
  value = dse_graph_result_member_value(graph_result, 0);
  ASSERT_TRUE(dse_graph_result_is_null(value));

  ASSERT_EQ("bool", String(dse_graph_result_member_key(graph_result, 1, NULL)));
  value = dse_graph_result_member_value(graph_result, 1);
  ASSERT_TRUE(dse_graph_result_is_bool(value));
  ASSERT_TRUE(dse_graph_result_get_bool(value));

  ASSERT_EQ("int32", String(dse_graph_result_member_key(graph_result, 2, NULL)));
  value = dse_graph_result_member_value(graph_result, 2);
  ASSERT_TRUE(dse_graph_result_is_int32(value));
  ASSERT_EQ(1, dse_graph_result_get_int32(value));

  ASSERT_EQ("int64", String(dse_graph_result_member_key(graph_result, 3, NULL)));
  value = dse_graph_result_member_value(graph_result, 3);
  ASSERT_TRUE(dse_graph_result_is_int64(value));
  ASSERT_EQ(2, dse_graph_result_get_int64(value));

  ASSERT_EQ("double", String(dse_graph_result_member_key(graph_result, 4, NULL)));
  value = dse_graph_result_member_value(graph_result, 4);
  ASSERT_TRUE(dse_graph_result_is_double(value));
  ASSERT_EQ(1.2, dse_graph_result_get_double(value));

  ASSERT_EQ("string", String(dse_graph_result_member_key(graph_result, 5, NULL)));
  value = dse_graph_result_member_value(graph_result, 5);
  ASSERT_TRUE(dse_graph_result_is_string(value));
  ASSERT_EQ("abc", String(dse_graph_result_get_string(value, NULL)));
}

TEST_F(GraphObjectUnitTest, Point) {
  ASSERT_EQ(CASS_OK, dse_graph_object_add_point(graph_object, "point", 1.0, 2.0));
  dse_graph_object_finish(graph_object);

  const DseGraphResult* graph_result = to_graph_result();
  ASSERT_TRUE(graph_result != NULL);
  ASSERT_TRUE(dse_graph_result_is_object(graph_result));
  ASSERT_EQ(1u, dse_graph_result_member_count(graph_result));

  const DseGraphResult* value;
  cass_double_t x, y;
  ASSERT_EQ("point", String(dse_graph_result_member_key(graph_result, 0, NULL)));
  value = dse_graph_result_member_value(graph_result, 0);
  ASSERT_EQ(CASS_OK, dse_graph_result_as_point(value, &x, &y));
  ASSERT_EQ(1.0, x); ASSERT_EQ(2.0, y);
}

TEST_F(GraphObjectUnitTest, LineString) {
  line_string = dse_line_string_new();
  ASSERT_EQ(CASS_OK, dse_line_string_add_point(line_string, 1.0, 2.0));
  ASSERT_EQ(CASS_OK, dse_line_string_add_point(line_string, 3.0, 4.0));
  ASSERT_EQ(CASS_OK, dse_line_string_add_point(line_string, 5.0, 6.0));
  ASSERT_EQ(CASS_OK, dse_line_string_finish(line_string));

  ASSERT_EQ(CASS_OK, dse_graph_object_add_line_string(graph_object, "line_string", line_string));
  dse_graph_object_finish(graph_object);

  const DseGraphResult* graph_result = to_graph_result();
  ASSERT_TRUE(graph_result != NULL);
  ASSERT_TRUE(dse_graph_result_is_object(graph_result));
  ASSERT_EQ(1u, dse_graph_result_member_count(graph_result));

  const DseGraphResult* value;
  cass_double_t x, y;
  ASSERT_EQ("line_string", String(dse_graph_result_member_key(graph_result, 0, NULL)));
  value = dse_graph_result_member_value(graph_result, 0);

  line_string_iterator = dse_line_string_iterator_new();
  ASSERT_EQ(CASS_OK, dse_graph_result_as_line_string(value, line_string_iterator));
  ASSERT_EQ(3u, dse_line_string_iterator_num_points(line_string_iterator));

  ASSERT_EQ(CASS_OK, dse_line_string_iterator_next_point(line_string_iterator, &x, &y));
  ASSERT_EQ(1.0, x); ASSERT_EQ(2.0, y);

  ASSERT_EQ(CASS_OK, dse_line_string_iterator_next_point(line_string_iterator, &x, &y));
  ASSERT_EQ(3.0, x); ASSERT_EQ(4.0, y);

  ASSERT_EQ(CASS_OK, dse_line_string_iterator_next_point(line_string_iterator, &x, &y));
  ASSERT_EQ(5.0, x); ASSERT_EQ(6.0, y);
}

TEST_F(GraphObjectUnitTest, Polygon) {
  polygon = dse_polygon_new();

  ASSERT_EQ(CASS_OK, dse_polygon_start_ring(polygon));
  ASSERT_EQ(CASS_OK, dse_polygon_add_point(polygon, 1.0, 2.0));
  ASSERT_EQ(CASS_OK, dse_polygon_add_point(polygon, 3.0, 4.0));
  ASSERT_EQ(CASS_OK, dse_polygon_add_point(polygon, 5.0, 6.0));

  ASSERT_EQ(CASS_OK, dse_polygon_start_ring(polygon));
  ASSERT_EQ(CASS_OK, dse_polygon_add_point(polygon, 7.0, 8.0));
  ASSERT_EQ(CASS_OK, dse_polygon_add_point(polygon, 9.0, 10.0));
  ASSERT_EQ(CASS_OK, dse_polygon_add_point(polygon, 11.0, 12.0));

  ASSERT_EQ(CASS_OK, dse_polygon_finish(polygon));

  ASSERT_EQ(CASS_OK, dse_graph_object_add_polygon(graph_object, "polygon", polygon));
  dse_graph_object_finish(graph_object);

  const DseGraphResult* graph_result = to_graph_result();
  ASSERT_TRUE(graph_result != NULL);
  ASSERT_TRUE(dse_graph_result_is_object(graph_result));
  ASSERT_EQ(1u, dse_graph_result_member_count(graph_result));

  const DseGraphResult* value;
  cass_double_t x, y;
  ASSERT_EQ("polygon", String(dse_graph_result_member_key(graph_result, 0, NULL)));
  value = dse_graph_result_member_value(graph_result, 0);

  polygon_iterator = dse_polygon_iterator_new();
  ASSERT_EQ(CASS_OK, dse_graph_result_as_polygon(value, polygon_iterator));

  cass_uint32_t num_points;

  // First ring
  ASSERT_EQ(CASS_OK, dse_polygon_iterator_next_num_points(polygon_iterator, &num_points));
  ASSERT_EQ(3u, num_points);

  ASSERT_EQ(CASS_OK, dse_polygon_iterator_next_point(polygon_iterator, &x, &y));
  ASSERT_EQ(1.0, x); ASSERT_EQ(2.0, y);

  ASSERT_EQ(CASS_OK, dse_polygon_iterator_next_point(polygon_iterator, &x, &y));
  ASSERT_EQ(3.0, x); ASSERT_EQ(4.0, y);

  ASSERT_EQ(CASS_OK, dse_polygon_iterator_next_point(polygon_iterator, &x, &y));
  ASSERT_EQ(5.0, x); ASSERT_EQ(6.0, y);

  // Second ring
  ASSERT_EQ(CASS_OK, dse_polygon_iterator_next_num_points(polygon_iterator, &num_points));
  ASSERT_EQ(3u, num_points);

  ASSERT_EQ(CASS_OK, dse_polygon_iterator_next_point(polygon_iterator, &x, &y));
  ASSERT_EQ(7.0, x); ASSERT_EQ(8.0, y);

  ASSERT_EQ(CASS_OK, dse_polygon_iterator_next_point(polygon_iterator, &x, &y));
  ASSERT_EQ(9.0, x); ASSERT_EQ(10.0, y);

  ASSERT_EQ(CASS_OK, dse_polygon_iterator_next_point(polygon_iterator, &x, &y));
  ASSERT_EQ(11.0, x); ASSERT_EQ(12.0, y);
}
