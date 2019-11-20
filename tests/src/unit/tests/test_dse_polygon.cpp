/*
  Copyright (c) DataStax, Inc.

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

#include <gtest/gtest.h>

#include "decoder.hpp"
#include "dse.h"
#include "dse_polygon.hpp"
#include "string.hpp"

#include <value.hpp>

#define RESET_ITERATOR_WITH(x) iterator.reset_text((x), strlen((x)))

using namespace datastax;
using namespace datastax::internal::core;
using namespace datastax::internal::enterprise;

class PolygonUnitTest : public testing::Test {
public:
  void SetUp() { polygon = dse_polygon_new(); }

  void TearDown() { dse_polygon_free(polygon); }

  const CassValue* to_value() {
    char* data = const_cast<char*>(reinterpret_cast<const char*>(polygon->bytes().data()));
    value = Value(DataType::ConstPtr(new CustomType(DSE_POLYGON_TYPE)),
                  Decoder(data, polygon->bytes().size(), 0)); // Protocol version not used
    return CassValue::to(&value);
  }

  DsePolygon* polygon;
  Value value;
  PolygonIterator iterator;
};

TEST_F(PolygonUnitTest, BinaryEmptyRing) {
  ASSERT_EQ(CASS_OK, dse_polygon_start_ring(polygon));
  ASSERT_EQ(CASS_OK, dse_polygon_finish(polygon));

  ASSERT_EQ(CASS_OK, iterator.reset_binary(to_value()));
  ASSERT_EQ(1u, iterator.num_rings());
}

TEST_F(PolygonUnitTest, BinarySingleRing) {
  ASSERT_EQ(CASS_OK, dse_polygon_start_ring(polygon));
  ASSERT_EQ(CASS_OK, dse_polygon_add_point(polygon, 0, 1));
  ASSERT_EQ(CASS_OK, dse_polygon_add_point(polygon, 2, 3));
  ASSERT_EQ(CASS_OK, dse_polygon_add_point(polygon, 4, 5));
  ASSERT_EQ(CASS_OK, dse_polygon_finish(polygon));

  ASSERT_EQ(CASS_OK, iterator.reset_binary(to_value()));
  ASSERT_EQ(1u, iterator.num_rings());

  cass_uint32_t num_points;
  ASSERT_EQ(CASS_OK, iterator.next_num_points(&num_points));
  ASSERT_EQ(3u, num_points);

  cass_double_t x, y;
  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(0.0, x);
  ASSERT_EQ(1.0, y);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(2.0, x);
  ASSERT_EQ(3.0, y);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(4.0, x);
  ASSERT_EQ(5.0, y);
}

TEST_F(PolygonUnitTest, BinaryMultipleRings) {
  ASSERT_EQ(CASS_OK, dse_polygon_start_ring(polygon));
  ASSERT_EQ(CASS_OK, dse_polygon_add_point(polygon, 0, 1));
  ASSERT_EQ(CASS_OK, dse_polygon_add_point(polygon, 2, 3));
  ASSERT_EQ(CASS_OK, dse_polygon_add_point(polygon, 4, 5));

  ASSERT_EQ(CASS_OK, dse_polygon_start_ring(polygon));
  ASSERT_EQ(CASS_OK, dse_polygon_add_point(polygon, 6, 7));
  ASSERT_EQ(CASS_OK, dse_polygon_add_point(polygon, 8, 9));
  ASSERT_EQ(CASS_OK, dse_polygon_add_point(polygon, 10, 11));
  ASSERT_EQ(CASS_OK, dse_polygon_add_point(polygon, 12, 13));

  ASSERT_EQ(CASS_OK, dse_polygon_finish(polygon));

  ASSERT_EQ(CASS_OK, iterator.reset_binary(to_value()));
  ASSERT_EQ(2u, iterator.num_rings());

  // First ring
  cass_uint32_t num_points;
  ASSERT_EQ(CASS_OK, iterator.next_num_points(&num_points));
  ASSERT_EQ(3u, num_points);

  cass_double_t x, y;
  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(0.0, x);
  ASSERT_EQ(1.0, y);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(2.0, x);
  ASSERT_EQ(3.0, y);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(4.0, x);
  ASSERT_EQ(5.0, y);

  // Second ring
  ASSERT_EQ(CASS_OK, iterator.next_num_points(&num_points));
  ASSERT_EQ(4u, num_points);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(6.0, x);
  ASSERT_EQ(7.0, y);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(8.0, x);
  ASSERT_EQ(9.0, y);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(10.0, x);
  ASSERT_EQ(11.0, y);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(12.0, x);
  ASSERT_EQ(13.0, y);
}

TEST_F(PolygonUnitTest, TextMissingY) {
  ASSERT_EQ(CASS_ERROR_LIB_BAD_PARAMS, RESET_ITERATOR_WITH("POLYGON ((1))"));
}

TEST_F(PolygonUnitTest, TextBadX) {
  ASSERT_EQ(CASS_ERROR_LIB_BAD_PARAMS, RESET_ITERATOR_WITH("POLYGON ((a 1))"));
}

TEST_F(PolygonUnitTest, TextBadY) {
  ASSERT_EQ(CASS_ERROR_LIB_BAD_PARAMS, RESET_ITERATOR_WITH("POLYGON ((1 a))"));
}

TEST_F(PolygonUnitTest, TextJunkBeforePolygon) {
  ASSERT_EQ(CASS_ERROR_LIB_BAD_PARAMS, RESET_ITERATOR_WITH("bobo POLYGON ((1 2))"));
}

TEST_F(PolygonUnitTest, TextJunkAfterPolygon) {
  ASSERT_EQ(CASS_OK, RESET_ITERATOR_WITH("POLYGON ((1 2)) bobo"));
  ASSERT_EQ(1u, iterator.num_rings());

  cass_uint32_t num_points;
  ASSERT_EQ(CASS_OK, iterator.next_num_points(&num_points));
  ASSERT_EQ(1u, num_points);

  cass_double_t x, y;
  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(1.0, x);
  ASSERT_EQ(2.0, y);
}

TEST_F(PolygonUnitTest, TextJunkAfterEmptyPolygon) {
  ASSERT_EQ(CASS_OK, RESET_ITERATOR_WITH("POLYGON EMPTY bobo"));
  ASSERT_EQ(0u, iterator.num_rings());
}

TEST_F(PolygonUnitTest, TextEmpty) {
  String wkt = polygon->to_wkt();
  ASSERT_EQ("POLYGON EMPTY", wkt);

  ASSERT_EQ(CASS_OK, iterator.reset_text(wkt.data(), wkt.size()));
  ASSERT_EQ(0u, iterator.num_rings());
}

TEST_F(PolygonUnitTest, TextEmptyRing) {
  ASSERT_EQ(CASS_OK, dse_polygon_start_ring(polygon));
  ASSERT_EQ(CASS_OK, dse_polygon_finish(polygon));

  String wkt = polygon->to_wkt();
  ASSERT_EQ("POLYGON (())", wkt);

  ASSERT_EQ(CASS_OK, iterator.reset_text(wkt.data(), wkt.size()));
  ASSERT_EQ(1u, iterator.num_rings());
}

TEST_F(PolygonUnitTest, TextSingleRing) {
  ASSERT_EQ(CASS_OK, dse_polygon_start_ring(polygon));
  ASSERT_EQ(CASS_OK, dse_polygon_add_point(polygon, 0, 1));
  ASSERT_EQ(CASS_OK, dse_polygon_add_point(polygon, 2, 3));
  ASSERT_EQ(CASS_OK, dse_polygon_add_point(polygon, 4, 5));
  ASSERT_EQ(CASS_OK, dse_polygon_finish(polygon));

  String wkt = polygon->to_wkt();
  ASSERT_EQ("POLYGON ((0 1, 2 3, 4 5))", wkt);

  ASSERT_EQ(CASS_OK, iterator.reset_text(wkt.data(), wkt.size()));
  ASSERT_EQ(1u, iterator.num_rings());

  cass_uint32_t num_points;
  ASSERT_EQ(CASS_OK, iterator.next_num_points(&num_points));
  ASSERT_EQ(3u, num_points);

  cass_double_t x, y;
  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(0.0, x);
  ASSERT_EQ(1.0, y);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(2.0, x);
  ASSERT_EQ(3.0, y);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(4.0, x);
  ASSERT_EQ(5.0, y);
}

TEST_F(PolygonUnitTest, TextMultipleRings) {
  ASSERT_EQ(CASS_OK, dse_polygon_start_ring(polygon));
  ASSERT_EQ(CASS_OK, dse_polygon_add_point(polygon, 0, 1));
  ASSERT_EQ(CASS_OK, dse_polygon_add_point(polygon, 2, 3));
  ASSERT_EQ(CASS_OK, dse_polygon_add_point(polygon, 4, 5));

  ASSERT_EQ(CASS_OK, dse_polygon_start_ring(polygon));
  ASSERT_EQ(CASS_OK, dse_polygon_add_point(polygon, 6, 7));
  ASSERT_EQ(CASS_OK, dse_polygon_add_point(polygon, 8, 9));
  ASSERT_EQ(CASS_OK, dse_polygon_add_point(polygon, 10, 11));
  ASSERT_EQ(CASS_OK, dse_polygon_add_point(polygon, 12, 13));

  ASSERT_EQ(CASS_OK, dse_polygon_finish(polygon));

  String wkt = polygon->to_wkt();
  ASSERT_EQ("POLYGON ((0 1, 2 3, 4 5), (6 7, 8 9, 10 11, 12 13))", wkt);

  ASSERT_EQ(CASS_OK, iterator.reset_text(wkt.data(), wkt.size()));
  ASSERT_EQ(2u, iterator.num_rings());

  // First ring
  cass_uint32_t num_points;
  ASSERT_EQ(CASS_OK, iterator.next_num_points(&num_points));
  ASSERT_EQ(3u, num_points);

  cass_double_t x, y;
  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(0.0, x);
  ASSERT_EQ(1.0, y);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(2.0, x);
  ASSERT_EQ(3.0, y);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(4.0, x);
  ASSERT_EQ(5.0, y);

  // Second ring
  ASSERT_EQ(CASS_OK, iterator.next_num_points(&num_points));
  ASSERT_EQ(4u, num_points);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(6.0, x);
  ASSERT_EQ(7.0, y);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(8.0, x);
  ASSERT_EQ(9.0, y);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(10.0, x);
  ASSERT_EQ(11.0, y);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(12.0, x);
  ASSERT_EQ(13.0, y);
}

TEST_F(PolygonUnitTest, TextLeadingSpace) {
  ASSERT_EQ(CASS_OK, RESET_ITERATOR_WITH("  POLYGON ((1 3))"));
  ASSERT_EQ(1u, iterator.num_rings());

  cass_uint32_t num_points;
  ASSERT_EQ(CASS_OK, iterator.next_num_points(&num_points));
  ASSERT_EQ(1u, num_points);

  cass_double_t x, y;
  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(1.0, x);
  ASSERT_EQ(3.0, y);
}

TEST_F(PolygonUnitTest, TextTrailingSpace) {
  ASSERT_EQ(CASS_OK, RESET_ITERATOR_WITH("POLYGON ((1 3))  "));
  ASSERT_EQ(1u, iterator.num_rings());

  cass_uint32_t num_points;
  ASSERT_EQ(CASS_OK, iterator.next_num_points(&num_points));
  ASSERT_EQ(1u, num_points);

  cass_double_t x, y;
  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(1.0, x);
  ASSERT_EQ(3.0, y);
}
