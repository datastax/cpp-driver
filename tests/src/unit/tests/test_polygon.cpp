/*
  Copyright (c) 2016 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include <gtest/gtest.h>

#include "dse.h"
#include "polygon.hpp"

#include <value.hpp>

class PolygonUnitTest : public testing::Test {
public:
  void SetUp() {
    polygon = dse_polygon_new();
  }

  void TearDown() {
    dse_polygon_free(polygon);
  }

  const CassValue* to_value() {
    char* data = const_cast<char*>(reinterpret_cast<const char*>(polygon->bytes().data()));
    value =  cass::Value(0, // Not used
                         cass::DataType::ConstPtr(new cass::CustomType(DSE_POLYGON_TYPE)),
                         data, polygon->bytes().size());
    return CassValue::to(&value);
  }

  DsePolygon* polygon;
  cass::Value value;
};

TEST_F(PolygonUnitTest, BinaryEmptyRing) {
  ASSERT_EQ(CASS_OK, dse_polygon_start_ring(polygon));
  ASSERT_EQ(CASS_OK, dse_polygon_finish(polygon));

  dse::PolygonIterator iterator;
  ASSERT_EQ(CASS_OK, iterator.reset_binary(to_value()));
  ASSERT_EQ(1u, iterator.num_rings());
}

TEST_F(PolygonUnitTest, BinarySingleRing) {
  ASSERT_EQ(CASS_OK, dse_polygon_start_ring(polygon));
  ASSERT_EQ(CASS_OK, dse_polygon_add_point(polygon, 0, 1));
  ASSERT_EQ(CASS_OK, dse_polygon_add_point(polygon, 2, 3));
  ASSERT_EQ(CASS_OK, dse_polygon_add_point(polygon, 4, 5));
  ASSERT_EQ(CASS_OK, dse_polygon_finish(polygon));

  dse::PolygonIterator iterator;
  ASSERT_EQ(CASS_OK, iterator.reset_binary(to_value()));
  ASSERT_EQ(1u, iterator.num_rings());

  cass_uint32_t num_points;
  ASSERT_EQ(CASS_OK, iterator.next_num_points(&num_points));
  ASSERT_EQ(3u, num_points);

  cass_double_t x, y;
  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(0.0, x); ASSERT_EQ(1.0, y);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(2.0, x); ASSERT_EQ(3.0, y);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(4.0, x); ASSERT_EQ(5.0, y);
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

  dse::PolygonIterator iterator;
  ASSERT_EQ(CASS_OK, iterator.reset_binary(to_value()));
  ASSERT_EQ(2u, iterator.num_rings());

  // First ring
  cass_uint32_t num_points;
  ASSERT_EQ(CASS_OK, iterator.next_num_points(&num_points));
  ASSERT_EQ(3u, num_points);

  cass_double_t x, y;
  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(0.0, x); ASSERT_EQ(1.0, y);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(2.0, x); ASSERT_EQ(3.0, y);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(4.0, x); ASSERT_EQ(5.0, y);

  // Second ring
  ASSERT_EQ(CASS_OK, iterator.next_num_points(&num_points));
  ASSERT_EQ(4u, num_points);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(6.0, x); ASSERT_EQ(7.0, y);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(8.0, x); ASSERT_EQ(9.0, y);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(10.0, x); ASSERT_EQ(11.0, y);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(12.0, x); ASSERT_EQ(13.0, y);
}

TEST_F(PolygonUnitTest, TextEmpty) {
  std::string wkt = polygon->to_wkt();
  ASSERT_EQ("POLYGON ()", wkt);

  dse::PolygonIterator iterator;
  ASSERT_EQ(CASS_OK, iterator.reset_text(wkt.data(), wkt.size()));
  ASSERT_EQ(0u, iterator.num_rings());
}

TEST_F(PolygonUnitTest, TextEmptyRing) {
  ASSERT_EQ(CASS_OK, dse_polygon_start_ring(polygon));
  ASSERT_EQ(CASS_OK, dse_polygon_finish(polygon));

  std::string wkt = polygon->to_wkt();
  ASSERT_EQ("POLYGON (())", wkt);

  dse::PolygonIterator iterator;
  ASSERT_EQ(CASS_OK, iterator.reset_text(wkt.data(), wkt.size()));
  ASSERT_EQ(1u, iterator.num_rings());
}

TEST_F(PolygonUnitTest, TextSingleRing) {
  ASSERT_EQ(CASS_OK, dse_polygon_start_ring(polygon));
  ASSERT_EQ(CASS_OK, dse_polygon_add_point(polygon, 0, 1));
  ASSERT_EQ(CASS_OK, dse_polygon_add_point(polygon, 2, 3));
  ASSERT_EQ(CASS_OK, dse_polygon_add_point(polygon, 4, 5));
  ASSERT_EQ(CASS_OK, dse_polygon_finish(polygon));

  std::string wkt = polygon->to_wkt();
  ASSERT_EQ("POLYGON ((0 1, 2 3, 4 5))", wkt);

  dse::PolygonIterator iterator;
  ASSERT_EQ(CASS_OK, iterator.reset_text(wkt.data(), wkt.size()));
  ASSERT_EQ(1u, iterator.num_rings());

  cass_uint32_t num_points;
  ASSERT_EQ(CASS_OK, iterator.next_num_points(&num_points));
  ASSERT_EQ(3u, num_points);

  cass_double_t x, y;
  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(0.0, x); ASSERT_EQ(1.0, y);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(2.0, x); ASSERT_EQ(3.0, y);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(4.0, x); ASSERT_EQ(5.0, y);
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

  std::string wkt = polygon->to_wkt();
  ASSERT_EQ("POLYGON ((0 1, 2 3, 4 5), (6 7, 8 9, 10 11, 12 13))", wkt);

  dse::PolygonIterator iterator;
  ASSERT_EQ(CASS_OK, iterator.reset_text(wkt.data(), wkt.size()));
  ASSERT_EQ(2u, iterator.num_rings());

  // First ring
  cass_uint32_t num_points;
  ASSERT_EQ(CASS_OK, iterator.next_num_points(&num_points));
  ASSERT_EQ(3u, num_points);

  cass_double_t x, y;
  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(0.0, x); ASSERT_EQ(1.0, y);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(2.0, x); ASSERT_EQ(3.0, y);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(4.0, x); ASSERT_EQ(5.0, y);

  // Second ring
  ASSERT_EQ(CASS_OK, iterator.next_num_points(&num_points));
  ASSERT_EQ(4u, num_points);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(6.0, x); ASSERT_EQ(7.0, y);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(8.0, x); ASSERT_EQ(9.0, y);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(10.0, x); ASSERT_EQ(11.0, y);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(12.0, x); ASSERT_EQ(13.0, y);
}
