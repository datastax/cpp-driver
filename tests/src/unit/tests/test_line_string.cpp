/*
  Copyright (c) 2017 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include <gtest/gtest.h>

#include "dse.h"
#include "decoder.hpp"
#include "line_string.hpp"

#include <value.hpp>
#include <data_type.hpp>

#define RESET_ITERATOR_WITH(x) iterator.reset_text((x), strlen((x)))
 
class LineStringUnitTest : public testing::Test {
public:
  void SetUp() {
    line_string = dse_line_string_new();
  }

  void TearDown() {
    dse_line_string_free(line_string);
  }

  const CassValue* to_value() {
    char* data = const_cast<char*>(reinterpret_cast<const char*>(line_string->bytes().data()));
    value =  cass::Value(cass::DataType::ConstPtr(new cass::CustomType(DSE_LINE_STRING_TYPE)),
                         cass::Decoder(data, line_string->bytes().size(), 0)); // Protocol version not used
    return CassValue::to(&value);
  }

  DseLineString* line_string;
  cass::Value value;
  dse::LineStringIterator iterator;
};

TEST_F(LineStringUnitTest, BinaryEmpty) {
  ASSERT_EQ(CASS_OK, dse_line_string_finish(line_string));

  ASSERT_EQ(CASS_OK, iterator.reset_binary(to_value()));
}

TEST_F(LineStringUnitTest, BinarySingle) {
  ASSERT_EQ(CASS_OK, dse_line_string_add_point(line_string, 0.0, 1.0));
  ASSERT_EQ(CASS_ERROR_LIB_INVALID_STATE, dse_line_string_finish(line_string));
}

TEST_F(LineStringUnitTest, BinaryMultiple) {
  ASSERT_EQ(CASS_OK, dse_line_string_add_point(line_string, 0.0, 1.0));
  ASSERT_EQ(CASS_OK, dse_line_string_add_point(line_string, 2.0, 3.0));
  ASSERT_EQ(CASS_OK, dse_line_string_add_point(line_string, 4.0, 5.0));
  ASSERT_EQ(CASS_OK, dse_line_string_finish(line_string));

  ASSERT_EQ(CASS_OK, iterator.reset_binary(to_value()));
  ASSERT_EQ(3u, iterator.num_points());

  cass_double_t x, y;
  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(0.0, x); ASSERT_EQ(1.0, y);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(2.0, x); ASSERT_EQ(3.0, y);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(4.0, x); ASSERT_EQ(5.0, y);
}

TEST_F(LineStringUnitTest, TextMissingY) {
  ASSERT_EQ(CASS_ERROR_LIB_BAD_PARAMS, RESET_ITERATOR_WITH("LINESTRING (1)"));
}

TEST_F(LineStringUnitTest, TextBadX) {
  ASSERT_EQ(CASS_ERROR_LIB_BAD_PARAMS, RESET_ITERATOR_WITH("LINESTRING (a 1)"));
}

TEST_F(LineStringUnitTest, TextBadY) {
  ASSERT_EQ(CASS_ERROR_LIB_BAD_PARAMS, RESET_ITERATOR_WITH("LINESTRING (1 a)"));
}

TEST_F(LineStringUnitTest, TextJunkBeforeLineString) {
  ASSERT_EQ(CASS_ERROR_LIB_BAD_PARAMS, RESET_ITERATOR_WITH("bobo LINESTRING (1 2)"));
}

TEST_F(LineStringUnitTest, TextJunkAfterLineString) {
  ASSERT_EQ(CASS_OK, RESET_ITERATOR_WITH("LINESTRING (1 2) bobo"));
  ASSERT_EQ(1u, iterator.num_points());

  cass_double_t x, y;
  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(1.0, x); ASSERT_EQ(2.0, y);
}

TEST_F(LineStringUnitTest, TextJunkAfterEmptyLineString) {
  ASSERT_EQ(CASS_OK, RESET_ITERATOR_WITH("LINESTRING EMPTY bobo"));
  ASSERT_EQ(0u, iterator.num_points());
}

TEST_F(LineStringUnitTest, TextEmpty) {
  cass::String wkt = line_string->to_wkt();
  ASSERT_EQ("LINESTRING EMPTY", wkt);

  ASSERT_EQ(CASS_OK, iterator.reset_text(wkt.data(), wkt.size()));
  ASSERT_EQ(0u, iterator.num_points());
}

TEST_F(LineStringUnitTest, TextSingle) {
  ASSERT_EQ(CASS_OK, dse_line_string_add_point(line_string, 0.0, 1.0));

  cass::String wkt = line_string->to_wkt();
  ASSERT_EQ("LINESTRING (0 1)", wkt);

  ASSERT_EQ(CASS_OK, iterator.reset_text(wkt.data(), wkt.size()));
  ASSERT_EQ(1u, iterator.num_points());

  cass_double_t x, y;
  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(0.0, x); ASSERT_EQ(1.0, y);
}

TEST_F(LineStringUnitTest, TextMultiple) {
  ASSERT_EQ(CASS_OK, dse_line_string_add_point(line_string, 0.0, 1.0));
  ASSERT_EQ(CASS_OK, dse_line_string_add_point(line_string, 2.0, 3.0));
  ASSERT_EQ(CASS_OK, dse_line_string_add_point(line_string, 4.0, 5.0));

  cass::String wkt = line_string->to_wkt();
  ASSERT_EQ("LINESTRING (0 1, 2 3, 4 5)", wkt);

  ASSERT_EQ(CASS_OK, iterator.reset_text(wkt.data(), wkt.size()));
  ASSERT_EQ(3u, iterator.num_points());

  cass_double_t x, y;
  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(0.0, x); ASSERT_EQ(1.0, y);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(2.0, x); ASSERT_EQ(3.0, y);

  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(4.0, x); ASSERT_EQ(5.0, y);
}

TEST_F(LineStringUnitTest, TextPrecision) {
  ASSERT_EQ(CASS_OK, dse_line_string_add_point(line_string, 0.0001, 0.012345678901234567));

  cass::String wkt = line_string->to_wkt();
  ASSERT_EQ("LINESTRING (0.0001 0.012345678901234567)", wkt);

  ASSERT_EQ(CASS_OK, iterator.reset_text(wkt.data(), wkt.size()));
  ASSERT_EQ(1u, iterator.num_points());

  cass_double_t x, y;
  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(0.0001, x); ASSERT_EQ(0.012345678901234567, y);
}

TEST_F(LineStringUnitTest, TextLeadingSpace) {
  ASSERT_EQ(CASS_OK, RESET_ITERATOR_WITH("  LINESTRING (1 3)"));
  ASSERT_EQ(1u, iterator.num_points());

  cass_double_t x, y;
  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(1.0, x); ASSERT_EQ(3.0, y);
}

TEST_F(LineStringUnitTest, TextTrailingSpace) {
  ASSERT_EQ(CASS_OK, RESET_ITERATOR_WITH("LINESTRING (1 3)  "));
  ASSERT_EQ(1u, iterator.num_points());

  cass_double_t x, y;
  ASSERT_EQ(CASS_OK, iterator.next_point(&x, &y));
  ASSERT_EQ(1.0, x); ASSERT_EQ(3.0, y);
}
