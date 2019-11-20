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

#include "dse.h"

#include <data_type.hpp>
#include <value.hpp>

class PointUnitTest : public testing::Test {
public:
  cass_double_t x;
  cass_double_t y;
};

TEST_F(PointUnitTest, TextEmpty) {
  ASSERT_EQ(CASS_ERROR_LIB_BAD_PARAMS, dse_point_from_wkt("POINT ()", &x, &y));
  ASSERT_EQ(CASS_ERROR_LIB_BAD_PARAMS, dse_point_from_wkt("POINT EMPTY", &x, &y));
}

TEST_F(PointUnitTest, TextMissingY) {
  ASSERT_EQ(CASS_ERROR_LIB_BAD_PARAMS, dse_point_from_wkt("POINT (1)", &x, &y));
}

TEST_F(PointUnitTest, TextBadX) {
  ASSERT_EQ(CASS_ERROR_LIB_BAD_PARAMS, dse_point_from_wkt("POINT (a 1)", &x, &y));
}

TEST_F(PointUnitTest, TextBadY) {
  ASSERT_EQ(CASS_ERROR_LIB_BAD_PARAMS, dse_point_from_wkt("POINT (1 a)", &x, &y));
}

TEST_F(PointUnitTest, TextJunkBeforePoint) {
  ASSERT_EQ(CASS_ERROR_LIB_BAD_PARAMS, dse_point_from_wkt("bobo POINT (1 2)", &x, &y));
}

TEST_F(PointUnitTest, TextJunkAfterPoint) {
  ASSERT_EQ(CASS_OK, dse_point_from_wkt("POINT (1 2) bobo", &x, &y));
  ASSERT_EQ(1, x);
  ASSERT_EQ(2, y);
}

TEST_F(PointUnitTest, TextGoodInt) {
  ASSERT_EQ(CASS_OK, dse_point_from_wkt("POINT (1 2)", &x, &y));
  ASSERT_EQ(1, x);
  ASSERT_EQ(2, y);
}

TEST_F(PointUnitTest, TextPrecision) {
  ASSERT_EQ(CASS_OK, dse_point_from_wkt("POINT (0.0001 0.012345678901234567)", &x, &y));
  ASSERT_EQ(0.0001, x);
  ASSERT_EQ(0.012345678901234567, y);
}

TEST_F(PointUnitTest, TextLeadingSpace) {
  ASSERT_EQ(CASS_OK, dse_point_from_wkt(" POINT (1 2)", &x, &y));
  ASSERT_EQ(1, x);
  ASSERT_EQ(2, y);
}

TEST_F(PointUnitTest, TextTrailingSpace) {
  ASSERT_EQ(CASS_OK, dse_point_from_wkt("POINT (1 2) ", &x, &y));
  ASSERT_EQ(1, x);
  ASSERT_EQ(2, y);
}
