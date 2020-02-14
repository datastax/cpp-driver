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

#include "macros.hpp"
#include "utils.hpp"

#include <ctype.h>
#include <stdio.h>

using datastax::String;
using datastax::internal::escape_id;
using datastax::internal::num_leading_zeros;

TEST(UtilsUnitTest, EscapeId) {
  String s;

  s = "abc";
  EXPECT_EQ(escape_id(s), String("abc"));

  s = "aBc";
  EXPECT_EQ(escape_id(s), String("\"aBc\""));

  s = "\"";
  EXPECT_EQ(escape_id(s), String("\"\"\"\""));

  s = "a\"Bc";
  EXPECT_EQ(escape_id(s), String("\"a\"\"Bc\""));
}

TEST(UtilsUnitTest, NumLeadingZeros) {
  EXPECT_EQ(64u, num_leading_zeros(0));
  EXPECT_EQ(0u, num_leading_zeros(1LL << 63));
  EXPECT_EQ(0u, num_leading_zeros(1LL << 63 | 1 << 5));
}

TEST(UtilsUnitTest, NextPow2) {
  EXPECT_EQ(STATIC_NEXT_POW_2(1u), 2u);
  EXPECT_EQ(STATIC_NEXT_POW_2(1u), 2u);
  EXPECT_EQ(STATIC_NEXT_POW_2(2u), 2u);

  EXPECT_EQ(STATIC_NEXT_POW_2(3u), 4u);
  EXPECT_EQ(STATIC_NEXT_POW_2(4u), 4u);

  EXPECT_EQ(STATIC_NEXT_POW_2(7u), 8u);
  EXPECT_EQ(STATIC_NEXT_POW_2(8u), 8u);

  EXPECT_EQ(STATIC_NEXT_POW_2(9u), 16u);
  EXPECT_EQ(STATIC_NEXT_POW_2(15u), 16u);
  EXPECT_EQ(STATIC_NEXT_POW_2(16u), 16u);

  EXPECT_EQ(STATIC_NEXT_POW_2(17u), 32u);
  EXPECT_EQ(STATIC_NEXT_POW_2(31u), 32u);
  EXPECT_EQ(STATIC_NEXT_POW_2(32u), 32u);
}

TEST(UtilsUnitTest, Trim) {
  String s;

  s = " abc";
  EXPECT_EQ(trim(s), String("abc"));

  s = "abc ";
  EXPECT_EQ(trim(s), String("abc"));

  s = "  abc  ";
  EXPECT_EQ(trim(s), String("abc"));

  s = "  a bc ";
  EXPECT_EQ(trim(s), String("a bc"));
}
