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

TEST(UtilsUnitTest, CqlId)
{
  std::string s;

  // valid id
  s = "abc";
  EXPECT_EQ(cass::to_cql_id(s), std::string("abc"));

  // test lower cassing
  s = "ABC";
  EXPECT_EQ(cass::to_cql_id(s), std::string("abc"));

  // quoted
  s = "\"aBc\"";
  EXPECT_EQ(cass::to_cql_id(s), std::string("aBc"));

  // invalid chars
  s = "!@#";
  EXPECT_EQ(cass::to_cql_id(s), std::string("!@#"));
}

TEST(UtilsUnitTest, EscapeId)
{
  std::string s;

  s = "abc";
  EXPECT_EQ(cass::escape_id(s), std::string("abc"));

  s = "aBc";
  EXPECT_EQ(cass::escape_id(s), std::string("\"aBc\""));

  s = "\"";
  EXPECT_EQ(cass::escape_id(s), std::string("\"\"\"\""));

  s = "a\"Bc";
  EXPECT_EQ(cass::escape_id(s), std::string("\"a\"\"Bc\""));
}

TEST(UtilsUnitTest, NumLeadingZeros)
{
  EXPECT_EQ(64u, cass::num_leading_zeros(0));
  EXPECT_EQ(0u, cass::num_leading_zeros(1LL << 63));
  EXPECT_EQ(0u, cass::num_leading_zeros(1LL << 63 | 1 << 5));
}

TEST(UtilsUnitTest, NextPow2)
{
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
