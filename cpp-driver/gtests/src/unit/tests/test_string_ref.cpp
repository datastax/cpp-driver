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

#include "string_ref.hpp"

using datastax::StringRef;

TEST(StringRefUnitTest, Compare) {
  const char* value = "abc";
  StringRef s(value);

  // Equals
  EXPECT_EQ(s.compare(s), 0);
  EXPECT_EQ(s, s);
  EXPECT_EQ(s, value);

  // Not equals
  EXPECT_NE(s, "xyz");
  EXPECT_NE(s, StringRef("xyz"));

  // Case insensitive
  EXPECT_TRUE(s.iequals("ABC"));
  EXPECT_TRUE(iequals(s, "ABC"));
}

TEST(StringRefUnitTest, Empty) {
  StringRef s;

  EXPECT_TRUE(s.empty());
  EXPECT_EQ(s, "");
  EXPECT_NE(s, "abc");

  EXPECT_TRUE(starts_with(s, ""));
  EXPECT_TRUE(ends_with(s, ""));

  EXPECT_FALSE(starts_with(s, "abc"));
  EXPECT_FALSE(ends_with(s, "abc"));
}

TEST(StringRefUnitTest, Substr) {
  StringRef s("abcxyz");

  // Full string
  EXPECT_EQ(s.substr(0, s.length()), s);

  // Exceeds length
  EXPECT_EQ(s.substr(0, s.length() + 1), s);
  EXPECT_EQ(s.substr(0, StringRef::npos), s);

  // More tests in "starts_with" and "ends_with"
}

TEST(StringRefUnitTest, Find) {
  StringRef s("abcxyz");

  EXPECT_EQ(s.find(""), 0u);
  EXPECT_EQ(s.find("abc"), 0u);
  EXPECT_EQ(s.find("xyz"), 3u);
  EXPECT_EQ(s.find("z"), 5u);

  EXPECT_EQ(s.find("invalid"), StringRef::npos);
  EXPECT_EQ(s.find("abcxyza"), StringRef::npos);

  EXPECT_EQ(s.find(""), 0u);
  EXPECT_EQ(StringRef("").find(""), 0u);
}

TEST(StringRefUnitTest, StartsWith) {
  StringRef s("abcxyz");

  // Various lengths
  for (size_t i = 0; i < s.length(); ++i) {
    EXPECT_TRUE(starts_with(s, s.substr(0, i)));
  }

  // Does not start with
  EXPECT_FALSE(starts_with(s, "xyz"));

  // Too long
  EXPECT_FALSE(starts_with(s, "abcxyzabcxyz"));
}

TEST(StringRefUnitTest, EndsWith) {
  StringRef s("abcxyz");

  // Various lengths
  for (size_t i = 0; i < s.length(); ++i) {
    EXPECT_TRUE(ends_with(s, s.substr(i, StringRef::npos)));
  }

  // Does not end with
  EXPECT_FALSE(ends_with(s, "abc"));

  // Too long
  EXPECT_FALSE(ends_with(s, "abcxyzabcxyz"));
}
