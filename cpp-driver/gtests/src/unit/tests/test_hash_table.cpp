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

#include "hash_table.hpp"

using namespace datastax;
using namespace datastax::internal::core;

struct TestEntry : HashTableEntry<TestEntry> {
  TestEntry(const String& name)
      : name(name) {}
  String name;
};

TEST(HashTableUnitTests, Simple) {
  CaseInsensitiveHashTable<TestEntry> ht(4);
  ht.add(TestEntry("abc"));
  ht.add(TestEntry("def"));
  ht.add(TestEntry("123"));
  ht.add(TestEntry("456"));

  IndexVec indices;

  EXPECT_GT(ht.get_indices("abc", &indices), 0u);
  EXPECT_EQ(indices[0], 0u);

  EXPECT_GT(ht.get_indices("def", &indices), 0u);
  EXPECT_EQ(indices[0], 1u);

  EXPECT_GT(ht.get_indices("123", &indices), 0u);
  EXPECT_EQ(indices[0], 2u);

  EXPECT_GT(ht.get_indices("456", &indices), 0u);
  EXPECT_EQ(indices[0], 3u);

  EXPECT_EQ(ht.get_indices("does_not_exist", &indices), 0u);
}

TEST(HashTableUnitTests, CaseSensitivity) {
  CaseInsensitiveHashTable<TestEntry> ht(4);
  ht.add(TestEntry("abc"));
  ht.add(TestEntry("def"));
  ht.add(TestEntry("DEF"));

  IndexVec indices;

  EXPECT_GT(ht.get_indices("aBc", &indices), 0u);
  EXPECT_EQ(indices[0], 0u);

  EXPECT_GT(ht.get_indices("Abc", &indices), 0u);
  EXPECT_EQ(indices[0], 0u);

  EXPECT_GT(ht.get_indices("ABC", &indices), 0u);
  EXPECT_EQ(indices[0], 0u);

  EXPECT_EQ(ht.get_indices("def", &indices), 2u);
  EXPECT_EQ(indices[0], 1u);
  EXPECT_EQ(indices[1], 2u);

  EXPECT_EQ(ht.get_indices("\"def\"", &indices), 1u);
  EXPECT_EQ(indices[0], 1u);

  EXPECT_EQ(ht.get_indices("\"DEF\"", &indices), 1u);
  EXPECT_EQ(indices[0], 2u);
}

TEST(HashTableUnitTests, Resize) {
  CaseInsensitiveHashTable<TestEntry> ht(0);

  for (int i = 0; i < 26; ++i) {
    String s;
    s.push_back('a' + i);
    ht.add(TestEntry(s));
  }

  IndexVec indices;

  EXPECT_GT(ht.get_indices("a", &indices), 0u);
  EXPECT_EQ(indices[0], 0u);

  EXPECT_GT(ht.get_indices("d", &indices), 0u);
  EXPECT_EQ(indices[0], 3u);

  EXPECT_GT(ht.get_indices("z", &indices), 0u);
  EXPECT_EQ(indices[0], 25u);
}
