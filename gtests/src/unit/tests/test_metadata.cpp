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

#include "result_metadata.hpp"

cass::SharedRefPtr<cass::ResultMetadata> create_metadata(const char* column_names[]) {
  size_t count = 0;
  while (column_names[count] != NULL) { count++; }

  cass::ResultMetadata::Ptr metadata(
        cass::Memory::allocate<cass::ResultMetadata>(count,
                                                     cass::RefBuffer::Ptr()));

  for (size_t i = 0; column_names[i] != NULL; ++i) {
    cass::ColumnDefinition def;
    def.name = cass::StringRef(column_names[i]);
    def.index = i;
    metadata->add(def);
  }

  return metadata;
}

TEST(ResultMetadataUnitTest, Simple) {
  const char* column_names[] = { "abc", "def", "xyz", NULL };
  cass::SharedRefPtr<cass::ResultMetadata> metadata(create_metadata(column_names));

  for (size_t i = 0; column_names[i] != NULL; ++i) {
    cass::IndexVec indices;
    size_t count = metadata->get_indices(column_names[i], &indices);
    EXPECT_EQ(count, 1u);
    EXPECT_GT(indices.size(), 0u);
    EXPECT_EQ(indices[0], i);
  }
}

TEST(ResultMetadataUnitTest, CaseSensitive) {
  const char* column_names[] = { "a", "A", "abc", "Abc", "ABc", "ABC", "aBc", "aBC", "abC", NULL };
  cass::SharedRefPtr<cass::ResultMetadata> metadata(create_metadata(column_names));

  for (size_t i = 0; column_names[i] != NULL; ++i) {
    cass::IndexVec indices;
    cass::String name;
    name.push_back('"');
    name.append(column_names[i]);
    name.push_back('"');
    size_t count = metadata->get_indices(name.c_str(), &indices);
    EXPECT_EQ(count, 1u);
    EXPECT_GT(indices.size(), 0u);
    EXPECT_EQ(indices[0], i);
  }

  {
    cass::IndexVec indices;
    size_t count = metadata->get_indices("a", &indices);
    EXPECT_EQ(count, 2u);
  }

  {
    cass::IndexVec indices;
    size_t count = metadata->get_indices("abc", &indices);
    EXPECT_EQ(count, 7u);
  }
}
