/*
  Copyright (c) 2014-2016 DataStax

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

#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include "result_metadata.hpp"

#include <boost/test/unit_test.hpp>

cass::SharedRefPtr<cass::ResultMetadata> create_metadata(const char* column_names[]) {
  size_t count = 0;
  while (column_names[count] != NULL) { count++; }

  cass::SharedRefPtr<cass::ResultMetadata> metadata(new cass::ResultMetadata(count));

  for (size_t i = 0; column_names[i] != NULL; ++i) {
    cass::ColumnDefinition def;
    def.name = cass::StringRef(column_names[i]);
    def.index = i;
    metadata->add(def);
  }

  return metadata;
}

BOOST_AUTO_TEST_SUITE(metadata)

BOOST_AUTO_TEST_CASE(simple)
{
  const char* column_names[] = { "abc", "def", "xyz", NULL };
  cass::SharedRefPtr<cass::ResultMetadata> metadata(create_metadata(column_names));

  for (size_t i = 0; column_names[i] != NULL; ++i) {
    cass::IndexVec indices;
    size_t count = metadata->get_indices(column_names[i], &indices);
    BOOST_CHECK(count == 1);
    BOOST_CHECK(indices.size() > 0 && indices[0] == i);
  }
}

BOOST_AUTO_TEST_CASE(case_sensitive)
{
  const char* column_names[] = { "a", "A", "abc", "Abc", "ABc", "ABC", "aBc", "aBC", "abC", NULL };
  cass::SharedRefPtr<cass::ResultMetadata> metadata(create_metadata(column_names));

  for (size_t i = 0; column_names[i] != NULL; ++i) {
    cass::IndexVec indices;
    std::string name;
    name.push_back('"');
    name.append(column_names[i]);
    name.push_back('"');
    size_t count = metadata->get_indices(name.c_str(), &indices);
    BOOST_CHECK(count == 1);
    BOOST_CHECK(indices.size() > 0 && indices[0] == i);
  }

  {
    cass::IndexVec indices;
    size_t count = metadata->get_indices("a", &indices);
    BOOST_CHECK(count == 2);
  }

  {
    cass::IndexVec indices;
    size_t count = metadata->get_indices("abc", &indices);
    BOOST_CHECK(count == 7);
  }
}

BOOST_AUTO_TEST_SUITE_END()

