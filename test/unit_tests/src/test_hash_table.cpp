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

#include "hash_table.hpp"

#include <boost/test/unit_test.hpp>

#include <string.h>

struct TestEntry : cass::HashTableEntry<TestEntry> {
  TestEntry(const std::string& name)
    : name(name) { }
  std::string name;
};

BOOST_AUTO_TEST_SUITE(hash_table)

BOOST_AUTO_TEST_CASE(simple)
{
  cass::CaseInsensitiveHashTable<TestEntry> ht(4);
  ht.add(TestEntry("abc"));
  ht.add(TestEntry("def"));
  ht.add(TestEntry("123"));
  ht.add(TestEntry("456"));

  cass::IndexVec indices;

  BOOST_CHECK(ht.get_indices("abc", &indices) > 0);
  BOOST_CHECK(indices[0] == 0);

  BOOST_CHECK(ht.get_indices("def", &indices) > 0);
  BOOST_CHECK(indices[0] == 1);

  BOOST_CHECK(ht.get_indices("123", &indices) > 0);
  BOOST_CHECK(indices[0] == 2);

  BOOST_CHECK(ht.get_indices("456", &indices) > 0);
  BOOST_CHECK(indices[0] == 3);

  BOOST_CHECK(ht.get_indices("does_not_exist", &indices) == 0);
}

BOOST_AUTO_TEST_CASE(case_sensitivity)
{
  cass::CaseInsensitiveHashTable<TestEntry> ht(4);
  ht.add(TestEntry("abc"));
  ht.add(TestEntry("def"));
  ht.add(TestEntry("DEF"));

  cass::IndexVec indices;

  BOOST_CHECK(ht.get_indices("aBc", &indices) > 0);
  BOOST_CHECK(indices[0] == 0);

  BOOST_CHECK(ht.get_indices("Abc", &indices) > 0);
  BOOST_CHECK(indices[0] == 0);

  BOOST_CHECK(ht.get_indices("ABC", &indices) > 0);
  BOOST_CHECK(indices[0] == 0);

  BOOST_CHECK(ht.get_indices("def", &indices) == 2);
  BOOST_CHECK(indices[0] == 1);
  BOOST_CHECK(indices[1] == 2);

  BOOST_CHECK(ht.get_indices("\"def\"", &indices) == 1);
  BOOST_CHECK(indices[0] == 1);

  BOOST_CHECK(ht.get_indices("\"DEF\"", &indices) == 1);
  BOOST_CHECK(indices[0] == 2);
}

BOOST_AUTO_TEST_CASE(resize)
{
  cass::CaseInsensitiveHashTable<TestEntry> ht(0);

  for (int i = 0; i < 26; ++i) {
    std::string s;
    s.push_back('a' + i);
    ht.add(TestEntry(s));
  }

  cass::IndexVec indices;

  BOOST_CHECK(ht.get_indices("a", &indices) > 0);
  BOOST_CHECK(indices[0] == 0);

  BOOST_CHECK(ht.get_indices("d", &indices) > 0);
  BOOST_CHECK(indices[0] == 3);

  BOOST_CHECK(ht.get_indices("z", &indices) > 0);
  BOOST_CHECK(indices[0] == 25);
}

BOOST_AUTO_TEST_SUITE_END()


