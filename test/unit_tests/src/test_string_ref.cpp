/*
  Copyright (c) 2014-2015 DataStax

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

#include "string_ref.hpp"

#include <boost/test/unit_test.hpp>


BOOST_AUTO_TEST_SUITE(string_ref)

BOOST_AUTO_TEST_CASE(compare)
{
  const char* value = "abc";
  cass::StringRef s(value);

  // Equals
  BOOST_CHECK(s.compare(s) == 0);
  BOOST_CHECK(s == s);
  BOOST_CHECK(s == value);
  BOOST_CHECK(s.compare(0, strlen(value), value) == 0);

  // Not equals
  BOOST_CHECK(s != "xyz");
  BOOST_CHECK(s != cass::StringRef("xyz"));

  // Case insensitive
  BOOST_CHECK(s.iequals("ABC"));
  BOOST_CHECK(cass::iequals(s, "ABC"));

  // Zero length
  BOOST_CHECK(s.compare(0, 0, value) < 0);
  BOOST_CHECK(s.compare(strlen(value) - 1, 0, value) < 0);

  // Exceeds length
  BOOST_CHECK(s.compare(0, strlen(value) + 1, value) == 0);
  BOOST_CHECK(s.compare(0, cass::StringRef::npos, value) == 0);

  // Position
  BOOST_CHECK(s.compare(1, 2, "bc") == 0);
  BOOST_CHECK(s.compare(1, 3, "bc") == 0);
  BOOST_CHECK(s.compare(1, cass::StringRef::npos, "bc") == 0);

  BOOST_CHECK(s.compare(2, 1, "c") == 0);
  BOOST_CHECK(s.compare(2, 1, "c") == 0);
  BOOST_CHECK(s.compare(2, cass::StringRef::npos, "c") == 0);
}

BOOST_AUTO_TEST_CASE(substr)
{
  cass::StringRef s("abcxyz");

  // Full string
  BOOST_CHECK(s.substr(0, s.length()) == s);

  // Exceeds length
  BOOST_CHECK(s.substr(0, s.length() + 1) == s);
  BOOST_CHECK(s.substr(0, cass::StringRef::npos) == s);

  // More tests in "starts_with" and "ends_with"
}

BOOST_AUTO_TEST_CASE(starts_with)
{
  cass::StringRef s("abcxyz");

  // Various lengths
  for (size_t i = 0; i < s.length(); ++i) {
    BOOST_CHECK(cass::starts_with(s, s.substr(0, i)));
  }

  // Does not start with
  BOOST_CHECK(!cass::starts_with(s, "xyz"));

  // Too long
  BOOST_CHECK(!cass::starts_with(s, "abcxyzabcxyz"));
}

BOOST_AUTO_TEST_CASE(ends_with)
{
  cass::StringRef s("abcxyz");

  // Various lengths
  for (size_t i = 0; i < s.length(); ++i) {
    BOOST_CHECK(cass::ends_with(s, s.substr(i, cass::StringRef::npos)));
  }

  // Does not end with
  BOOST_CHECK(!cass::ends_with(s, "abc"));

  // Too long
  BOOST_CHECK(!cass::ends_with(s, "abcxyzabcxyz"));
}

BOOST_AUTO_TEST_SUITE_END()
