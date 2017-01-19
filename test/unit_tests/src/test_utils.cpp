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

#include "macros.hpp"
#include "utils.hpp"

#include <ctype.h>
#include <stdio.h>

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_SUITE(utils)

BOOST_AUTO_TEST_CASE(cql_id)
{
  std::string s;

  // valid id
  s = "abc";
  BOOST_CHECK_EQUAL(cass::to_cql_id(s), std::string("abc"));

  // test lower cassing
  s = "ABC";
  BOOST_CHECK_EQUAL(cass::to_cql_id(s), std::string("abc"));

  // quoted
  s = "\"aBc\"";
  BOOST_CHECK_EQUAL(cass::to_cql_id(s), std::string("aBc"));

  // invalid chars
  s = "!@#";
  BOOST_CHECK_EQUAL(cass::to_cql_id(s), std::string("!@#"));
}

BOOST_AUTO_TEST_CASE(escape_id)
{
  std::string s;

  s = "abc";
  BOOST_CHECK_EQUAL(cass::escape_id(s), std::string("abc"));

  s = "aBc";
  BOOST_CHECK_EQUAL(cass::escape_id(s), std::string("\"aBc\""));

  s = "\"";
  BOOST_CHECK_EQUAL(cass::escape_id(s), std::string("\"\"\"\""));

  s = "a\"Bc";
  BOOST_CHECK_EQUAL(cass::escape_id(s), std::string("\"a\"\"Bc\""));
}

BOOST_AUTO_TEST_CASE(num_leading_zeros)
{
  BOOST_CHECK_EQUAL(64, cass::num_leading_zeros(0));
  BOOST_CHECK_EQUAL(0, cass::num_leading_zeros(1LL << 63));
  BOOST_CHECK_EQUAL(0, cass::num_leading_zeros(1LL << 63 | 1 << 5));
}

BOOST_AUTO_TEST_CASE(next_pow_2)
{
  BOOST_CHECK_EQUAL(STATIC_NEXT_POW_2(1u), 2u);
  BOOST_CHECK_EQUAL(STATIC_NEXT_POW_2(1u), 2u);
  BOOST_CHECK_EQUAL(STATIC_NEXT_POW_2(2u), 2u);

  BOOST_CHECK_EQUAL(STATIC_NEXT_POW_2(3u), 4u);
  BOOST_CHECK_EQUAL(STATIC_NEXT_POW_2(4u), 4u);

  BOOST_CHECK_EQUAL(STATIC_NEXT_POW_2(7u), 8u);
  BOOST_CHECK_EQUAL(STATIC_NEXT_POW_2(8u), 8u);

  BOOST_CHECK_EQUAL(STATIC_NEXT_POW_2(9u), 16u);
  BOOST_CHECK_EQUAL(STATIC_NEXT_POW_2(15u), 16u);
  BOOST_CHECK_EQUAL(STATIC_NEXT_POW_2(16u), 16u);

  BOOST_CHECK_EQUAL(STATIC_NEXT_POW_2(17u), 32u);
  BOOST_CHECK_EQUAL(STATIC_NEXT_POW_2(31u), 32u);
  BOOST_CHECK_EQUAL(STATIC_NEXT_POW_2(32u), 32u);
}

BOOST_AUTO_TEST_SUITE_END()
