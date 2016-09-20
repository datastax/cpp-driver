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

#include "address.hpp"

#include <boost/test/unit_test.hpp>
#include <time.h>

BOOST_AUTO_TEST_SUITE(address)

BOOST_AUTO_TEST_CASE(compare_ipv4)
{
  BOOST_CHECK(cass::Address("255.255.255.255", 9042).compare(cass::Address("0.0.0.0", 9042)) > 0);
  BOOST_CHECK(cass::Address("0.0.0.0", 9042).compare(cass::Address("255.255.255.255", 9042)) < 0);
  BOOST_CHECK(cass::Address("1.2.3.4", 9042).compare(cass::Address("1.2.3.4", 9042)) == 0);
}

BOOST_AUTO_TEST_CASE(compare_no_port)
{
  BOOST_CHECK(cass::Address("0.0.0.0", 0).compare(cass::Address("0.0.0.0", 1), false) == 0);
  BOOST_CHECK(cass::Address("0.0.0.0", 0).compare(cass::Address("0.0.0.0", 1), true) < 0);
  BOOST_CHECK(cass::Address("0.0.0.0", 1).compare(cass::Address("0.0.0.0", 0), true) > 0);
}

BOOST_AUTO_TEST_SUITE_END()

