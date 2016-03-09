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

#include "cassandra.h"

#include <boost/test/unit_test.hpp>
#include <time.h>

BOOST_AUTO_TEST_SUITE(date_time)

BOOST_AUTO_TEST_CASE(simple)
{
  time_t now = ::time(NULL);
  cass_uint32_t date = cass_date_from_epoch(now);
  cass_int64_t time = cass_time_from_epoch(now);
  BOOST_CHECK(cass_date_time_to_epoch(date, time) == now);
}

BOOST_AUTO_TEST_CASE(date)
{
  BOOST_CHECK(cass_date_from_epoch(0) ==2147483648u);
  BOOST_CHECK(cass_date_from_epoch(24 * 3600) ==2147483649u);
  BOOST_CHECK(cass_date_from_epoch(2 * 24 * 3600) ==2147483650u);
}

BOOST_AUTO_TEST_CASE(time)
{
  time_t now = ::time(NULL);
  struct tm* tm = gmtime(&now);
  cass_int64_t actual = cass_time_from_epoch(now);
  cass_int64_t expected = 1000000000LL * (tm->tm_hour * 3600  + 60 * tm->tm_min + tm->tm_sec);
  BOOST_CHECK(actual == expected);
}

BOOST_AUTO_TEST_SUITE_END()
