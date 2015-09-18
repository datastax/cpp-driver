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

#include "cassandra.h"

#include <boost/test/unit_test.hpp>
#include <boost/date_time.hpp>
#include <string.h>
#include <time.h>

BOOST_AUTO_TEST_SUITE(date_time)

BOOST_AUTO_TEST_CASE(simple)
{
  time_t now = time(NULL);
  cass_uint32_t date = cass_date_from_epoch(now);
  cass_int64_t time = cass_time_from_epoch(now);
  BOOST_CHECK(cass_date_time_to_epoch(date, time) == now);
}

BOOST_AUTO_TEST_SUITE_END()
