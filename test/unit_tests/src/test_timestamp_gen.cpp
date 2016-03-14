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

#include "timestamp_generator.hpp"

#include <boost/test/unit_test.hpp>


BOOST_AUTO_TEST_SUITE(timestamp_gen)

BOOST_AUTO_TEST_CASE(server)
{
 cass::ServerSideTimestampGenerator gen;
 BOOST_CHECK(gen.next() == CASS_INT64_MIN);
}

 // This test may log: "Sub-millisecond counter overflowed, some query timestamps will not be distinct"
BOOST_AUTO_TEST_CASE(monotonic)
{
 cass::MonotonicTimestampGenerator gen;
 // This should get at least 1000 unique timestamps
 int64_t ts = gen.next();
 BOOST_CHECK(gen.next() > CASS_INT64_MIN);
 for (int i = 0; i < 999; ++i) {
   int64_t now = gen.next();
   BOOST_CHECK(now > ts);
 }
 BOOST_CHECK(gen.next() >= ts);
}

BOOST_AUTO_TEST_SUITE_END()


