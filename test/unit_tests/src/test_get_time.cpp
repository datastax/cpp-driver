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

#include "get_time.hpp"

#include <boost/test/unit_test.hpp>
#include <boost/thread/thread.hpp>

BOOST_AUTO_TEST_SUITE(get_time)

BOOST_AUTO_TEST_CASE(monotonic)
{
  uint64_t prev = cass::get_time_monotonic_ns();
  for (int i = 0; i < 100; ++i) {
    uint64_t current = cass::get_time_monotonic_ns();
    BOOST_CHECK(current >= prev);
    prev = current;
  }
}

BOOST_AUTO_TEST_CASE(monotonic_duration)
{
  // Sleep can be off by as much as 10+ ms on most systems (or >10% for 100ms)
  double tolerance = 15.0;

#ifdef _WIN32
  // Sleep can be off more on Windows; increasing tolerance
  // https://msdn.microsoft.com/en-us/library/windows/desktop/ms686298(v=vs.85).aspx
  tolerance = 25.0;
#endif

  uint64_t start = cass::get_time_monotonic_ns();
  boost::this_thread::sleep_for(boost::chrono::seconds(1));
  uint64_t elapsed = cass::get_time_monotonic_ns() - start;
  BOOST_REQUIRE_CLOSE(static_cast<double>(elapsed),
                      static_cast<double>(NANOSECONDS_PER_SECOND),
                      tolerance);
}

BOOST_AUTO_TEST_SUITE_END()
