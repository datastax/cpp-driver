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

#include "stream_manager.hpp"

#include <boost/test/unit_test.hpp>


BOOST_AUTO_TEST_SUITE(streams)


BOOST_AUTO_TEST_CASE(max_streams)
{
  BOOST_REQUIRE(cass::StreamManager<int>(1).max_streams() == 128);
  BOOST_REQUIRE(cass::StreamManager<int>(2).max_streams() == 128);
  BOOST_REQUIRE(cass::StreamManager<int>(3).max_streams() == 32768);
}

BOOST_AUTO_TEST_CASE(simple)
{
  const int protocol_versions[] = { 1, 3, 0 };

  for (const int* version = protocol_versions; *version > 0; ++version) {
    cass::StreamManager<int> streams(*version);

    for (size_t i = 0; i < streams.max_streams(); ++i) {
      int stream = streams.acquire(i);
      BOOST_REQUIRE(stream >= 0);
    }

    // Verify there are no more streams left
    BOOST_CHECK(streams.acquire(streams.max_streams()) < 0);

    for (size_t i = 0; i < streams.max_streams(); ++i) {
      int item = -1;
      BOOST_CHECK(streams.get_pending_and_release(i, item));
      BOOST_CHECK(item >= 0);
    }

    for (size_t i = 0; i < streams.max_streams(); ++i) {
      int stream = streams.acquire(i);
      BOOST_REQUIRE(stream >= 0);
    }

    // Verify there are no more streams left
    BOOST_CHECK(streams.acquire(streams.max_streams()) < 0);
  }
}

BOOST_AUTO_TEST_CASE(release)
{
  const int protocol_versions[] = { 1, 3, 0 };

  for (const int* version = protocol_versions; *version > 0; ++version) {
    cass::StreamManager<int> streams(*version);

    for (size_t i = 0; i < streams.max_streams(); ++i) {
      int stream = streams.acquire(i);
      BOOST_REQUIRE(stream >= 0);
    }

    // Verify there are no more streams left
    BOOST_CHECK(streams.acquire(streams.max_streams()) < 0);

    // Verify that the stream the was previously release is re-acquired
    for (size_t i = 0; i < streams.max_streams(); ++i) {
      streams.release(i);
      int stream = streams.acquire(i);
      BOOST_REQUIRE(static_cast<size_t>(stream) == i);
    }

    // Verify there are no more streams left
    BOOST_CHECK(streams.acquire(streams.max_streams()) < 0);
  }
}

BOOST_AUTO_TEST_SUITE_END()
