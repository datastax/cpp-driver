/*
  Copyright (c) 2014 DataStax

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

#define BOOST_TEST_DYN_LINK
#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include "cassandra.h"

#include <boost/chrono.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/thread.hpp>

BOOST_AUTO_TEST_SUITE(uuids)

BOOST_AUTO_TEST_CASE(v1)
{
  CassUuid uuid;
  cass_uuid_generate_time(uuid);
  BOOST_CHECK(cass_uuid_version(uuid) == 1);

  cass_uint64_t last_ts = cass_uuid_timestamp(uuid);

  for (int i = 0; i < 10; ++i) {
    boost::this_thread::sleep_for(boost::chrono::milliseconds(1));
    cass_uuid_generate_time(uuid);

    cass_uint64_t ts = cass_uuid_timestamp(uuid);

    BOOST_CHECK(cass_uuid_version(uuid) == 1);
    BOOST_CHECK(ts > last_ts);
    last_ts = ts;
  }
}

BOOST_AUTO_TEST_SUITE_END()
