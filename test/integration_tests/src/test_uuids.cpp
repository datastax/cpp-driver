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
#include "test_utils.hpp"
#include "get_time.hpp"

#include <boost/algorithm/string.hpp>
#include <boost/chrono.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/thread.hpp>

#include <string.h>

BOOST_AUTO_TEST_SUITE(uuids)

BOOST_AUTO_TEST_CASE(v1)
{
  test_utils::CassUuidGenPtr uuid_gen(cass_uuid_gen_new());

  CassUuid prev_uuid;
  cass_uuid_gen_time(uuid_gen.get(), &prev_uuid);
  BOOST_CHECK(cass_uuid_version(prev_uuid) == 1);

  for (int i = 0; i < 1000; ++i) {
    CassUuid uuid;
    uint64_t curr_ts = cass::get_time_since_epoch_ms();
    cass_uuid_gen_time(uuid_gen.get(), &uuid);
    cass_uint64_t ts = cass_uuid_timestamp(uuid);

    BOOST_CHECK(cass_uuid_version(uuid) == 1);
    BOOST_CHECK(ts == curr_ts || ts - 1 == curr_ts);

    // This can't compare the uuids directly because a uuid timestamp is
    // only accurate to the millisecond. The generated uuid might have more
    // granularity.
    CassUuid from_ts_uuid;
    cass_uuid_gen_from_time(uuid_gen.get(), ts, &from_ts_uuid);
    BOOST_CHECK(ts == cass_uuid_timestamp(from_ts_uuid));
    BOOST_CHECK(cass_uuid_version(from_ts_uuid) == 1);

    BOOST_CHECK(uuid != prev_uuid);
    prev_uuid = uuid;
  }
}

BOOST_AUTO_TEST_CASE(v1_node)
{
  test_utils::CassUuidGenPtr uuid_gen(cass_uuid_gen_new_with_node(0x0000112233445566LL));

  CassUuid uuid;
  cass_uuid_gen_time(uuid_gen.get(), &uuid);
  BOOST_CHECK(cass_uuid_version(uuid) == 1);

  char str[CASS_UUID_STRING_LENGTH];
  cass_uuid_string(uuid, str);

  BOOST_CHECK(strstr(str, "-112233445566") != NULL);
}

BOOST_AUTO_TEST_CASE(v4)
{
  test_utils::CassUuidGenPtr uuid_gen(cass_uuid_gen_new());

  CassUuid prev_uuid;
  cass_uuid_gen_random(uuid_gen.get(), &prev_uuid);
  BOOST_CHECK(cass_uuid_version(prev_uuid) == 4);

  for (int i = 0; i < 1000; ++i) {
    CassUuid uuid;
    cass_uuid_gen_random(uuid_gen.get(), &uuid);
    BOOST_CHECK(cass_uuid_version(uuid) == 4);
    BOOST_CHECK(uuid != prev_uuid);
    prev_uuid = uuid;
  }
}

BOOST_AUTO_TEST_CASE(from_string)
{
  CassUuid uuid;
  const std::string expected = "c3b54ca0-7b01-11e4-aea6-c30dd51eaa64";
  char actual[CASS_UUID_STRING_LENGTH];

  BOOST_CHECK(cass_uuid_from_string(expected.c_str(), &uuid) == CASS_OK);
  cass_uuid_string(uuid, actual);
  BOOST_CHECK(expected == actual);

  const std::string upper  = boost::to_upper_copy(expected);
  BOOST_CHECK(cass_uuid_from_string(upper.c_str(), &uuid) == CASS_OK);
  cass_uuid_string(uuid, actual);
  BOOST_CHECK(expected == actual);
}

BOOST_AUTO_TEST_CASE(from_string_invalid)
{
  CassUuid uuid;
  // Empty
  BOOST_CHECK(cass_uuid_from_string("", &uuid) == CASS_ERROR_LIB_BAD_PARAMS);
  // One char short
  BOOST_CHECK(cass_uuid_from_string("c3b54ca0-7b01-11e4-aea6-c30dd51eaa6", &uuid) == CASS_ERROR_LIB_BAD_PARAMS);
  // All '-'
  BOOST_CHECK(cass_uuid_from_string("------------------------------------", &uuid) == CASS_ERROR_LIB_BAD_PARAMS);
  // Invalid char
  BOOST_CHECK(cass_uuid_from_string("c3b54ca0-7b01-11e4-aea6-c30dd51eaz64", &uuid) == CASS_ERROR_LIB_BAD_PARAMS);
  // Extra '-'
  BOOST_CHECK(cass_uuid_from_string("c3b54ca0-7b01-11e4-aea6-c30dd51eaa-4", &uuid) == CASS_ERROR_LIB_BAD_PARAMS);
  // Invalid group
  BOOST_CHECK(cass_uuid_from_string("c3b54ca07b0-1-11e4-aea6-c30dd51eaa64", &uuid) == CASS_ERROR_LIB_BAD_PARAMS);
}

BOOST_AUTO_TEST_SUITE_END()
