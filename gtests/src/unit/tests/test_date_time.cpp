/*
  Copyright (c) DataStax, Inc.

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

#include <gtest/gtest.h>

#include "cassandra.h"

#include <time.h>

#define CASS_DATE_EPOCH 2147483648U

TEST(DateTimeUnitTests, Simple) {
  time_t now = ::time(NULL);
  cass_uint32_t date = cass_date_from_epoch(now);
  cass_int64_t time = cass_time_from_epoch(now);
  EXPECT_EQ(cass_date_time_to_epoch(date, time), now);
}

TEST(DateTimeUnitTests, DateFromEpoch) {
  EXPECT_EQ(cass_date_from_epoch(0), CASS_DATE_EPOCH);
  EXPECT_EQ(cass_date_from_epoch(24 * 3600), CASS_DATE_EPOCH + 1);
  EXPECT_EQ(cass_date_from_epoch(2 * 24 * 3600), CASS_DATE_EPOCH + 2);
}

TEST(DateTimeUnitTests, TimeFromEpoch) {
  time_t now = ::time(NULL);
  struct tm* tm = gmtime(&now);
  cass_int64_t actual = cass_time_from_epoch(now);
  cass_int64_t expected = 1000000000LL * (tm->tm_hour * 3600 + 60 * tm->tm_min + tm->tm_sec);
  EXPECT_EQ(actual, expected);
}

TEST(DateTimeUnitTests, DateTimeToEpoch) {
  EXPECT_EQ(cass_date_time_to_epoch(CASS_DATE_EPOCH, 0), 0L);               // Epoch
  EXPECT_EQ(cass_date_time_to_epoch(CASS_DATE_EPOCH - 1, 0), -24L * 3600L); // Epoch - 1 day
  EXPECT_EQ(cass_date_time_to_epoch(CASS_DATE_EPOCH + 1, 0), 24L * 3600L);  // Epoch + 1 day
}
