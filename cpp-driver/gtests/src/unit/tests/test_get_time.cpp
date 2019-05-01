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

#include "get_time.hpp"
#include "test_utils.hpp"

using datastax::internal::get_time_monotonic_ns;

TEST(GetTimeUnitTest, Monotonic) {
  uint64_t prev = get_time_monotonic_ns();
  for (int i = 0; i < 100; ++i) {
    uint64_t current = get_time_monotonic_ns();
    EXPECT_GE(current, prev);
    prev = current;
  }
}

TEST(GetTimeUnitTest, MonotonicDuration) {
  uint64_t start = get_time_monotonic_ns();

  test::Utils::msleep(1000); // 1 second
  uint64_t elapsed = get_time_monotonic_ns() - start;
  EXPECT_GE(elapsed, static_cast<double>(NANOSECONDS_PER_SECOND));
  EXPECT_LE(elapsed, static_cast<double>(2 * NANOSECONDS_PER_SECOND));
}
