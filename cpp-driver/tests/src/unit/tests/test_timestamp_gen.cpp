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

#include "unit.hpp"

#include "get_time.hpp"
#include "timestamp_generator.hpp"

#include <utility>

using namespace datastax;
using namespace datastax::internal;
using namespace datastax::internal::core;

static void clock_skew_log_callback(const CassLogMessage* message, void* data) {
  String msg(message->message);
  int* counter = reinterpret_cast<int*>(data);
  if (msg.find("Clock skew detected") != String::npos) {
    (*counter)++;
  }
}

class TimestampGenUnitTest : public Unit {
public:
  int run_monotonic_timestamp_gen(uint64_t warning_threshold_us, uint64_t warning_interval_ms,
                                  uint64_t duration_ms) {
    const int NUM_TIMESTAMPS_PER_ITERATION = 1000;

    MonotonicTimestampGenerator gen(warning_threshold_us, warning_interval_ms);

    int timestamp_count = 0;
    int warn_count = 0;

    Logger::set_log_level(CASS_LOG_WARN);
    Logger::set_callback(clock_skew_log_callback, &warn_count);

    uint64_t start = get_time_since_epoch_ms();
    uint64_t elapsed;

    do {
      int64_t prev = gen.next();
      for (int i = 0; i < NUM_TIMESTAMPS_PER_ITERATION; ++i) {
        int64_t now = gen.next();
        // Verify that timestamps are alway increasing
        EXPECT_GT(now, prev);
        prev = now;
        timestamp_count++;
      }

      elapsed = get_time_since_epoch_ms() - start;
    } while (elapsed < duration_ms);

    // We can generate at most 1,000,000 timestamps in a second. If we exceed
    // this limit and the clock skew threshold then a warning log should have
    // been printed.
    double timestamp_rate = (static_cast<double>(timestamp_count) / elapsed) * 1000;
    if (timestamp_rate <= 1000000.0 ||
        elapsed * MICROSECONDS_PER_MILLISECOND <= warning_threshold_us) {
      fprintf(stderr, "Warning: The test may not have exceeded the timestamp "
                      "generator's maximum rate.");
    }

    EXPECT_GT(warn_count, 0);

    return warn_count;
  }
};

TEST_F(TimestampGenUnitTest, Server) {
  ServerSideTimestampGenerator gen;
  EXPECT_EQ(gen.next(), CASS_INT64_MIN);
}

TEST_F(TimestampGenUnitTest, Monotonic) {
  MonotonicTimestampGenerator gen;

  int64_t prev = gen.next();
  for (int i = 0; i < 100; ++i) {
    int64_t now = gen.next();
    // Verify that timestamps are alway increasing
    EXPECT_GT(now, prev);
    prev = now;
  }
}

TEST_F(TimestampGenUnitTest, MonotonicExceedWarningThreshold) {
  // Set the threshold to something small that we're guaranteed to easily exceed.
  run_monotonic_timestamp_gen(1, 1000, 1000);
}

TEST_F(TimestampGenUnitTest, MonotonicWarningInterval) {
  // Run for 1000 ms so that we give time for the generation rate to exceed the
  // warning threshold for a good amount of time.
  int warn_count_100ms = run_monotonic_timestamp_gen(1, 100, 1000);
  int warn_count_1000ms = run_monotonic_timestamp_gen(1, 1000, 1000);

  // The 100ms timestamp generator should have logged more times because
  // it had a shorter interval.
  EXPECT_GT(warn_count_100ms, warn_count_1000ms);
}
