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
#include "logger.hpp"
#include "timestamp_generator.hpp"

#include <utility>
#include <boost/test/unit_test.hpp>

static void clock_skew_log_callback(const CassLogMessage* message, void* data) {
  std::string msg(message->message);
  int* counter = reinterpret_cast<int*>(data);
  if (msg.find("Clock skew detected") != std::string::npos) {
    (*counter)++;
  }
}

int run_monotonic_timestamp_gen(uint64_t warning_threshold_us, uint64_t warning_interval_ms, uint64_t duration_ms) {
  const int NUM_TIMESTAMPS_PER_ITERATION = 1000;

  cass::MonotonicTimestampGenerator gen(warning_threshold_us, warning_interval_ms);

  int timestamp_count = 0;
  int warn_count = 0;

  cass::Logger::set_log_level(CASS_LOG_WARN);
  cass::Logger::set_callback(clock_skew_log_callback, &warn_count);

  uint64_t start = cass::get_time_since_epoch_ms();
  uint64_t elapsed;

  do {
    int64_t prev = gen.next();
    for (int i = 0; i < NUM_TIMESTAMPS_PER_ITERATION; ++i) {
      int64_t now = gen.next();
      // Verify that timestamps are alway increasing
      BOOST_CHECK(now > prev);
      prev = now;
      timestamp_count++;
    }

    elapsed = cass::get_time_since_epoch_ms() - start;
  } while (elapsed < duration_ms);

  // We can generate at most 1,000,000 timestamps in a second. If we exceed this
  // limit and the clock skew threshold then a warning log should have been printed.
  double timestamp_rate = (static_cast<double>(timestamp_count) / elapsed) * 1000;
  if (timestamp_rate <= 1000000.0 ||
      elapsed * MICROSECONDS_PER_MILLISECOND <= warning_threshold_us) {
    BOOST_TEST_MESSAGE("Warning: The test may not have exceeded the timestamp generator's maximum rate.");
  }

  BOOST_CHECK(warn_count > 0);

  return warn_count;
}

BOOST_AUTO_TEST_SUITE(timestamp_gen)

BOOST_AUTO_TEST_CASE(server)
{
  cass::ServerSideTimestampGenerator gen;
  BOOST_CHECK(gen.next() == CASS_INT64_MIN);
}

BOOST_AUTO_TEST_CASE(monotonic)
{
  cass::MonotonicTimestampGenerator gen;

  int64_t prev = gen.next();
  for (int i = 0; i < 100; ++i) {
    int64_t now = gen.next();
    // Verify that timestamps are alway increasing
    BOOST_CHECK(now > prev);
    prev = now;
  }
}

BOOST_AUTO_TEST_CASE(monotonic_exceed_warning_threshold)  {
  // Set the threshold to something small that we're guaranteed to easily exceed.
  run_monotonic_timestamp_gen(1, 1000, 1000);
}

BOOST_AUTO_TEST_CASE(monotonic_warning_interval) {
  // Run for 1000 ms so that we give time for the generation rate to exceed the
  // warning threshold for a good amount of time.
  int warn_count_100ms = run_monotonic_timestamp_gen(1, 100, 1000);
  int warn_count_1000ms = run_monotonic_timestamp_gen(1, 1000, 1000);

  // The 100ms timestamp generator should have logged more times because
  // it had a shorter interval.
  BOOST_CHECK(warn_count_100ms > warn_count_1000ms);
}

BOOST_AUTO_TEST_SUITE_END()


