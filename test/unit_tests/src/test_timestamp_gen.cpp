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

#include <boost/test/unit_test.hpp>

static void clock_skew_log_callback(const CassLogMessage* message, void* data) {
  std::string msg(message->message);
  int* counter = reinterpret_cast<int*>(data);
  if (msg.find("Clock skew detected") != std::string::npos) {
    (*counter)++;
  }
}

BOOST_AUTO_TEST_SUITE(timestamp_gen)

BOOST_AUTO_TEST_CASE(server)
{
  cass::ServerSideTimestampGenerator gen;
  BOOST_CHECK(gen.next() == CASS_INT64_MIN);
}

BOOST_AUTO_TEST_CASE(monotonic)
{
  const int NUM_TIMESTAMPS = 5000000;

  cass::MonotonicTimestampGenerator gen;
  int counter = 0;

  cass::Logger::set_callback(clock_skew_log_callback, &counter);

  uint64_t start = cass::get_time_since_epoch_ms();

  int64_t prev = gen.next();
  for (int i = 0; i < NUM_TIMESTAMPS; ++i) {
    int64_t now = gen.next();
    // Verify that timestamps are alway increasing
    BOOST_CHECK(now > prev);
    prev = now;
  }
  double elapsed = (cass::get_time_since_epoch_ms() - start) / 1000.0; // Convert to seconds

  // We can generate at most 1,000,000 timestamps in a second. If we exceed this
  // limit and the clock skew threshold then a log should have been printed.
  double timestamp_rate = static_cast<double>(NUM_TIMESTAMPS) / elapsed;
  if (timestamp_rate > 1000000.0 && elapsed > 1.0) {
    BOOST_CHECK(counter > 0);
  } else {
    BOOST_TEST_MESSAGE("Warning: The test did not exceed the timestamp generator maximum rate.");
    BOOST_CHECK(counter == 0);
  }
}

BOOST_AUTO_TEST_CASE(monotonic_warning_interval) {
  const int NUM_TIMESTAMPS = 2500000;

  cass::MonotonicTimestampGenerator gen_default;
  cass::MonotonicTimestampGenerator gen_100ms(1000000, 100); // 100 ms interval

  int counter_default = 0;
  int counter_100ms = 0;

  cass::Logger::set_callback(clock_skew_log_callback, &counter_default);

  int64_t start = cass::get_time_since_epoch_ms();

  int64_t prev = gen_default.next();
  for (int i = 0; i < NUM_TIMESTAMPS; ++i) {
    int64_t now = gen_default.next();
    // Verify that timestamps are alway increasing
    BOOST_CHECK(now > prev);
    prev = now;
  }
  double elapsed_default = (cass::get_time_since_epoch_ms() - start) / 1000.0; // Convert to seconds

  cass::Logger::set_callback(clock_skew_log_callback, &counter_100ms);

  start = cass::get_time_since_epoch_ms();

  prev = gen_100ms.next();
  for (int i = 0; i < NUM_TIMESTAMPS; ++i) {
    int64_t now = gen_100ms.next();
    // Verify that timestamps are alway increasing
    BOOST_CHECK(now > prev);
    prev = now;
  }

  double elapsed_100ms = (cass::get_time_since_epoch_ms() - start) / 1000.0; // Convert to seconds

  double timestamp_rate_default = static_cast<double>(NUM_TIMESTAMPS) / elapsed_default;
  if (timestamp_rate_default > 1000000.0 && elapsed_default > 1.0) {
    BOOST_CHECK(counter_default > 0);
  } else {
    BOOST_TEST_MESSAGE("Warning: The default generator did not exceed its maximum rate.");
    BOOST_CHECK(counter_default == 0);
  }

  double timestamp_rate_100ms = static_cast<double>(NUM_TIMESTAMPS) / elapsed_100ms;
  if (timestamp_rate_100ms > 1000000.0 && elapsed_100ms > 1.0) {
    BOOST_CHECK(counter_100ms > 0);
  } else {
    BOOST_TEST_MESSAGE("Warning: The 100ms generator did not exceed its maximum rate.");
    BOOST_CHECK(counter_100ms == 0);
  }

  // The 100ms timestamp generator should have logged more times because
  // it had a shorter iterval.
  BOOST_CHECK(counter_100ms > counter_default);
}

BOOST_AUTO_TEST_SUITE_END()


