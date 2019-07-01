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

#include "timestamp_generator.hpp"

#include "external.hpp"
#include "get_time.hpp"
#include "logger.hpp"

using namespace datastax::internal::core;

extern "C" {

CassTimestampGen* cass_timestamp_gen_server_side_new() {
  TimestampGenerator* timestamp_gen = new ServerSideTimestampGenerator();
  timestamp_gen->inc_ref();
  return CassTimestampGen::to(timestamp_gen);
}

CassTimestampGen* cass_timestamp_gen_monotonic_new() {
  TimestampGenerator* timestamp_gen = new MonotonicTimestampGenerator();
  timestamp_gen->inc_ref();
  return CassTimestampGen::to(timestamp_gen);
}

CassTimestampGen* cass_timestamp_gen_monotonic_new_with_settings(int64_t warning_threshold_us,
                                                                 int64_t warning_interval_ms) {
  TimestampGenerator* timestamp_gen =
      new MonotonicTimestampGenerator(warning_threshold_us, warning_interval_ms);
  timestamp_gen->inc_ref();
  return CassTimestampGen::to(timestamp_gen);
}

void cass_timestamp_gen_free(CassTimestampGen* timestamp_gen) { timestamp_gen->dec_ref(); }

} // extern "C"

int64_t MonotonicTimestampGenerator::next() {
  while (true) {
    int64_t last = last_.load();
    int64_t next = compute_next(last);
    if (last_.compare_exchange_strong(last, next)) {
      return next;
    }
  }
}

// This is guaranteed to return a monotonic timestamp. If clock skew is detected
// then this method will increment the last timestamp.
int64_t MonotonicTimestampGenerator::compute_next(int64_t last) {
  int64_t current = get_time_since_epoch_us();

  if (last >= current) { // There's clock skew
    // If we exceed our warning threshold then warn periodically that clock
    // skew has been detected.
    if (warning_threshold_us_ >= 0 && last > current + warning_threshold_us_) {
      // Using a monotonic clock to prevent the effects of clock skew from properly
      // triggering warnings.
      int64_t now = get_time_monotonic_ns() / NANOSECONDS_PER_MILLISECOND;
      int64_t last_warning = last_warning_.load();
      if (now > last_warning + warning_interval_ms_ &&
          last_warning_.compare_exchange_strong(last_warning, now)) {
        LOG_WARN("Clock skew detected. The current time (%lld) was %lld "
                 "microseconds behind the last generated timestamp (%lld). "
                 "The next generated timestamp will be artificially incremented "
                 "to guarantee monotonicity.",
                 static_cast<long long>(current), static_cast<long long>(last - current),
                 static_cast<long long>(last));
      }
    }

    return last + 1;
  }

  return current;
}
