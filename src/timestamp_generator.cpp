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

#include "timestamp_generator.hpp"

#include "external.hpp"
#include "get_time.hpp"
#include "logger.hpp"

extern "C" {

CassTimestampGen* cass_timestamp_gen_server_side_new() {
  cass::TimestampGenerator* timestamp_gen = new cass::ServerSideTimestampGenerator();
  timestamp_gen->inc_ref();
  return CassTimestampGen::to(timestamp_gen);
}

CassTimestampGen* cass_timestamp_gen_monotonic_new() {
  cass::TimestampGenerator* timestamp_gen = new cass::MonotonicTimestampGenerator();
  timestamp_gen->inc_ref();
  return CassTimestampGen::to(timestamp_gen);
}

void cass_timestamp_gen_free(CassTimestampGen* timestamp_gen) {
  timestamp_gen->dec_ref();
}

} // extern "C"

namespace cass {

int64_t MonotonicTimestampGenerator::next() {
  while (true) {
    int64_t last = last_.load();
    int64_t next = compute_next(last);
    if (last_.compare_exchange_strong(last, next)) {
      return next;
    }
  }
}

int64_t MonotonicTimestampGenerator::compute_next(int64_t last) {
  int64_t millis = last / 1000;
  int64_t counter = last % 1000;

  int64_t now = get_time_since_epoch_ms();

  if (millis >= now) {
    if (counter == 999) {
      LOG_WARN("Sub-millisecond counter overflowed, some query timestamps will not be distinct");
    } else {
      ++counter;
    }
  } else {
    millis = now;
    counter = 0;
  }

  return 1000 * millis + counter;
}

} // namespace cass
