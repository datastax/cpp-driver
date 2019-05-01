/*
  Copyright (c) DataStax, Inc

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

#include <unistd.h>

#if _POSIX_TIMERS > 0

#include "get_time.hpp"

#include <time.h>

namespace datastax { namespace internal {

class ClockInfo {
public:
  ClockInfo() {
    struct timespec res;
    struct timespec tp;
    supports_monotonic_ =
        clock_getres(CLOCK_MONOTONIC, &res) == 0 && clock_gettime(CLOCK_MONOTONIC, &tp) == 0;
  }

  static bool supports_monotonic() { return supports_monotonic_; }

private:
  static bool supports_monotonic_;
};

bool ClockInfo::supports_monotonic_;

static ClockInfo __clock_info__; // Initializer

uint64_t get_time_since_epoch_us() {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return static_cast<uint64_t>(ts.tv_sec) * 1000000 + static_cast<uint64_t>(ts.tv_nsec) / 1000;
}

uint64_t get_time_monotonic_ns() {
  if (ClockInfo::supports_monotonic()) {
    struct timespec tp;
    clock_gettime(CLOCK_MONOTONIC, &tp);
    return static_cast<uint64_t>(tp.tv_sec) * NANOSECONDS_PER_SECOND +
           static_cast<uint64_t>(tp.tv_nsec);
  } else {
    return get_time_since_epoch_us() * NANOSECONDS_PER_MICROSECOND;
  }
}

}} // namespace datastax::internal

#endif
