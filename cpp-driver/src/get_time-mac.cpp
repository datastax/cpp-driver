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

#if defined(__APPLE__) && defined(__MACH__)

#include "get_time.hpp"

#include <mach/mach.h>
#include <mach/mach_time.h>
#include <sys/time.h>

namespace datastax { namespace internal {

// Information on converting the absolute time to nanoseconds can be found
// here: https://developer.apple.com/library/content/qa/qa1398/_index.html.

class ClockInfo {
public:
  ClockInfo() {
    mach_timebase_info_data_t info;
    mach_timebase_info(&info);
    frequency_ = info.numer / info.denom;
  }

  static uint64_t frequency() { return frequency_; }

private:
  static uint64_t frequency_;
};

uint64_t ClockInfo::frequency_;

static ClockInfo __clock_info__; // Initializer

uint64_t get_time_since_epoch_us() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000 + static_cast<uint64_t>(tv.tv_usec);
}

uint64_t get_time_monotonic_ns() {
  uint64_t time = mach_absolute_time();
  return time * ClockInfo::frequency();
}

}} // namespace datastax::internal

#endif // defined(__APPLE__) && defined(__MACH__)
