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

#if defined(_WIN32)

#ifndef _WINSOCKAPI_
#define _WINSOCKAPI_
#endif

#include <Windows.h>

#include "get_time.hpp"

namespace datastax { namespace internal {

// Information for using the query performance counter can be found here:
// https://msdn.microsoft.com/en-us/library/dn553408(v=vs.85).aspx

class ClockInfo {
public:
  ClockInfo() {
    LARGE_INTEGER frequency;
    frequency_ = QueryPerformanceFrequency(&frequency) ? frequency.QuadPart : 0;
  }

  static uint64_t frequency() { return frequency_; }

private:
  static uint64_t frequency_;
};

uint64_t ClockInfo::frequency_;

static ClockInfo __clock_info__; // Initializer

uint64_t get_time_since_epoch_us() {
  _FILETIME ft;
  GetSystemTimeAsFileTime(&ft);
  uint64_t ns100 =
      (static_cast<uint64_t>(ft.dwHighDateTime) << 32 | static_cast<uint64_t>(ft.dwLowDateTime)) -
      116444736000000000LL; // 100 nanosecond increments between
                            // Jan. 1, 1601 - Jan. 1, 1970
  return ns100 / 10;        // 100 nanosecond increments to microseconds
}

uint64_t get_time_monotonic_ns() {
  if (ClockInfo::frequency() != 0) {
    LARGE_INTEGER counter;
    QueryPerformanceCounter(&counter);

    counter.QuadPart *= NANOSECONDS_PER_SECOND;
    return static_cast<uint64_t>(counter.QuadPart) / ClockInfo::frequency();
  } else {
    return get_time_since_epoch_us() * NANOSECONDS_PER_MICROSECOND;
  }
}

}} // namespace datastax::internal

#endif // defined(_WIN32)
