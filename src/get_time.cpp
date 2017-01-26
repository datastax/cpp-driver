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

#include "get_time.hpp"

#if defined(_WIN32)
#ifndef _WINSOCKAPI_
#define _WINSOCKAPI_
#endif
#include <Windows.h>
#elif defined(__APPLE__) && defined(__MACH__)
#include <mach/mach.h>
#include <mach/mach_time.h>
#include <sys/time.h>
#else
#include <time.h>
#endif

namespace cass {

#if defined(_WIN32)

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
  uint64_t ns100 = (static_cast<uint64_t>(ft.dwHighDateTime) << 32 |
                    static_cast<uint64_t>(ft.dwLowDateTime)) -
                   116444736000000000LL; // 100 nanosecond increments between
                                         // Jan. 1, 1601 - Jan. 1, 1970
  return ns100 / 10;                     // 100 nanosecond increments to microseconds
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

#elif defined(__APPLE__) && defined(__MACH__)

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
  return static_cast<uint64_t>(tv.tv_sec) * 1000000 +
         static_cast<uint64_t>(tv.tv_usec);
}

uint64_t get_time_monotonic_ns() {
  uint64_t time = mach_absolute_time();
  return time * ClockInfo::frequency();
}

#else

class ClockInfo {
public:
  ClockInfo() {
    struct timespec res;
    struct timespec tp;
    supports_monotonic_ = clock_getres(CLOCK_MONOTONIC, &res) == 0 &&
                          clock_gettime(CLOCK_MONOTONIC, &tp) == 0;
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
  return static_cast<uint64_t>(ts.tv_sec)  * 1000000 +
         static_cast<uint64_t>(ts.tv_nsec) / 1000;
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

#endif
}
