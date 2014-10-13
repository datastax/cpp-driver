/*
  Copyright (c) 2014 DataStax

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

#if defined(WIN32) || defined(_WIN32)
#include <Windows.h>
#elif defined(__APPLE__) && defined(__MACH__)
#include <sys/time.h>
#else
#include <time.h>
#endif

namespace cass {

#if defined(WIN32) || defined(_WIN32)

uint64_t get_time_since_epoch_ms() {
  _FILETIME ft;
  GetSystemTimeAsFileTime(&ft);
  uint64_t ns100 = (static_cast<uint64_t>(ft.dwHighDateTime) << 32 |
                    static_cast<uint64_t>(ft.dwLowDateTime)) -
                   116444736000000000LL; // 100 nanosecond increments between
                                         // Jan. 1, 1601 - Jan. 1, 1970
  return ns100 / 10000;                  // 100 nanoseconds to milliseconds
}

#elif defined(__APPLE__) && defined(__MACH__)

uint64_t get_time_since_epoch_ms() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

#else

uint64_t get_time_since_epoch_ms() {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

#endif
}
