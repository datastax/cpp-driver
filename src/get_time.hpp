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

#ifndef __CASS_GET_TIME_HPP_INCLUDED__
#define __CASS_GET_TIME_HPP_INCLUDED__

#include <uv.h>

#define NANOSECONDS_PER_MICROSECOND  1000LL
#define NANOSECONDS_PER_MILLISECOND  1000000LL
#define NANOSECONDS_PER_SECOND       1000000000LL

#define MICROSECONDS_PER_MILLISECOND 1000LL

namespace cass {

uint64_t get_time_since_epoch_us();

inline uint64_t get_time_since_epoch_ms() {
  return get_time_since_epoch_us() / MICROSECONDS_PER_MILLISECOND;
}

// This is a best effort monotonic clock with an arbitrary start time. If the
// system or platform doesn't have a monotonic clock then
// `get_time_since_epoch_us()` will be used.
uint64_t get_time_monotonic_ns();

}

#endif
