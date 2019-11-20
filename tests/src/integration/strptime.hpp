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

#ifndef __STRPTIME_HPP__
#define __STRPTIME_HPP__

#include <time.h>

namespace test {

/**
 * strptime() is not available on all platforms. This implementation is from musl libc.
 *
 * @param s The string to be parsed
 * @param f The format of the string
 * @param tm The resulting time struct
 * @return NULL if an error occurred
 */
char* strptime(const char* s, const char* f, struct tm* tm);

} // namespace test

#endif // __STRPTIME_HPP__
