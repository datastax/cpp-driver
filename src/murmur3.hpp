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

/*-----------------------------------------------------------------------------
 * MurmurHash3 was written by Austin Appleby, and is placed in the public
 * domain. The author hereby disclaims copyright to this source code.
 */

#ifndef DATASTAX_INTERNAL_MURMUR3_HPP
#define DATASTAX_INTERNAL_MURMUR3_HPP

#include "macros.hpp"

#include <uv.h>

namespace datastax { namespace internal {

int64_t MurmurHash3_x64_128(const void* key, const int len, const uint32_t seed);

}} // namespace datastax::internal

#endif
