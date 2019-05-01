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

#ifndef DATASTAX_INTERNAL_HASH_HPP
#define DATASTAX_INTERNAL_HASH_HPP

#include <algorithm>
#include <stdint.h>

namespace datastax { namespace hash {

typedef int(Op)(int);

inline int nop(int c) { return c; }

#if defined(__x86_64__) || defined(_M_X64) || defined(__aarch64__)
#define FNV1_64_INIT 0xcbf29ce484222325ULL
#define FNV1_64_PRIME 0x100000001b3ULL

inline uint64_t fnv1a(const char* data, size_t length, Op op = nop) {
  uint64_t h = FNV1_64_INIT;
  for (size_t i = 0; i < length; ++i) {
    h ^= static_cast<uint64_t>(op(data[i]));
    h *= FNV1_64_PRIME;
  }
  return h;
}
#else
#define FNV1_32_INIT 0x811c9dc5
#define FNV1_32_PRIME 0x01000193

inline uint32_t fnv1a(const char* data, size_t length, Op op = nop) {
  uint32_t h = FNV1_32_INIT;
  for (size_t i = 0; i < length; ++i) {
    h ^= static_cast<uint32_t>(op(data[i]));
    h *= FNV1_32_PRIME;
  }
  return h;
}
#endif

}} // namespace datastax::hash

#endif
