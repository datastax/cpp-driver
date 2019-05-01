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

#ifndef DATASTAX_INTERNAL_ALIGNED_STORAGE_HPP
#define DATASTAX_INTERNAL_ALIGNED_STORAGE_HPP

#include <stddef.h>

#if defined(_MSC_VER)
#define ALIGN_AS(Alignment) __declspec(align(Alignment))
#define ALIGN_OF(Type) __alignof(Type)
#elif defined(__GNUC__)
#define ALIGN_AS(Alignment) __attribute__((aligned(Alignment)))
#define ALIGN_OF(Type) __alignof__(Type)
#else
#error Unsupported compiler!
#endif

namespace datastax { namespace internal {

// This allows for the allocation of memory that is of the same size and
// alignment as a required by a non-POD, but is represented as a POD type (char).
// The allows for the memory to be correctly allocated without invoking the
// constructor.

template <size_t N, size_t A>
class AlignedStorage;

#define ALIGNED_STORAGE(Alignment)                \
  template <size_t N>                             \
  class AlignedStorage<N, Alignment> {            \
  public:                                         \
    void* address() { return data_; }             \
    const void* address() const { return data_; } \
                                                  \
  private:                                        \
    ALIGN_AS(Alignment) char data_[N];            \
  }

ALIGNED_STORAGE(1);
ALIGNED_STORAGE(2);
ALIGNED_STORAGE(4);
ALIGNED_STORAGE(8);
ALIGNED_STORAGE(16);
ALIGNED_STORAGE(32);
ALIGNED_STORAGE(64);

#undef ALIGNED_STORAGE

}} // namespace datastax::internal

#endif
