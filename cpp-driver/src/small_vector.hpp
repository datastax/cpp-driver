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

#ifndef DATASTAX_INTERNAL_SMALL_VECTOR_HPP
#define DATASTAX_INTERNAL_SMALL_VECTOR_HPP

#include "fixed_allocator.hpp"

#include <vector>

namespace datastax { namespace internal {

// This vector uses a fixed buffer as long as it doesn't exceed N items.
// This can help to avoid heap allocation in cases where the vector remains
// small and doesn't excceed the fixed buffer.

template <class T, size_t N>
class SmallVector : public std::vector<T, FixedAllocator<T, N> > {
public:
  SmallVector()
      : std::vector<T, FixedAllocator<T, N> >(FixedAllocator<T, N>(&fixed_)) {
    this->reserve(N);
  }

  SmallVector(size_t inital_size)
      : std::vector<T, FixedAllocator<T, N> >(FixedAllocator<T, N>(&fixed_)) {
    this->resize(inital_size);
  }

  const typename FixedAllocator<T, N>::Fixed& fixed() const { return fixed_; }

private:
  typename FixedAllocator<T, N>::Fixed fixed_;

private:
  DISALLOW_COPY_AND_ASSIGN(SmallVector);
};

}} // namespace datastax::internal

#endif
