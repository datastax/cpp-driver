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

#ifndef DATASTAX_INTERNAL_FIXED_ALLOCATOR_HPP
#define DATASTAX_INTERNAL_FIXED_ALLOCATOR_HPP

#include "aligned_storage.hpp"
#include "macros.hpp"
#include "memory.hpp"

#include <limits>
#include <memory>

namespace datastax { namespace internal {

// This is an allocator that starts using a fixed size buffer that only
// uses the heap when exceeded. The allocator can be
// copied so the vector itself needs to hold on to the fixed buffer.

template <class T, size_t N>
class FixedAllocator {
public:
  typedef size_t size_type;
  typedef ptrdiff_t difference_type;
  typedef T value_type;
  typedef T* pointer;
  typedef const T* const_pointer;
  typedef T& reference;
  typedef const T& const_reference;

  template <class U>
  struct rebind {
    typedef FixedAllocator<U, N> other;
  };

  struct Fixed {
    Fixed()
        : is_used(false) {}

    // See aligned_storage.hpp for why this is required
    typedef AlignedStorage<N * sizeof(T), ALIGN_OF(T)> Storage;

    bool is_used;
    Storage data;
  };

  FixedAllocator()
      : fixed_(NULL) {}

  FixedAllocator(Fixed* fixed)
      : fixed_(fixed) {}

  FixedAllocator(const FixedAllocator<T, N>& allocator)
      : fixed_(allocator.fixed_) {}

  template <class U, size_t M>
  FixedAllocator(const FixedAllocator<U, M>& allocator)
      : fixed_(NULL) {}

  FixedAllocator(const std::allocator<T>& allocator)
      : fixed_(NULL) {}

  pointer address(reference x) const { return &x; }

  const_pointer address(const_reference x) const { return &x; }

  pointer allocate(size_type n, const_pointer hint = NULL) {
    if (fixed_ != NULL && !fixed_->is_used && n <= N) {
      fixed_->is_used = true; // Don't reuse the buffer
      return static_cast<T*>(fixed_->data.address());
    } else {
      return static_cast<T*>(Memory::malloc(sizeof(T) * n));
    }
  }

  void deallocate(pointer p, size_type n) {
    if (fixed_ != NULL && fixed_->data.address() == p) {
      fixed_->is_used = false; // It's safe to reuse the buffer
    } else {
      Memory::free(p);
    }
  }

  void construct(pointer p, const_reference x) { new (p) value_type(x); }

  void destroy(pointer p) { p->~value_type(); }

  size_type max_size() const throw() { return std::numeric_limits<size_type>::max(); }

private:
  Fixed* fixed_;
};

}} // namespace datastax::internal

#endif
