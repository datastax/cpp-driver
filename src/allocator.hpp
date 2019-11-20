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

#ifndef DATASTAX_INTERNAL_ALLOCATOR_HPP
#define DATASTAX_INTERNAL_ALLOCATOR_HPP

#include "memory.hpp"

#include <limits>

namespace datastax { namespace internal {

template <class T>
class Allocator {
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
    typedef Allocator<U> other;
  };

  Allocator() {}

  template <class U>
  Allocator(const Allocator<U>&) {}

  bool operator!=(const Allocator&) const { return false; }
  bool operator==(const Allocator&) const { return true; }

  pointer address(reference x) const { return &x; }

  const_pointer address(const_reference x) const { return &x; }

  pointer allocate(size_type n, const_pointer hint = NULL) {
    return static_cast<pointer>(Memory::malloc(sizeof(T) * n));
  }

  void deallocate(pointer p, size_type n) { Memory::free(p); }

  void construct(pointer p, const_reference x) { new (p) value_type(x); }

  void destroy(pointer p) { p->~value_type(); }

  size_type max_size() const throw() { return std::numeric_limits<size_type>::max(); }
};

}} // namespace datastax::internal

#endif
