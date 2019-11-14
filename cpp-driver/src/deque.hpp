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

#ifndef DATASTAX_INTERNAL_DEQUE_HPP
#define DATASTAX_INTERNAL_DEQUE_HPP

#include "allocator.hpp"

#include <deque>

namespace datastax { namespace internal {

template <class T>
class Deque : public std::deque<T, internal::Allocator<T> > {
public:
  typedef internal::Allocator<T> Allocator;

  explicit Deque(const Allocator& alloc = Allocator())
      : std::deque<T, Allocator>(alloc) {}

  explicit Deque(size_t count, const T& value = T(), const Allocator& alloc = Allocator())
      : std::deque<T, Allocator>(count, value, alloc) {}

  template <class InputIt>
  Deque(InputIt first, InputIt last, const Allocator& alloc = Allocator())
      : std::deque<T, Allocator>(first, last, alloc) {}
};

}} // namespace datastax::internal

#endif
