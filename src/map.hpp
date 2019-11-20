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

#ifndef DATASTAX_INTERNAL_MAP_HPP
#define DATASTAX_INTERNAL_MAP_HPP

#include "allocated.hpp"
#include "allocator.hpp"

#include <map>

namespace datastax { namespace internal {

template <class K, class V, class Compare = std::less<K> >
class Map
    : public Allocated
    , public std::map<K, V, Compare, internal::Allocator<std::pair<const K, V> > > {
public:
  typedef internal::Allocator<std::pair<const K, V> > Allocator;

  explicit Map(const Compare& compare = Compare(), const Allocator& alloc = Allocator())
      : std::map<K, V, Compare, Allocator>(compare, alloc) {}

  template <class InputIt>
  Map(InputIt first, InputIt last, const Compare& compare = Compare(),
      const Allocator& alloc = Allocator())
      : std::map<K, V, Compare, Allocator>(first, last, compare, alloc) {}
};

}} // namespace datastax::internal

#endif
