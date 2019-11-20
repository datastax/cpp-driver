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

#ifndef DATASTAX_INTERNAL_DENSE_HASH_SET_HPP
#define DATASTAX_INTERNAL_DENSE_HASH_SET_HPP

#include "allocator.hpp"

#include <sparsehash/dense_hash_set>

namespace datastax { namespace internal {

template <class T, class HashFcn = SPARSEHASH_HASH<T>, class EqualKey = std::equal_to<T> >
class DenseHashSet
    : public sparsehash::dense_hash_set<T, HashFcn, EqualKey, internal::Allocator<T> > {
public:
  typedef internal::Allocator<T> Allocator;

  explicit DenseHashSet(size_t expected_max_items_in_table = 0, const HashFcn& hf = HashFcn(),
                        const EqualKey& eql = EqualKey(), const Allocator& alloc = Allocator())
      : sparsehash::dense_hash_set<T, HashFcn, EqualKey, Allocator>(expected_max_items_in_table, hf,
                                                                    eql, alloc) {}

  template <class InputIterator>
  DenseHashSet(InputIterator first, InputIterator last, const T& empty_key_val,
               size_t expected_max_items_in_table = 0, const HashFcn& hf = HashFcn(),
               const EqualKey& eql = EqualKey(), const Allocator& alloc = Allocator())
      : sparsehash::dense_hash_set<T, HashFcn, EqualKey, Allocator>(
            first, last, empty_key_val, expected_max_items_in_table, hf, eql, alloc) {}
};

}} // namespace datastax::internal

#endif
