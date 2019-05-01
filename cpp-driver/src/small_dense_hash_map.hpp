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

#ifndef DATASTAX_INTERNAL_SMALL_DENSE_HASH_MAP_HPP
#define DATASTAX_INTERNAL_SMALL_DENSE_HASH_MAP_HPP

#include "fixed_allocator.hpp"
#include "macros.hpp"

#include <assert.h>
#include <sparsehash/dense_hash_map>

// This should be the same as sparsehash::densehashtable<>::HT_OCCUPANCY_PCT
// which can be found at the bottom of "sparesehash/internal/densehashtable.h".
#define OCCUPANCY_PCT 50

#define MIN_BUCKETS(N) STATIC_NEXT_POW_2((((N * 100) / OCCUPANCY_PCT) + 1))

namespace datastax { namespace internal { namespace core {

// This hash map uses a fixed buffer as long as it doesn't exceed N items.
// This can help to avoid heap allocation in cases where the hash map remains
// small and doesn't excceed the fixed buffer.

template <class K, class V, size_t N, class HashFcn = SPARSEHASH_HASH<K>,
          class EqualKey = std::equal_to<K> >
class SmallDenseHashMap
    : public sparsehash::dense_hash_map<K, V, HashFcn, EqualKey,
                                        FixedAllocator<std::pair<const K, V>, MIN_BUCKETS(N)> > {
public:
  typedef std::pair<const K, V> Pair;
  typedef FixedAllocator<std::pair<const K, V>, MIN_BUCKETS(N)> Allocator;
  typedef sparsehash::dense_hash_map<K, V, HashFcn, EqualKey, Allocator> HashMap;

  SmallDenseHashMap()
      : HashMap(N, typename HashMap::hasher(), typename HashMap::key_equal(), Allocator(&fixed_)) {
    assert(MIN_BUCKETS(N) >= this->bucket_count());
  }

  SmallDenseHashMap(size_t expected_max_items_in_table)
      : HashMap(expected_max_items_in_table, HashMap::hasher(), HashMap::key_equal(),
                Allocator(&fixed_)) {
    assert(MIN_BUCKETS(N) >= this->bucket_count());
  }

  const typename Allocator::Fixed& fixed() const { return fixed_; }

private:
  typename Allocator::Fixed fixed_;

private:
  DISALLOW_COPY_AND_ASSIGN(SmallDenseHashMap);
};

}}} // namespace datastax::internal::core

#endif
