/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef DATASTAX_INTERNAL_DENSE_HASH_MAP_HPP
#define DATASTAX_INTERNAL_DENSE_HASH_MAP_HPP

#include "allocator.hpp"

#include <sparsehash/dense_hash_map>

namespace datastax { namespace internal {

template <class K, class V, class HashFcn = SPARSEHASH_HASH<K>, class EqualKey = std::equal_to<K> >
class DenseHashMap
    : public sparsehash::dense_hash_map<K, V, HashFcn, EqualKey,
                                        internal::Allocator<std::pair<const K, V> > > {
public:
  typedef internal::Allocator<std::pair<const K, V> > Allocator;

  explicit DenseHashMap(size_t expected_max_items_in_table = 0, const HashFcn& hf = HashFcn(),
                        const EqualKey& eql = EqualKey(), const Allocator& alloc = Allocator())
      : sparsehash::dense_hash_map<K, V, HashFcn, EqualKey, Allocator>(expected_max_items_in_table,
                                                                       hf, eql, alloc) {}

  template <class InputIterator>
  DenseHashMap(InputIterator first, InputIterator last, const K& empty_key_val,
               size_t expected_max_items_in_table = 0, const HashFcn& hf = HashFcn(),
               const EqualKey& eql = EqualKey(), const Allocator& alloc = Allocator())
      : sparsehash::dense_hash_map<K, V, HashFcn, EqualKey, Allocator>(
            first, last, empty_key_val, expected_max_items_in_table, hf, eql, alloc) {}
};

}} // namespace datastax::internal

#endif
