/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __DSE_DENSE_HASH_MAP_HPP_INCLUDED__
#define __DSE_DENSE_HASH_MAP_HPP_INCLUDED__

#include "allocator.hpp"

#include <sparsehash/dense_hash_map>

namespace cass {

template <class K, class V,
          class HashFcn = SPARSEHASH_HASH<K>,
          class EqualKey = std::equal_to<K> >
class DenseHashMap
    : public sparsehash::dense_hash_map<
      K, V,  HashFcn, EqualKey,
      cass::Allocator<std::pair<const K, V> > > {
public:
  typedef cass::Allocator<std::pair<const K, V> > Allocator;

  explicit DenseHashMap(size_t expected_max_items_in_table = 0,
                        const HashFcn& hf = HashFcn(),
                        const EqualKey& eql = EqualKey(),
                        const Allocator& alloc = Allocator())
    : sparsehash::dense_hash_map<K, V, HashFcn, EqualKey, Allocator>(expected_max_items_in_table,
                                                                     hf, eql, alloc) { }

  template <class InputIterator>
  DenseHashMap(InputIterator first, InputIterator last,
               const K& empty_key_val,
               size_t expected_max_items_in_table = 0,
               const HashFcn& hf = HashFcn(),
               const EqualKey& eql = EqualKey(),
               const Allocator& alloc = Allocator())
    : sparsehash::dense_hash_map<K, V, HashFcn, EqualKey, Allocator>(first, last,
                                                                     empty_key_val,
                                                                     expected_max_items_in_table,
                                                                     hf, eql, alloc) { }
};

} // namespace cass

#endif
