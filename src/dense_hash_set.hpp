/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __DSE_DENSE_HASH_SET_HPP_INCLUDED__
#define __DSE_DENSE_HASH_SET_HPP_INCLUDED__

#include "allocator.hpp"

#include <sparsehash/dense_hash_set>

namespace cass {

template <class T,
          class HashFcn = SPARSEHASH_HASH<T>,
          class EqualKey = std::equal_to<T> >
class DenseHashSet
    : public sparsehash::dense_hash_set<
      T,  HashFcn, EqualKey,
      cass::Allocator<T> > {
public:
  typedef cass::Allocator<T> Allocator;

  explicit DenseHashSet(size_t expected_max_items_in_table = 0,
                        const HashFcn& hf = HashFcn(),
                        const EqualKey& eql = EqualKey(),
                        const Allocator& alloc = Allocator())
    : sparsehash::dense_hash_set<T, HashFcn, EqualKey, Allocator>(expected_max_items_in_table,
                                                                  hf, eql, alloc) { }

  template <class InputIterator>
  DenseHashSet(InputIterator first, InputIterator last,
               const T& empty_key_val,
               size_t expected_max_items_in_table = 0,
               const HashFcn& hf = HashFcn(),
               const EqualKey& eql = EqualKey(),
               const Allocator& alloc = Allocator())
    : sparsehash::dense_hash_set<T, HashFcn, EqualKey, Allocator>(first, last,
                                                                  empty_key_val,
                                                                  expected_max_items_in_table,
                                                                  hf, eql, alloc) { }
};

} // namespace cass

#endif
