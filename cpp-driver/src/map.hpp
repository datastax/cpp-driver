/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
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

  Map(const Map& other)
      : std::map<K, V, Compare, Allocator>(other) {}

  template <class InputIt>
  Map(InputIt first, InputIt last, const Compare& compare = Compare(),
      const Allocator& alloc = Allocator())
      : std::map<K, V, Compare, Allocator>(first, last, compare, alloc) {}
};

}} // namespace datastax::internal

#endif
