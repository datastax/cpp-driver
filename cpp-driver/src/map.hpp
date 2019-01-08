/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __DSE_MAP_HPP_INCLUDED__
#define __DSE_MAP_HPP_INCLUDED__

#include "allocated.hpp"
#include "allocator.hpp"

#include <map>

namespace cass {

template <class K, class V, class Compare = std::less<K> >
class Map :
    public Allocated,
    public std::map<K, V, Compare, cass::Allocator<std::pair<const K, V> > > {
public:
  typedef cass::Allocator<std::pair<const K, V> > Allocator;

  explicit Map(const Compare& compare = Compare(),
               const Allocator& alloc = Allocator())
    : std::map<K, V, Compare, Allocator>(compare, alloc) { }

  Map(const Map& other)
    : std::map<K, V, Compare, Allocator>(other) { }

  template<class InputIt>
  Map(InputIt first, InputIt last,
      const Compare& compare = Compare(),
      const Allocator& alloc = Allocator())
    : std::map<K, V, Compare, Allocator>(first, last, compare, alloc) { }
};

} // namespace cass

#endif
