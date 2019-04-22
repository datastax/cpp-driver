/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __DSE_SET_HPP_INCLUDED__
#define __DSE_SET_HPP_INCLUDED__

#include "allocated.hpp"
#include "allocator.hpp"

#include <set>

namespace cass {

template <class T, class Compare = std::less<T> >
class Set
    : public Allocated
    , public std::set<T, Compare, cass::Allocator<T> > {
public:
  typedef cass::Allocator<T> Allocator;

  explicit Set(const Compare& compare = Compare(),
               const Allocator& alloc = Allocator())
    : std::set<T, Compare, Allocator>(compare, alloc) { }

  Set(const Set& other)
    : std::set<T, Compare, Allocator>(other) { }

  template<class InputIt>
  Set(InputIt first, InputIt last,
      const Compare& compare = Compare(),
      const Allocator& alloc = Allocator())
    : std::set<T, Compare, Allocator>(first, last, compare, alloc) { }
};

} // namespace cass

#endif
