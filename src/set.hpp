/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef DATASTAX_INTERNAL_SET_HPP
#define DATASTAX_INTERNAL_SET_HPP

#include "allocated.hpp"
#include "allocator.hpp"

#include <set>

namespace datastax { namespace internal {

template <class T, class Compare = std::less<T> >
class Set
    : public Allocated
    , public std::set<T, Compare, internal::Allocator<T> > {
public:
  typedef internal::Allocator<T> Allocator;

  explicit Set(const Compare& compare = Compare(), const Allocator& alloc = Allocator())
      : std::set<T, Compare, Allocator>(compare, alloc) {}

  Set(const Set& other)
      : std::set<T, Compare, Allocator>(other) {}

  template <class InputIt>
  Set(InputIt first, InputIt last, const Compare& compare = Compare(),
      const Allocator& alloc = Allocator())
      : std::set<T, Compare, Allocator>(first, last, compare, alloc) {}
};

}} // namespace datastax::internal

#endif
