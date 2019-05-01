/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef DATASTAX_INTERNAL_DEQUE_HPP
#define DATASTAX_INTERNAL_DEQUE_HPP

#include "allocator.hpp"

#include <deque>

namespace datastax { namespace internal {

template <class T>
class Deque : public std::deque<T, internal::Allocator<T> > {
public:
  typedef internal::Allocator<T> Allocator;

  explicit Deque(const Allocator& alloc = Allocator())
      : std::deque<T, Allocator>(alloc) {}

  explicit Deque(size_t count, const T& value = T(), const Allocator& alloc = Allocator())
      : std::deque<T, Allocator>(count, value, alloc) {}

  template <class InputIt>
  Deque(InputIt first, InputIt last, const Allocator& alloc = Allocator())
      : std::deque<T, Allocator>(first, last, alloc) {}

  Deque(const Deque& other)
      : std::deque<T, Allocator>(other) {}
};

}} // namespace datastax::internal

#endif
