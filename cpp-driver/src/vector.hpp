/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef DATASTAX_INTERNAL_VECTOR_HPP
#define DATASTAX_INTERNAL_VECTOR_HPP

#include "allocated.hpp"
#include "allocator.hpp"

#include <vector>

namespace datastax { namespace internal {

template <class T>
class Vector
    : public Allocated
    , public std::vector<T, internal::Allocator<T> > {
public:
  typedef internal::Allocator<T> Allocator;

  explicit Vector(const Allocator& alloc = Allocator())
      : std::vector<T, Allocator>(alloc) {}

  explicit Vector(size_t count, const T& value = T())
      : std::vector<T, Allocator>(count, value) {}

  Vector(const Vector& other)
      : std::vector<T, Allocator>(other) {}

  template <class InputIt>
  Vector(InputIt first, InputIt last)
      : std::vector<T, Allocator>(first, last) {}
};

}} // namespace datastax::internal

#endif
