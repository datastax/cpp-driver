/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __DSE_VECTOR_HPP_INCLUDED__
#define __DSE_VECTOR_HPP_INCLUDED__

#include "allocated.hpp"
#include "allocator.hpp"

#include <vector>

namespace cass {

template <class T>
class Vector
    : public Allocated
    , public std::vector<T, cass::Allocator<T> > {
public:
  typedef cass::Allocator<T> Allocator;

  explicit Vector(const Allocator& alloc = Allocator())
    : std::vector<T, Allocator>(alloc) { }

  explicit Vector(size_t count, const T& value = T())
    : std::vector<T, Allocator>(count, value) { }

  Vector(const Vector& other)
    : std::vector<T, Allocator>(other) { }

  template<class InputIt>
  Vector(InputIt first, InputIt last)
    : std::vector<T, Allocator>(first, last) { }
};

} // namepsace cass

#endif
