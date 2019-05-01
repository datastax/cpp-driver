/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef DATASTAX_INTERNAL_STACK_HPP
#define DATASTAX_INTERNAL_STACK_HPP

#include "deque.hpp"

#include <stack>

namespace datastax { namespace internal {

template <class T, class Container = Deque<T> >
class Stack : public std::stack<T, Container> {
public:
  explicit Stack(const Container& container = Container())
      : std::stack<T, Container>(container) {}

  Stack(const Stack& other)
      : std::stack<T, Container>(other) {}
};

}} // namespace datastax::internal

#endif
