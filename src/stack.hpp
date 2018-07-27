/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __DSE_STACK_HPP_INCLUDED__
#define __DSE_STACK_HPP_INCLUDED__

#include "deque.hpp"

#include <stack>

namespace cass {

template <class T, class Container = Deque<T> >
class Stack : public std::stack<T,  Container> {
public:
  explicit Stack( const Container& container = Container())
    : std::stack<T, Container>(container) { }

  Stack(const Stack& other)
    : std::stack<T, Container>(other) { }
};

} // namespace cass

#endif
