/*
  Copyright (c) DataStax, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
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
