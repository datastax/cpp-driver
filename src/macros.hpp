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

#ifndef DATASTAX_INTERNAL_MACROS_HPP
#define DATASTAX_INTERNAL_MACROS_HPP

#include <stddef.h>
#include <string.h>

#define SAFE_STRLEN(s) ((s) ? strlen(s) : 0)

#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
  TypeName(const TypeName&);               \
  TypeName& operator=(const TypeName&)

#define UNUSED_(X) ((void)X)

#define ZERO_PARAMS1_()
#define ZERO_PARAMS_() ZERO_PARAMS1_()

#define ONE_PARAM1_(A) , A
#define ONE_PARAM_(A) ONE_PARAM1_(A)

#define TWO_PARAMS1_(A, B) , A, B
#define TWO_PARAMS_(A, B) TWO_PARAMS1_(A, B)

#define THREE_PARAMS1_(A, B, C) , A, B, C
#define THREE_PARAMS_(A, B, C) THREE_PARAMS1_(A, B, C)

// Done this way so that macros like __LINE__ will expand before
// being concatenated.
#define STATIC_ASSERT_CONCAT(Arg1, Arg2) STATIC_ASSERT_CONCAT1(Arg1, Arg2)
#define STATIC_ASSERT_CONCAT1(Arg1, Arg2) STATIC_ASSERT_CONCAT2(Arg1, Arg2)
#define STATIC_ASSERT_CONCAT2(Arg1, Arg2) Arg1##Arg2

#define STATIC_ASSERT(Expression)                                                               \
  struct STATIC_ASSERT_CONCAT(__static_assertion_at_line_, __LINE__) {                          \
    StaticAssert<static_cast<bool>(Expression)>                                                 \
        STATIC_ASSERT_CONCAT(STATIC_ASSERTION_FAILED_AT_LINE_, __LINE__);                       \
  };                                                                                            \
  typedef StaticAssertTest<sizeof(STATIC_ASSERT_CONCAT(__static_assertion_at_line_, __LINE__))> \
      STATIC_ASSERT_CONCAT(__static_assertion_test_at_line_, __LINE__)

template <bool>
struct StaticAssert;
template <>
struct StaticAssert<true> {};
template <size_t s>
struct StaticAssertTest {};

#define STATIC_NEXT_POW_2(N) StaticNextPow2<N>::value

template <size_t C, size_t N>
struct StaticNextPow2Helper {
  enum {
    value = static_cast<size_t>(StaticNextPow2Helper<C - 1, N>::value) < N
                ? static_cast<size_t>(1) << C
                : static_cast<size_t>(StaticNextPow2Helper<C - 1, N>::value)
  };
};

template <size_t N>
struct StaticNextPow2Helper<1, N> {
  enum { value = 2 };
};

template <size_t N>
struct StaticNextPow2 {
  enum { value = StaticNextPow2Helper<8 * sizeof(size_t) - 1, N>::value };
};

#define STRINGIFY_HELPER(s) #s
#define STRINGIFY(s) STRINGIFY_HELPER(s)

#endif
