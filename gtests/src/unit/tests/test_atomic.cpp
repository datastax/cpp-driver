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

#include <gtest/gtest.h>

#include "atomic.hpp"

#include <limits>
#include <stdint.h>

using datastax::internal::Atomic;

enum Enum { ONE, TWO, THREE };

struct Object {};

template <class T>
void test_atomic_integer() {
  const T max_value = std::numeric_limits<T>::max();
  const T min_value = std::numeric_limits<T>::min();

  const T zero = static_cast<T>(0);
  const T one = static_cast<T>(1);

  Atomic<T> i(zero);

  EXPECT_EQ(i.load(), zero);

  EXPECT_EQ(i.exchange(one), zero);
  EXPECT_EQ(i.load(), one);

  i.store(zero);
  T expected = zero;
  EXPECT_TRUE(i.compare_exchange_strong(expected, one));
  EXPECT_EQ(expected, zero);
  EXPECT_EQ(i.load(), one);

  EXPECT_TRUE(!i.compare_exchange_strong(expected, one));
  EXPECT_EQ(expected, one);
  EXPECT_EQ(i.load(), one);

  i.store(zero);
  expected = zero;
  EXPECT_TRUE(i.compare_exchange_weak(expected, one));
  EXPECT_EQ(expected, zero);
  EXPECT_EQ(i.load(), one);

  EXPECT_TRUE(!i.compare_exchange_weak(expected, one));
  EXPECT_EQ(expected, one);
  EXPECT_EQ(i.load(), one);

  i.store(zero);
  EXPECT_EQ(i.fetch_add(one), zero);
  EXPECT_EQ(i.load(), one);
  EXPECT_EQ(i.fetch_sub(one), one);
  EXPECT_EQ(i.load(), zero);

  i.store(max_value);
  EXPECT_EQ(i.load(), max_value);
  EXPECT_EQ(i.fetch_add(one), max_value);
  EXPECT_EQ(i.load(), min_value);

  i.store(min_value);
  EXPECT_EQ(i.fetch_sub(one), min_value);
  EXPECT_EQ(i.load(), max_value);
}

TEST(AtomicUnitTest, Integers) {
  test_atomic_integer<int32_t>();
  test_atomic_integer<int64_t>();
  test_atomic_integer<uint32_t>();
  test_atomic_integer<uint64_t>();
}

TEST(AtomicUnitTest, Enumeration) {
  Atomic<Enum> e(ONE);

  EXPECT_EQ(e.load(), ONE);

  EXPECT_EQ(e.exchange(TWO), 0);
  EXPECT_EQ(e.load(), TWO);

  e.store(ONE);
  Enum expected = ONE;
  EXPECT_TRUE(e.compare_exchange_strong(expected, TWO));
  EXPECT_EQ(expected, ONE);
  EXPECT_EQ(e.load(), TWO);

  EXPECT_TRUE(!e.compare_exchange_strong(expected, TWO));
  EXPECT_EQ(expected, TWO);
  EXPECT_EQ(e.load(), TWO);

  e.store(ONE);
  expected = ONE;
  EXPECT_TRUE(e.compare_exchange_weak(expected, TWO));
  EXPECT_EQ(expected, ONE);
  EXPECT_EQ(e.load(), TWO);

  EXPECT_TRUE(!e.compare_exchange_weak(expected, TWO));
  EXPECT_EQ(expected, TWO);
  EXPECT_EQ(e.load(), TWO);
}

TEST(AtomicUnitTest, Pointer) {
  Object one, two;
  Atomic<Object*> p(&one);

  EXPECT_EQ(p.load(), &one);

  EXPECT_EQ(p.exchange(&two), &one);
  EXPECT_EQ(p.load(), &two);

  p.store(&one);
  Object* expected = &one;
  EXPECT_TRUE(p.compare_exchange_strong(expected, &two));
  EXPECT_EQ(expected, &one);
  EXPECT_EQ(p.load(), &two);

  EXPECT_TRUE(!p.compare_exchange_strong(expected, &two));
  EXPECT_EQ(expected, &two);
  EXPECT_EQ(p.load(), &two);

  p.store(&one);
  expected = &one;
  EXPECT_TRUE(p.compare_exchange_weak(expected, &two));
  EXPECT_EQ(expected, &one);
  EXPECT_EQ(p.load(), &two);

  EXPECT_TRUE(!p.compare_exchange_weak(expected, &two));
  EXPECT_EQ(expected, &two);
  EXPECT_EQ(p.load(), &two);
}

TEST(AtomicUnitTest, Boolean) {
  Atomic<bool> b(false);

  EXPECT_EQ(b.load(), false);

  EXPECT_EQ(b.exchange(true), false);
  EXPECT_EQ(b.load(), true);

  b.store(false);
  bool expected = false;
  EXPECT_TRUE(b.compare_exchange_strong(expected, true));
  EXPECT_EQ(expected, false);
  EXPECT_EQ(b.load(), true);

  EXPECT_TRUE(!b.compare_exchange_strong(expected, true));
  EXPECT_EQ(expected, true);
  EXPECT_EQ(b.load(), true);

  b.store(false);
  expected = false;
  EXPECT_TRUE(b.compare_exchange_weak(expected, true));
  EXPECT_EQ(expected, false);
  EXPECT_EQ(b.load(), true);

  EXPECT_TRUE(!b.compare_exchange_weak(expected, true));
  EXPECT_EQ(expected, true);
  EXPECT_EQ(b.load(), true);
}
