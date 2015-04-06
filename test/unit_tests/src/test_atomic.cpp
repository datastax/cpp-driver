/*
  Copyright (c) 2014-2015 DataStax

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

#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include "atomic.hpp"

#include <boost/test/unit_test.hpp>

#include <limits>
#include <stdint.h>

enum Enum {
  ONE, TWO, THREE
};

struct Object {};

template <class T>
void test_atomic_integer() {
  const T max_value = std::numeric_limits<T>::max();
  const T min_value = std::numeric_limits<T>::min();

  cass::Atomic<T> i(0);

  BOOST_CHECK(i.load() == 0);

  BOOST_CHECK(i.exchange(1) == 0);
  BOOST_CHECK(i.load() == 1);

  i.store(0);
  T expected = 0;
  BOOST_CHECK(i.compare_exchange_strong(expected, 1));
  BOOST_CHECK(expected == 0);
  BOOST_CHECK(i.load() == 1);

  BOOST_CHECK(!i.compare_exchange_strong(expected, 1));
  BOOST_CHECK(expected == 1);
  BOOST_CHECK(i.load() == 1);

  i.store(0);
  expected = 0;
  BOOST_CHECK(i.compare_exchange_weak(expected, 1));
  BOOST_CHECK(expected == 0);
  BOOST_CHECK(i.load() == 1);

  BOOST_CHECK(!i.compare_exchange_weak(expected, 1));
  BOOST_CHECK(expected == 1);
  BOOST_CHECK(i.load() == 1);

  i.store(0);
  BOOST_CHECK(i.fetch_add(1) == 0);
  BOOST_CHECK(i.load() == 1);
  BOOST_CHECK(i.fetch_sub(1) == 1);
  BOOST_CHECK(i.load() == 0);

  i.store(max_value);
  BOOST_CHECK(i.load() == max_value);
  BOOST_CHECK(i.fetch_add(1) == max_value);
  BOOST_CHECK(i.load() == min_value);

  i.store(min_value);
  BOOST_CHECK(i.fetch_sub(1) == min_value);
  BOOST_CHECK(i.load() == max_value);
}

BOOST_AUTO_TEST_SUITE(atomic)

BOOST_AUTO_TEST_CASE(integers)
{
  test_atomic_integer<int32_t>();
  test_atomic_integer<int64_t>();
  test_atomic_integer<uint32_t>();
  test_atomic_integer<uint64_t>();
}

BOOST_AUTO_TEST_CASE(enumeration)
{
  cass::Atomic<Enum> e(ONE);

  BOOST_CHECK(e.load() == ONE);

  BOOST_CHECK(e.exchange(TWO) == 0);
  BOOST_CHECK(e.load() == TWO);

  e.store(ONE);
  Enum expected = ONE;
  BOOST_CHECK(e.compare_exchange_strong(expected, TWO));
  BOOST_CHECK(expected == ONE);
  BOOST_CHECK(e.load() == TWO);

  BOOST_CHECK(!e.compare_exchange_strong(expected, TWO));
  BOOST_CHECK(expected == TWO);
  BOOST_CHECK(e.load() == TWO);

  e.store(ONE);
  expected = ONE;
  BOOST_CHECK(e.compare_exchange_weak(expected, TWO));
  BOOST_CHECK(expected == ONE);
  BOOST_CHECK(e.load() == TWO);

  BOOST_CHECK(!e.compare_exchange_weak(expected, TWO));
  BOOST_CHECK(expected == TWO);
  BOOST_CHECK(e.load() == TWO);
}

BOOST_AUTO_TEST_CASE(pointer)
{
  Object one, two;
  cass::Atomic<Object*> p(&one);

  BOOST_CHECK(p.load() == &one);

  BOOST_CHECK(p.exchange(&two) == &one);
  BOOST_CHECK(p.load() == &two);

  p.store(&one);
  Object* expected = &one;
  BOOST_CHECK(p.compare_exchange_strong(expected, &two));
  BOOST_CHECK(expected == &one);
  BOOST_CHECK(p.load() == &two);

  BOOST_CHECK(!p.compare_exchange_strong(expected, &two));
  BOOST_CHECK(expected == &two);
  BOOST_CHECK(p.load() == &two);

  p.store(&one);
  expected = &one;
  BOOST_CHECK(p.compare_exchange_weak(expected, &two));
  BOOST_CHECK(expected == &one);
  BOOST_CHECK(p.load() == &two);

  BOOST_CHECK(!p.compare_exchange_weak(expected, &two));
  BOOST_CHECK(expected == &two);
  BOOST_CHECK(p.load() == &two);
}

BOOST_AUTO_TEST_CASE(boolean)
{
  cass::Atomic<bool> b(false);

  BOOST_CHECK(b.load() == false);

  BOOST_CHECK(b.exchange(true) == false);
  BOOST_CHECK(b.load() == true);

  b.store(false);
  bool expected = false;
  BOOST_CHECK(b.compare_exchange_strong(expected, true));
  BOOST_CHECK(expected == false);
  BOOST_CHECK(b.load() == true);

  BOOST_CHECK(!b.compare_exchange_strong(expected, true));
  BOOST_CHECK(expected == true);
  BOOST_CHECK(b.load() == true);

  b.store(false);
  expected = false;
  BOOST_CHECK(b.compare_exchange_weak(expected, true));
  BOOST_CHECK(expected == false);
  BOOST_CHECK(b.load() == true);

  BOOST_CHECK(!b.compare_exchange_weak(expected, true));
  BOOST_CHECK(expected == true);
  BOOST_CHECK(b.load() == true);
}

BOOST_AUTO_TEST_SUITE_END()
