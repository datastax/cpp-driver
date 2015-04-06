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

#include "common.hpp"

#include <boost/test/unit_test.hpp>

enum Enum {
  ONE, TWO, THREE
};

struct Struct {
  int one, two, three;
};

class Base {
public:
  int one;
};

class Derived : public Base {
public:
  int two, three;
};

BOOST_AUTO_TEST_SUITE(is_convertible)

BOOST_AUTO_TEST_CASE(simple)
{
  BOOST_CHECK((cass::IsConvertible<int, int>::value));
  BOOST_CHECK((cass::IsConvertible<Enum, Enum>::value));
  BOOST_CHECK((cass::IsConvertible<Struct, Struct>::value));
  BOOST_CHECK((cass::IsConvertible<Derived*, Base*>::value));
  BOOST_CHECK((cass::IsConvertible<Enum, int>::value));

  BOOST_CHECK(!(cass::IsConvertible<Base*, Derived*>::value));
  BOOST_CHECK(!(cass::IsConvertible<int, Enum>::value));
  BOOST_CHECK(!(cass::IsConvertible<Struct, Base>::value));
  BOOST_CHECK(!(cass::IsConvertible<Struct*, Base*>::value));
}

BOOST_AUTO_TEST_SUITE_END()
