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

#include "utils.hpp"

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


TEST(IsConvertibleUnitTest, Simple) {
  EXPECT_TRUE((cass::IsConvertible<int, int>::value));
  EXPECT_TRUE((cass::IsConvertible<Enum, Enum>::value));
  EXPECT_TRUE((cass::IsConvertible<Struct, Struct>::value));
  EXPECT_TRUE((cass::IsConvertible<Derived*, Base*>::value));
  EXPECT_TRUE((cass::IsConvertible<Enum, int>::value));

  EXPECT_FALSE((cass::IsConvertible<Base*, Derived*>::value));
  EXPECT_FALSE((cass::IsConvertible<int, Enum>::value));
  EXPECT_FALSE((cass::IsConvertible<Struct, Base>::value));
  EXPECT_FALSE((cass::IsConvertible<Struct*, Base*>::value));
}
