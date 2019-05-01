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

#include "small_vector.hpp"

using datastax::internal::SmallVector;

TEST(SmallVectorUnitTest, Simple) {
  SmallVector<int, 5> vec;
  EXPECT_EQ(vec.fixed().data.address(), vec.data());
  EXPECT_EQ(vec.fixed().is_used, true);

  for (int i = 0; i < 5; ++i) {
    vec.push_back(i);
    EXPECT_EQ(vec.fixed().data.address(), vec.data());
    EXPECT_EQ(vec.fixed().is_used, true);
  }

  vec.push_back(5);
  EXPECT_NE(vec.fixed().data.address(), vec.data());
  EXPECT_EQ(vec.fixed().is_used, false);
}
