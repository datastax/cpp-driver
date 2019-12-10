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

#include "copy_on_write_ptr.hpp"
#include "map.hpp"
#include "ref_counted.hpp"
#include "string.hpp"
#include "vector.hpp"

using namespace datastax::internal;
using namespace datastax::internal::core;

TEST(CopyOnWriteUnitTest, Simple) {
  Vector<int>* ptr = new Vector<int>();
  CopyOnWritePtr<Vector<int> > vec(ptr);

  // Only a single reference so no copy should be made
  EXPECT_EQ(static_cast<const CopyOnWritePtr<Vector<int> >&>(vec).operator->(), ptr);
  vec->push_back(1);
  EXPECT_EQ(static_cast<const CopyOnWritePtr<Vector<int> >&>(vec).operator->(), ptr);

  // Make const reference to object
  const CopyOnWritePtr<Vector<int> > const_vec(vec);
  EXPECT_EQ((*const_vec)[0], 1);
  EXPECT_EQ(const_vec.operator->(), ptr);

  // Force copy to be made
  vec->push_back(2);
  EXPECT_NE(static_cast<const CopyOnWritePtr<Vector<int> >&>(vec).operator->(), ptr);
  EXPECT_EQ(const_vec.operator->(), ptr);
}
