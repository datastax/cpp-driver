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

#include "address.hpp"

using datastax::internal::core::Address;

TEST(AddressUnitTest, CompareIPv4) {
  EXPECT_GT(Address("255.255.255.255", 9042).compare(Address("0.0.0.0", 9042)), 0);
  EXPECT_LT(Address("0.0.0.0", 9042).compare(Address("255.255.255.255", 9042)), 0);
  EXPECT_EQ(Address("1.2.3.4", 9042).compare(Address("1.2.3.4", 9042)), 0);
}

TEST(AddressUnitTest, CompareIPv6) {
  EXPECT_GT(Address("0.0.0.0", 1).compare(Address("0.0.0.0", 0), true), 0);
  EXPECT_LT(Address("0.0.0.0", 0).compare(Address("0.0.0.0", 1), true), 0);
  EXPECT_EQ(Address("0.0.0.0", 0).compare(Address("0.0.0.0", 1), false), 0);
}
