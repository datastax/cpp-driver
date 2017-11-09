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

TEST(AddressUnitTest, CompareIPv4) {
  EXPECT_GT(cass::Address("255.255.255.255", 9042).compare(cass::Address("0.0.0.0", 9042)), 0);
  EXPECT_LT(cass::Address("0.0.0.0", 9042).compare(cass::Address("255.255.255.255", 9042)), 0);
  EXPECT_EQ(cass::Address("1.2.3.4", 9042).compare(cass::Address("1.2.3.4", 9042)), 0);
}

TEST(AddressUnitTest, CompareIPv6) {
  EXPECT_GT(cass::Address("0.0.0.0", 1).compare(cass::Address("0.0.0.0", 0), true), 0);
  EXPECT_LT(cass::Address("0.0.0.0", 0).compare(cass::Address("0.0.0.0", 1), true), 0);
  EXPECT_EQ(cass::Address("0.0.0.0", 0).compare(cass::Address("0.0.0.0", 1), false), 0);
}
