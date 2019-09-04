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
using datastax::internal::core::AddressSet;

TEST(AddressUnitTest, FromString) {
  EXPECT_TRUE(Address("127.0.0.1", 9042).is_resolved());
  EXPECT_TRUE(Address("0.0.0.0", 9042).is_resolved());
  EXPECT_TRUE(Address("::", 9042).is_resolved());
  EXPECT_TRUE(Address("::1", 9042).is_resolved());
  EXPECT_TRUE(Address("2001:0db8:85a3:0000:0000:8a2e:0370:7334", 9042).is_resolved());

  EXPECT_FALSE(Address().is_resolved());
  EXPECT_FALSE(Address("localhost", 9042).is_resolved());
  EXPECT_FALSE(Address("datastax.com", 9042).is_resolved());
}

TEST(AddressUnitTest, CompareIPv4) {
  EXPECT_LT(Address("0.0.0.0", 9042), Address("255.255.255.255", 9042));
  EXPECT_EQ(Address("1.2.3.4", 9042), Address("1.2.3.4", 9042));
  EXPECT_NE(Address("1.2.3.4", 9042), Address("5.6.7.8", 9042));

  EXPECT_LT(Address("0.0.0.0", 9041), Address("0.0.0.0", 9042));
  EXPECT_NE(Address("0.0.0.0", 9041), Address("0.0.0.0", 9042));

  // Without comparing port
  EXPECT_TRUE(Address("0.0.0.0", 9041).equals(Address("0.0.0.0", 9042), false));
  EXPECT_FALSE(Address("127.0.0.1", 9042).equals(Address("0.0.0.0", 9042), false));
}

TEST(AddressUnitTest, CompareIPv6) {
  EXPECT_LT(Address("0:0:0:0:0:0:0:0", 9042), Address("0:0:0:0:0:0:0:FFFF", 9042));
  EXPECT_EQ(Address("0:0:0:0:0:0:0:1234", 9042), Address("0:0:0:0:0:0:0:1234", 9042));
  EXPECT_NE(Address("0:0:0:0:0:0:0:1234", 9042), Address("0:0:0:0:0:0:0:5678", 9042));

  EXPECT_LT(Address("0:0:0:0:0:0:0:0", 9041), Address("0:0:0:0:0:0:0:0", 9042));
  EXPECT_NE(Address("0:0:0:0:0:0:0:0", 9041), Address("0:0:0:0:0:0:0:0", 9042));

  // Without comparing port
  EXPECT_TRUE(Address("::", 9041).equals(Address("::", 9042), false));
  EXPECT_FALSE(Address("::1", 9042).equals(Address("::", 9042), false));

  EXPECT_EQ(Address("0:0:0:0:0:0:0:0", 9042), Address("::", 9042)); // Normalization
}

TEST(AddressUnitTest, ToSockAddrIPv4) {
  Address expected("127.0.0.1", 9042);
  Address::SocketStorage storage;
  Address actual(expected.to_sockaddr(&storage));
  EXPECT_EQ(expected, actual);
}

TEST(AddressUnitTest, ToSockAddrIPv6) {
  Address expected("::1", 9042);
  Address::SocketStorage storage;
  Address actual(expected.to_sockaddr(&storage));
  EXPECT_EQ(expected, actual);
}

TEST(AddressUnitTest, ToInetIPv4) {
  Address expected("127.0.0.1", 9042);

  uint8_t inet_address[4];
  uint8_t inet_address_length = expected.to_inet(inet_address);
  EXPECT_EQ(inet_address_length, 4u);

  Address actual(inet_address, inet_address_length, 9042);
  EXPECT_EQ(expected, actual);
}

TEST(AddressUnitTest, ToInetIPv6) {
  Address expected("::1", 9042);

  uint8_t inet_address[16];
  uint8_t inet_address_length = expected.to_inet(inet_address);
  EXPECT_EQ(inet_address_length, 16u);

  Address actual(inet_address, inet_address_length, 9042);
  EXPECT_EQ(expected, actual);
}

TEST(AddressUnitTest, ToString) {
  // Only hostname/address
  EXPECT_EQ(Address("127.0.0.1", 9042).hostname_or_address(), "127.0.0.1");
  EXPECT_EQ(Address("::1", 9042).hostname_or_address(), "::1");
  EXPECT_EQ(Address("0:0:0:0:0:0:0:1", 9042).hostname_or_address(), "::1"); // IPv6 normalization
  EXPECT_EQ(Address("0:0:0:0:0:0:0:0", 9042).hostname_or_address(), "::");  // IPv6 normalization
  EXPECT_EQ(Address("datastax.com", 9042).hostname_or_address(), "datastax.com");

  // w/o port
  EXPECT_EQ(Address("127.0.0.1", 9042).to_string(), "127.0.0.1");
  EXPECT_EQ(Address("::1", 9042).to_string(), "::1");
  EXPECT_EQ(Address("datastax.com", 9042).to_string(), "datastax.com");

  // w/ port
  EXPECT_EQ(Address("127.0.0.1", 9042).to_string(true), "127.0.0.1:9042");
  EXPECT_EQ(Address("::1", 9042).to_string(true), "[::1]:9042");
  EXPECT_EQ(Address("datastax.com", 9042).to_string(true), "datastax.com:9042");

  // w/ servername
  EXPECT_EQ(Address("127.0.0.1", 9042, "d1f1884b-6e05-4b3f-9e88-8a93904bb0e5").to_string(),
            "127.0.0.1 (d1f1884b-6e05-4b3f-9e88-8a93904bb0e5)");
  EXPECT_EQ(Address("::1", 9042, "d1f1884b-6e05-4b3f-9e88-8a93904bb0e5").to_string(),
            "::1 (d1f1884b-6e05-4b3f-9e88-8a93904bb0e5)");
  EXPECT_EQ(Address("datastax.com", 9042, "d1f1884b-6e05-4b3f-9e88-8a93904bb0e5").to_string(),
            "datastax.com (d1f1884b-6e05-4b3f-9e88-8a93904bb0e5)");

  // w/ servername and port
  EXPECT_EQ(Address("127.0.0.1", 9042, "d1f1884b-6e05-4b3f-9e88-8a93904bb0e5").to_string(true),
            "127.0.0.1:9042 (d1f1884b-6e05-4b3f-9e88-8a93904bb0e5)");
  EXPECT_EQ(Address("::1", 9042, "d1f1884b-6e05-4b3f-9e88-8a93904bb0e5").to_string(true),
            "[::1]:9042 (d1f1884b-6e05-4b3f-9e88-8a93904bb0e5)");
  EXPECT_EQ(Address("datastax.com", 9042, "d1f1884b-6e05-4b3f-9e88-8a93904bb0e5").to_string(true),
            "datastax.com:9042 (d1f1884b-6e05-4b3f-9e88-8a93904bb0e5)");
}

TEST(AddressUnitTest, Hash) {
  AddressSet set;

  EXPECT_EQ(set.size(), 0u); // Empty

  set.insert(Address("0.0.0.0", 9042));
  EXPECT_EQ(set.size(), 1u); // Added

  // Reinsert
  set.insert(Address("0.0.0.0", 9042));
  EXPECT_EQ(set.size(), 1u); // No change

  // Remove
  set.erase(Address("0.0.0.0", 9042));
  EXPECT_EQ(set.size(), 0u); // Removed

  // Multiple
  set.insert(Address("0.0.0.0", 9042));
  set.insert(Address("127.0.0.1", 9042));
  set.insert(Address("localhost", 9042));
  set.insert(Address("::1", 9042));
  EXPECT_EQ(set.size(), 4u); // Added
  EXPECT_EQ(set.count(Address("0.0.0.0", 9042)), 1u);
  EXPECT_EQ(set.count(Address("127.0.0.1", 9042)), 1u);
  EXPECT_EQ(set.count(Address("localhost", 9042)), 1u);
  EXPECT_EQ(set.count(Address("::1", 9042)), 1u);

  // Different port
  set.insert(Address("0.0.0.0", 9041));
  EXPECT_EQ(set.size(), 5u); // Added
}

TEST(AddressUnitTest, StrictWeakOrder) {
  { // Family
    Address a("localhost", 9042);
    Address b("127.0.0.1", 30002, "a");
    ASSERT_NE(a, b);
    ASSERT_TRUE(a < b);
    ASSERT_FALSE(b < a);
  }

  { // Port
    Address a("localhost", 9042, "b");
    Address b("localhost", 30002, "a");
    ASSERT_NE(a, b);
    ASSERT_TRUE(a < b);
    ASSERT_FALSE(b < a);
  }

  { // Server name
    Address a("127.0.0.2", 9042, "a");
    Address b("127.0.0.1", 9042, "b");
    ASSERT_NE(a, b);
    ASSERT_TRUE(a < b);
    ASSERT_FALSE(b < a);
  }

  { // Hostname or address
    Address a("127.0.0.1", 9042, "a");
    Address b("127.0.0.2", 9042, "a");
    ASSERT_NE(a, b);
    ASSERT_TRUE(a < b);
    ASSERT_FALSE(b < a);
  }
}
