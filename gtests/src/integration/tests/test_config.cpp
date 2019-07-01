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

#include "cassandra.h"

#include "driver_utils.hpp"
#include "objects/cluster.hpp"

TEST(ConfigTest, Options) {
  test::driver::Cluster cluster =
      test::driver::Cluster::build().with_connect_timeout(9999u).with_port(7000);
  EXPECT_EQ(9999u, test::driver::internals::Utils::connect_timeout(cluster.get()));
  EXPECT_EQ(7000, test::driver::internals::Utils::port(cluster.get()));
}

TEST(ConfigTest, ContactPointsSimple) {
  std::string contact_points = "127.0.0.1,127.0.0.2,127.0.0.3";
  test::driver::Cluster cluster =
      test::driver::Cluster::build().with_contact_points(contact_points);
  EXPECT_STREQ(contact_points.c_str(),
               test::driver::internals::Utils::contact_points(cluster.get()).c_str());
}

TEST(ConfigTest, ContactPointsClear) {
  std::string contact_points = "127.0.0.1,127.0.0.2,127.0.0.3";
  test::driver::Cluster cluster =
      test::driver::Cluster::build().with_contact_points(contact_points);
  EXPECT_STREQ(contact_points.c_str(),
               test::driver::internals::Utils::contact_points(cluster.get()).c_str());
  cluster.with_contact_points("");
  EXPECT_TRUE(test::driver::internals::Utils::contact_points(cluster.get()).empty());
}

TEST(ConfigTest, ContactPointsExtraCommas) {
  std::string contact_points = ",,,,127.0.0.1,,,,127.0.0.2,127.0.0.3,,,,";
  test::driver::Cluster cluster =
      test::driver::Cluster::build().with_contact_points(contact_points);
  EXPECT_STREQ("127.0.0.1,127.0.0.2,127.0.0.3",
               test::driver::internals::Utils::contact_points(cluster.get()).c_str());
}

TEST(ConfigTest, ContactPointsExtraWhitespace) {
  std::string contact_points =
      "   ,\r\n,  ,   ,  127.0.0.1 ,,,  ,\t127.0.0.2,127.0.0.3,  \t\n, ,,   ";
  test::driver::Cluster cluster =
      test::driver::Cluster::build().with_contact_points(contact_points);
  EXPECT_STREQ("127.0.0.1,127.0.0.2,127.0.0.3",
               test::driver::internals::Utils::contact_points(cluster.get()).c_str());
}

TEST(ConfigTest, ContactPointsAppend) {
  test::driver::Cluster cluster = test::driver::Cluster::build().with_contact_points("127.0.0.1");
  EXPECT_STREQ("127.0.0.1", test::driver::internals::Utils::contact_points(cluster.get()).c_str());
  cluster.with_contact_points("127.0.0.2");
  EXPECT_STREQ("127.0.0.1,127.0.0.2",
               test::driver::internals::Utils::contact_points(cluster.get()).c_str());
  cluster.with_contact_points("127.0.0.3");
  EXPECT_STREQ("127.0.0.1,127.0.0.2,127.0.0.3",
               test::driver::internals::Utils::contact_points(cluster.get()).c_str());
}
