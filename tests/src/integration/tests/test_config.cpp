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

#include "integration.hpp"
#include "testing.hpp"

class ConfigTests : public Integration {
public:
  ConfigTests() { Integration::SetUp(); }
};

CASSANDRA_INTEGRATION_TEST_F(ConfigTests, Options) {
  test::driver::Cluster cluster =
      test::driver::Cluster::build().with_connect_timeout(9999u).with_port(7000);
  EXPECT_EQ(9999u, datastax::internal::testing::get_connect_timeout_from_cluster(cluster.get()));
  EXPECT_EQ(7000, datastax::internal::testing::get_port_from_cluster(cluster.get()));
}

CASSANDRA_INTEGRATION_TEST_F(ConfigTests, ContactPointsSimple) {
  std::string contact_points = "127.0.0.1,127.0.0.2,127.0.0.3";
  test::driver::Cluster cluster =
      test::driver::Cluster::build().with_contact_points(contact_points);
  EXPECT_STREQ(contact_points.c_str(),
               datastax::internal::testing::get_contact_points_from_cluster(cluster.get()).c_str());
}

CASSANDRA_INTEGRATION_TEST_F(ConfigTests, ContactPointsClear) {
  std::string contact_points = "127.0.0.1,127.0.0.2,127.0.0.3";
  test::driver::Cluster cluster =
      test::driver::Cluster::build().with_contact_points(contact_points);
  EXPECT_STREQ(contact_points.c_str(),
               datastax::internal::testing::get_contact_points_from_cluster(cluster.get()).c_str());
  cluster.with_contact_points("");
  EXPECT_TRUE(datastax::internal::testing::get_contact_points_from_cluster(cluster.get()).empty());
}

CASSANDRA_INTEGRATION_TEST_F(ConfigTests, ContactPointsExtraCommas) {
  std::string contact_points = ",,,,127.0.0.1,,,,127.0.0.2,127.0.0.3,,,,";
  test::driver::Cluster cluster =
      test::driver::Cluster::build().with_contact_points(contact_points);
  EXPECT_STREQ("127.0.0.1,127.0.0.2,127.0.0.3",
               datastax::internal::testing::get_contact_points_from_cluster(cluster.get()).c_str());
}

CASSANDRA_INTEGRATION_TEST_F(ConfigTests, ContactPointsExtraWhitespace) {
  std::string contact_points =
      "   ,\r\n,  ,   ,  127.0.0.1 ,,,  ,\t127.0.0.2,127.0.0.3,  \t\n, ,,   ";
  test::driver::Cluster cluster =
      test::driver::Cluster::build().with_contact_points(contact_points);
  EXPECT_STREQ("127.0.0.1,127.0.0.2,127.0.0.3",
               datastax::internal::testing::get_contact_points_from_cluster(cluster.get()).c_str());
}

CASSANDRA_INTEGRATION_TEST_F(ConfigTests, ContactPointsAppend) {
  test::driver::Cluster cluster = test::driver::Cluster::build().with_contact_points("127.0.0.1");
  EXPECT_STREQ("127.0.0.1",
               datastax::internal::testing::get_contact_points_from_cluster(cluster.get()).c_str());
  cluster.with_contact_points("127.0.0.2");
  EXPECT_STREQ("127.0.0.1,127.0.0.2",
               datastax::internal::testing::get_contact_points_from_cluster(cluster.get()).c_str());
  cluster.with_contact_points("127.0.0.3");
  EXPECT_STREQ("127.0.0.1,127.0.0.2,127.0.0.3",
               datastax::internal::testing::get_contact_points_from_cluster(cluster.get()).c_str());
}
