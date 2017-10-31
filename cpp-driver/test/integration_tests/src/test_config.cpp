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

#include <boost/test/unit_test.hpp>

#include "cassandra.h"
#include "testing.hpp"
#include "test_utils.hpp"

struct ConfigTests {
  ConfigTests() { }
};

BOOST_FIXTURE_TEST_SUITE(config, ConfigTests)


BOOST_AUTO_TEST_CASE(options)
{
  test_utils::CassClusterPtr cluster(cass_cluster_new());

  {
    unsigned connect_timeout = 9999;
    cass_cluster_set_connect_timeout(cluster.get(), connect_timeout);
    BOOST_CHECK(cass::get_connect_timeout_from_cluster(cluster.get()) == connect_timeout);
  }

  {
    int port = 7000;
    cass_cluster_set_port(cluster.get(), port);
    BOOST_CHECK(cass::get_port_from_cluster(cluster.get()) == port);
  }
}

BOOST_AUTO_TEST_CASE(contact_points)
{
  test_utils::CassClusterPtr cluster(cass_cluster_new());

  // Simple
  const char* contact_points1 = "127.0.0.1,127.0.0.2,127.0.0.3";
  cass_cluster_set_contact_points(cluster.get(), contact_points1);
  BOOST_REQUIRE(cass::get_contact_points_from_cluster(cluster.get()).compare(contact_points1) == 0);

  // Clear
  cass_cluster_set_contact_points(cluster.get(), "");
  BOOST_REQUIRE(cass::get_contact_points_from_cluster(cluster.get()).empty());

  // Extra commas
  const char* contact_points1_commas = ",,,,127.0.0.1,,,,127.0.0.2,127.0.0.3,,,,";
  cass_cluster_set_contact_points(cluster.get(), contact_points1_commas);
  BOOST_REQUIRE(cass::get_contact_points_from_cluster(cluster.get()).compare(contact_points1) == 0);

  // Clear
  cass_cluster_set_contact_points(cluster.get(), "");
  BOOST_REQUIRE(cass::get_contact_points_from_cluster(cluster.get()).empty());

  // Extra whitespace
  const char* contact_points1_ws = "   ,\r\n,  ,   ,  127.0.0.1 ,,,  ,\t127.0.0.2,127.0.0.3,  \t\n, ,,   ";
  cass_cluster_set_contact_points(cluster.get(), contact_points1_ws);
  BOOST_REQUIRE(cass::get_contact_points_from_cluster(cluster.get()).compare(contact_points1) == 0);

  // Clear
  cass_cluster_set_contact_points(cluster.get(), "");
  BOOST_REQUIRE(cass::get_contact_points_from_cluster(cluster.get()).empty());

  // Append
  const char* contact_point1 = "127.0.0.1";
  cass_cluster_set_contact_points(cluster.get(), contact_point1);

  const char* contact_point2 = "127.0.0.2";
  cass_cluster_set_contact_points(cluster.get(), contact_point2);

  const char* contact_point3 = "127.0.0.3";
  cass_cluster_set_contact_points(cluster.get(), contact_point3);

  BOOST_REQUIRE(cass::get_contact_points_from_cluster(cluster.get()).compare(contact_points1) == 0);
}

BOOST_AUTO_TEST_SUITE_END()


