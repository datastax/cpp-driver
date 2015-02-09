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

#include "address.hpp"
#include "host.hpp"
#include "replication_strategy.hpp"

#include <boost/test/unit_test.hpp>

#include <vector>
#include <set>
#include <string>

static cass::SharedRefPtr<cass::Host> create_host(const std::string& ip,
                                                  const std::string& rack = "",
                                                  const std::string& dc = "") {
  cass::SharedRefPtr<cass::Host> host =
      cass::SharedRefPtr<cass::Host>(new cass::Host(cass::Address(ip, 4092), false));
  host->set_rack_and_dc(rack, dc);
  return host;
}

void check_host(const cass::SharedRefPtr<cass::Host>& host,
                const std::string& ip,
                const std::string& rack = "",
                const std::string& dc = "") {
  BOOST_CHECK(host->address() == cass::Address(ip, 4092));
  BOOST_CHECK(host->rack() == rack);
  BOOST_CHECK(host->dc() == dc);
}

BOOST_AUTO_TEST_SUITE(replication_strategy)

BOOST_AUTO_TEST_CASE(simple)
{
  cass::SimpleStrategy strategy("SimpleStrategy", 3);

  cass::TokenHostMap primary;

  cass::Token t1(1, 'f');
  cass::Token t2(1, 'l');
  cass::Token t3(1, 'r');
  cass::Token t4(1, 'z');

  primary[t1] = create_host("1.0.0.1");
  primary[t2] = create_host("1.0.0.2");
  primary[t3] = create_host("1.0.0.3");
  primary[t4] = create_host("1.0.0.4");

  cass::TokenReplicaMap replicas;
  strategy.tokens_to_replicas(primary, &replicas);

  {
    BOOST_REQUIRE(replicas.count(t1) > 0);
    const cass::CopyOnWriteHostVec& hosts = replicas.find(t1)->second;
    BOOST_REQUIRE(hosts->size() == 3);
    check_host((*hosts)[0], "1.0.0.1");
    check_host((*hosts)[1], "1.0.0.2");
    check_host((*hosts)[2], "1.0.0.3");
  }

  {
    BOOST_REQUIRE(replicas.count(t2) > 0);
    const cass::CopyOnWriteHostVec& hosts = replicas.find(t2)->second;
    BOOST_REQUIRE(hosts->size() == 3);
    check_host((*hosts)[0], "1.0.0.2");
    check_host((*hosts)[1], "1.0.0.3");
    check_host((*hosts)[2], "1.0.0.4");
  }

  {
    BOOST_REQUIRE(replicas.count(t3) > 0);
    const cass::CopyOnWriteHostVec& hosts = replicas.find(t3)->second;
    BOOST_REQUIRE(hosts->size() == 3);
    check_host((*hosts)[0], "1.0.0.3");
    check_host((*hosts)[1], "1.0.0.4");
    check_host((*hosts)[2], "1.0.0.1");
  }

  {
    BOOST_REQUIRE(replicas.count(t4) > 0);
    const cass::CopyOnWriteHostVec& hosts = replicas.find(t4)->second;
    BOOST_REQUIRE(hosts->size() == 3);
    check_host((*hosts)[0], "1.0.0.4");
    check_host((*hosts)[1], "1.0.0.1");
    check_host((*hosts)[2], "1.0.0.2");
  }
}

BOOST_AUTO_TEST_CASE(network_topology)
{
  cass::NetworkTopologyStrategy::DCReplicaCountMap dc_replicas;
  dc_replicas["dc1"] = 2;
  dc_replicas["dc2"] = 2;

  cass::NetworkTopologyStrategy strategy("NetworkTopologyStrategy", dc_replicas);

  cass::TokenHostMap primary;

  cass::Token t1(1, 'c');
  cass::Token t2(1, 'f');
  cass::Token t3(1, 'i');
  cass::Token t4(1, 'l');

  primary[t1] = create_host("1.0.0.1", "rack1", "dc1");
  primary[t2] = create_host("1.0.0.2", "rack1", "dc1");
  primary[t3] = create_host("1.0.0.3", "rack2", "dc1");
  primary[t4] = create_host("1.0.0.4", "rack2", "dc1");

  cass::Token t5(1, 'o');
  cass::Token t6(1, 'r');
  cass::Token t7(1, 'u');
  cass::Token t8(1, 'z');

  primary[t5] = create_host("2.0.0.1", "rack1", "dc2");
  primary[t6] = create_host("2.0.0.2", "rack1", "dc2");
  primary[t7] = create_host("2.0.0.3", "rack2", "dc2");
  primary[t8] = create_host("2.0.0.4", "rack2", "dc2");

  cass::TokenReplicaMap replicas;
  strategy.tokens_to_replicas(primary, &replicas);

  {
    const cass::CopyOnWriteHostVec& hosts = replicas.find(t1)->second;
    BOOST_REQUIRE(hosts->size() == 4);
    check_host((*hosts)[0], "1.0.0.1", "rack1", "dc1");
    check_host((*hosts)[1], "1.0.0.3", "rack2", "dc1");
    check_host((*hosts)[2], "2.0.0.1", "rack1", "dc2");
    check_host((*hosts)[3], "2.0.0.3", "rack2", "dc2");
  }

  {
    const cass::CopyOnWriteHostVec& hosts = replicas.find(t2)->second;
    BOOST_REQUIRE(hosts->size() == 4);
    check_host((*hosts)[0], "1.0.0.2", "rack1", "dc1");
    check_host((*hosts)[1], "1.0.0.3", "rack2", "dc1");
    check_host((*hosts)[2], "2.0.0.1", "rack1", "dc2");
    check_host((*hosts)[3], "2.0.0.3", "rack2", "dc2");
  }

  {
    const cass::CopyOnWriteHostVec& hosts = replicas.find(t3)->second;
    BOOST_REQUIRE(hosts->size() == 4);
    check_host((*hosts)[0], "1.0.0.3", "rack2", "dc1");
    check_host((*hosts)[1], "2.0.0.1", "rack1", "dc2");
    check_host((*hosts)[2], "2.0.0.3", "rack2", "dc2");
    check_host((*hosts)[3], "1.0.0.1", "rack1", "dc1");
  }

  {
    const cass::CopyOnWriteHostVec& hosts = replicas.find(t4)->second;
    BOOST_REQUIRE(hosts->size() == 4);
    check_host((*hosts)[0], "1.0.0.4", "rack2", "dc1");
    check_host((*hosts)[1], "2.0.0.1", "rack1", "dc2");
    check_host((*hosts)[2], "2.0.0.3", "rack2", "dc2");
    check_host((*hosts)[3], "1.0.0.1", "rack1", "dc1");
  }

  {
    const cass::CopyOnWriteHostVec& hosts = replicas.find(t5)->second;
    BOOST_REQUIRE(hosts->size() == 4);
    check_host((*hosts)[0], "2.0.0.1", "rack1", "dc2");
    check_host((*hosts)[1], "2.0.0.3", "rack2", "dc2");
    check_host((*hosts)[2], "1.0.0.1", "rack1", "dc1");
    check_host((*hosts)[3], "1.0.0.3", "rack2", "dc1");
  }

  {
    const cass::CopyOnWriteHostVec& hosts = replicas.find(t6)->second;
    BOOST_REQUIRE(hosts->size() == 4);
    check_host((*hosts)[0], "2.0.0.2", "rack1", "dc2");
    check_host((*hosts)[1], "2.0.0.3", "rack2", "dc2");
    check_host((*hosts)[2], "1.0.0.1", "rack1", "dc1");
    check_host((*hosts)[3], "1.0.0.3", "rack2", "dc1");
  }

  {
    const cass::CopyOnWriteHostVec& hosts = replicas.find(t7)->second;
    BOOST_REQUIRE(hosts->size() == 4);
    check_host((*hosts)[0], "2.0.0.3", "rack2", "dc2");
    check_host((*hosts)[1], "1.0.0.1", "rack1", "dc1");
    check_host((*hosts)[2], "1.0.0.3", "rack2", "dc1");
    check_host((*hosts)[3], "2.0.0.1", "rack1", "dc2");
  }

  {
    const cass::CopyOnWriteHostVec& hosts = replicas.find(t8)->second;
    BOOST_REQUIRE(hosts->size() == 4);
    check_host((*hosts)[0], "2.0.0.4", "rack2", "dc2");
    check_host((*hosts)[1], "1.0.0.1", "rack1", "dc1");
    check_host((*hosts)[2], "1.0.0.3", "rack2", "dc1");
    check_host((*hosts)[3], "2.0.0.1", "rack1", "dc2");
  }
}

BOOST_AUTO_TEST_CASE(network_topology_same_rack)
{
  cass::NetworkTopologyStrategy::DCReplicaCountMap dc_replicas;
  dc_replicas["dc1"] = 2;
  dc_replicas["dc2"] = 1;

  cass::NetworkTopologyStrategy strategy("NetworkTopologyStrategy", dc_replicas);

  cass::TokenHostMap primary;

  cass::Token t1(1, 'd');
  cass::Token t2(1, 'h');
  cass::Token t3(1, 'l');

  primary[t1] = create_host("1.0.0.1", "rack1", "dc1");
  primary[t2] = create_host("1.0.0.2", "rack1", "dc1");
  primary[t3] = create_host("1.0.0.3", "rack1", "dc1");

  cass::Token t4(1, 'p');
  cass::Token t5(1, 't');
  cass::Token t6(1, 'z');

  primary[t4] = create_host("2.0.0.1", "rack1", "dc2");
  primary[t5] = create_host("2.0.0.2", "rack1", "dc2");
  primary[t6] = create_host("2.0.0.3", "rack1", "dc2");

  cass::TokenReplicaMap replicas;
  strategy.tokens_to_replicas(primary, &replicas);

  {
    const cass::CopyOnWriteHostVec& hosts = replicas.find(t1)->second;
    BOOST_REQUIRE(hosts->size() == 3);
    check_host((*hosts)[0], "1.0.0.1", "rack1", "dc1");
    check_host((*hosts)[1], "1.0.0.2", "rack1", "dc1");
    check_host((*hosts)[2], "2.0.0.1", "rack1", "dc2");
  }

  {
    const cass::CopyOnWriteHostVec& hosts = replicas.find(t2)->second;
    BOOST_REQUIRE(hosts->size() == 3);
    check_host((*hosts)[0], "1.0.0.2", "rack1", "dc1");
    check_host((*hosts)[1], "1.0.0.3", "rack1", "dc1");
    check_host((*hosts)[2], "2.0.0.1", "rack1", "dc2");
  }

  {
    const cass::CopyOnWriteHostVec& hosts = replicas.find(t3)->second;
    BOOST_REQUIRE(hosts->size() == 3);
    check_host((*hosts)[0], "1.0.0.3", "rack1", "dc1");
    check_host((*hosts)[1], "2.0.0.1", "rack1", "dc2");
    check_host((*hosts)[2], "1.0.0.1", "rack1", "dc1");
  }

  {
    const cass::CopyOnWriteHostVec& hosts = replicas.find(t4)->second;
    BOOST_REQUIRE(hosts->size() == 3);
    check_host((*hosts)[0], "2.0.0.1", "rack1", "dc2");
    check_host((*hosts)[1], "1.0.0.1", "rack1", "dc1");
    check_host((*hosts)[2], "1.0.0.2", "rack1", "dc1");
  }

  {
    const cass::CopyOnWriteHostVec& hosts = replicas.find(t5)->second;
    BOOST_REQUIRE(hosts->size() == 3);
    check_host((*hosts)[0], "2.0.0.2", "rack1", "dc2");
    check_host((*hosts)[1], "1.0.0.1", "rack1", "dc1");
    check_host((*hosts)[2], "1.0.0.2", "rack1", "dc1");
  }

  {
    const cass::CopyOnWriteHostVec& hosts = replicas.find(t6)->second;
    BOOST_REQUIRE(hosts->size() == 3);
    check_host((*hosts)[0], "2.0.0.3", "rack1", "dc2");
    check_host((*hosts)[1], "1.0.0.1", "rack1", "dc1");
    check_host((*hosts)[2], "1.0.0.2", "rack1", "dc1");
  }
}

BOOST_AUTO_TEST_CASE(network_topology_not_enough_racks)
{
  cass::NetworkTopologyStrategy::DCReplicaCountMap dc_replicas;
  dc_replicas["dc1"] = 3;

  cass::NetworkTopologyStrategy strategy("NetworkTopologyStrategy", dc_replicas);

  cass::TokenHostMap primary;

  cass::Token t1(1, 'd');
  cass::Token t2(1, 'h');
  cass::Token t3(1, 'l');
  cass::Token t4(1, 'p');

  primary[t1] = create_host("1.0.0.1", "rack1", "dc1");
  primary[t2] = create_host("1.0.0.2", "rack1", "dc1");
  primary[t3] = create_host("1.0.0.3", "rack1", "dc1");
  primary[t4] = create_host("1.0.0.4", "rack2", "dc1");

  cass::TokenReplicaMap replicas;
  strategy.tokens_to_replicas(primary, &replicas);

  {
    const cass::CopyOnWriteHostVec& hosts = replicas.find(t1)->second;
    BOOST_REQUIRE(hosts->size() == 3);
    check_host((*hosts)[0], "1.0.0.1", "rack1", "dc1");
    check_host((*hosts)[1], "1.0.0.4", "rack2", "dc1");
    check_host((*hosts)[2], "1.0.0.2", "rack1", "dc1");
  }

  {
    const cass::CopyOnWriteHostVec& hosts = replicas.find(t2)->second;
    BOOST_REQUIRE(hosts->size() == 3);
    check_host((*hosts)[0], "1.0.0.2", "rack1", "dc1");
    check_host((*hosts)[1], "1.0.0.4", "rack2", "dc1");
    check_host((*hosts)[2], "1.0.0.3", "rack1", "dc1");
  }

  {
    const cass::CopyOnWriteHostVec& hosts = replicas.find(t3)->second;
    BOOST_REQUIRE(hosts->size() == 3);
    check_host((*hosts)[0], "1.0.0.3", "rack1", "dc1");
    check_host((*hosts)[1], "1.0.0.4", "rack2", "dc1");
    check_host((*hosts)[2], "1.0.0.1", "rack1", "dc1");
  }

  {
    const cass::CopyOnWriteHostVec& hosts = replicas.find(t4)->second;
    BOOST_REQUIRE(hosts->size() == 3);
    check_host((*hosts)[0], "1.0.0.4", "rack2", "dc1");
    check_host((*hosts)[1], "1.0.0.1", "rack1", "dc1");
    check_host((*hosts)[2], "1.0.0.2", "rack1", "dc1");
  }
}

BOOST_AUTO_TEST_SUITE_END()
