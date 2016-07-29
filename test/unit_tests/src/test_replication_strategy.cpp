/*
  Copyright (c) 2014-2016 DataStax

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

#include "test_token_map_utils.hpp"

#include <boost/test/unit_test.hpp>

#include <vector>
#include <set>
#include <string>
#include <sstream>

namespace {

static const cass::CopyOnWriteHostVec NO_REPLICAS(NULL);

template <class Partitioner>
struct MockTokenMap {
  typedef typename cass::ReplicationStrategy<Partitioner>::Token Token;
  typedef typename cass::ReplicationStrategy<Partitioner>::TokenHost TokenHost;
  typedef typename cass::ReplicationStrategy<Partitioner>::TokenHostVec TokenHostVec;
  typedef typename cass::ReplicationStrategy<Partitioner>::TokenReplicas TokenReplicas;
  typedef typename cass::ReplicationStrategy<Partitioner>::TokenReplicasVec TokenReplicasVec;

  struct TokenReplicasCompare {
    bool operator()(const TokenReplicas& lhs, const TokenReplicas& rhs) const {
      return lhs.first < rhs.first;
    }
  };

  cass::HostSet hosts;
  cass::IdGenerator dc_ids;
  cass::IdGenerator rack_ids;

  cass::ReplicationStrategy<Partitioner> strategy;
  TokenHostVec tokens;
  TokenReplicasVec replicas;
  cass::DatacenterMap datacenters;

  void init_simple_strategy(size_t replication_factor) {
    cass::DataType::ConstPtr varchar_data_type(new cass::DataType(CASS_VALUE_TYPE_VARCHAR));

    ColumnMetadataVec column_metadata;
    column_metadata.push_back(ColumnMetadata("keyspace_name", varchar_data_type));
    column_metadata.push_back(ColumnMetadata("replication", cass::CollectionType::map(varchar_data_type, varchar_data_type, true)));
    RowResultResponseBuilder builder(column_metadata);

    ReplicationMap replication;
    replication["class"] = CASS_SIMPLE_STRATEGY;

    std::stringstream ss;
    ss << replication_factor;
    replication["replication_factor"] = ss.str();

    builder.append_keyspace_row_v3("ks1", replication);
    builder.finish();

    cass::ResultIterator iterator(builder.finish());
    BOOST_CHECK(iterator.next());
    strategy.init(dc_ids, cass::VersionNumber(3, 0, 0), iterator.row());
  }

  void init_network_topology_strategy(ReplicationMap& replication) {
    cass::DataType::ConstPtr varchar_data_type(new cass::DataType(CASS_VALUE_TYPE_VARCHAR));

    ColumnMetadataVec column_metadata;
    column_metadata.push_back(ColumnMetadata("keyspace_name", varchar_data_type));
    column_metadata.push_back(ColumnMetadata("replication", cass::CollectionType::map(varchar_data_type, varchar_data_type, true)));
    RowResultResponseBuilder builder(column_metadata);

    replication["class"] = CASS_NETWORK_TOPOLOGY_STRATEGY;
    builder.append_keyspace_row_v3("ks1", replication);
    builder.finish();

    cass::ResultIterator iterator(builder.finish());
    BOOST_CHECK(iterator.next());
    strategy.init(dc_ids, cass::VersionNumber(3, 0, 0), iterator.row());
  }

  void add_token(Token token,
                 const std::string& address,
                 const std::string& rack = "",
                 const std::string& dc = "") {
    tokens.push_back(TokenHost(token, create_host(address, rack, dc)));
  }

  void build_replicas() {
    std::sort(tokens.begin(), tokens.end()); // We assume sorted tokens
    cass::build_datacenters(hosts, datacenters);
    strategy.build_replicas(tokens, datacenters, replicas);
  }


  const cass::CopyOnWriteHostVec& find_hosts(Token token) {
    typename TokenReplicasVec::const_iterator i = std::lower_bound(replicas.begin(), replicas.end(),
                                                                   TokenReplicas(token, NO_REPLICAS),
                                                                   TokenReplicasCompare());
    if (i != replicas.end() && i->first == token) {
      return i->second;
    }
    return NO_REPLICAS;
  }

  cass::Host* create_host(const std::string& address,
                          const std::string& rack = "",
                          const std::string& dc = "") {
    cass::Host::Ptr host(new cass::Host(cass::Address(address, 9042), false));
    host->set_rack_and_dc(rack, dc);
    host->set_rack_and_dc_ids(rack_ids.get(rack), dc_ids.get(dc));
    cass::HostSet::iterator i = hosts.find(host);
    if (i != hosts.end()) {
      return i->get();
    } else {
      hosts.insert(host);
      return host.get();
    }
  }
};

void check_host(const cass::SharedRefPtr<cass::Host>& host,
                const std::string& ip,
                const std::string& rack = "",
                const std::string& dc = "") {
  BOOST_CHECK(host->address() == cass::Address(ip, 9042));
  BOOST_CHECK(host->rack() == rack);
  BOOST_CHECK(host->dc() == dc);
}

} // namespace

BOOST_AUTO_TEST_SUITE(replication_strategy)

BOOST_AUTO_TEST_CASE(simple)
{
  MockTokenMap<cass::Murmur3Partitioner> token_map;

  token_map.init_simple_strategy(3);

  MockTokenMap<cass::Murmur3Partitioner>::Token t1 = 0;
  MockTokenMap<cass::Murmur3Partitioner>::Token t2 = 100;
  MockTokenMap<cass::Murmur3Partitioner>::Token t3 = 200;
  MockTokenMap<cass::Murmur3Partitioner>::Token t4 = 300;

  token_map.add_token(t1, "1.0.0.1");
  token_map.add_token(t2, "1.0.0.2");
  token_map.add_token(t3, "1.0.0.3");
  token_map.add_token(t4, "1.0.0.4");

  token_map.build_replicas();

  {
    const cass::CopyOnWriteHostVec& hosts = token_map.find_hosts(t1);
    BOOST_REQUIRE(hosts && hosts->size() == 3);
    check_host((*hosts)[0], "1.0.0.1");
    check_host((*hosts)[1], "1.0.0.2");
    check_host((*hosts)[2], "1.0.0.3");
  }

  {
    const cass::CopyOnWriteHostVec& hosts = token_map.find_hosts(t2);
    check_host((*hosts)[0], "1.0.0.2");
    check_host((*hosts)[1], "1.0.0.3");
    check_host((*hosts)[2], "1.0.0.4");
  }

  {
    const cass::CopyOnWriteHostVec& hosts = token_map.find_hosts(t3);
    check_host((*hosts)[0], "1.0.0.3");
    check_host((*hosts)[1], "1.0.0.4");
    check_host((*hosts)[2], "1.0.0.1");
  }

  {
    const cass::CopyOnWriteHostVec& hosts = token_map.find_hosts(t4);
    check_host((*hosts)[0], "1.0.0.4");
    check_host((*hosts)[1], "1.0.0.1");
    check_host((*hosts)[2], "1.0.0.2");
  }
}

BOOST_AUTO_TEST_CASE(network_topology)
{
  MockTokenMap<cass::Murmur3Partitioner> token_map;

  ReplicationMap replication;
  replication["dc1"] = "2";
  replication["dc2"] = "2";

  token_map.init_network_topology_strategy(replication);

  MockTokenMap<cass::Murmur3Partitioner>::Token t1 = 0;
  MockTokenMap<cass::Murmur3Partitioner>::Token t2 = 100;
  MockTokenMap<cass::Murmur3Partitioner>::Token t3 = 200;
  MockTokenMap<cass::Murmur3Partitioner>::Token t4 = 300;

  token_map.add_token(t1, "1.0.0.1", "rack1", "dc1");
  token_map.add_token(t2, "1.0.0.2", "rack1", "dc1");
  token_map.add_token(t3, "1.0.0.3", "rack2", "dc1");
  token_map.add_token(t4, "1.0.0.4", "rack2", "dc1");

  MockTokenMap<cass::Murmur3Partitioner>::Token t5 = 400;
  MockTokenMap<cass::Murmur3Partitioner>::Token t6 = 500;
  MockTokenMap<cass::Murmur3Partitioner>::Token t7 = 600;
  MockTokenMap<cass::Murmur3Partitioner>::Token t8 = 700;

  token_map.add_token(t5, "2.0.0.1", "rack1", "dc2");
  token_map.add_token(t6, "2.0.0.2", "rack1", "dc2");
  token_map.add_token(t7, "2.0.0.3", "rack2", "dc2");
  token_map.add_token(t8, "2.0.0.4", "rack2", "dc2");

  token_map.build_replicas();

  {
    const cass::CopyOnWriteHostVec& hosts = token_map.find_hosts(t1);
    BOOST_REQUIRE(hosts && hosts->size() == 4);
    check_host((*hosts)[0], "1.0.0.1", "rack1", "dc1");
    check_host((*hosts)[1], "1.0.0.3", "rack2", "dc1");
    check_host((*hosts)[2], "2.0.0.1", "rack1", "dc2");
    check_host((*hosts)[3], "2.0.0.3", "rack2", "dc2");
  }

  {
    const cass::CopyOnWriteHostVec& hosts = token_map.find_hosts(t2);
    BOOST_REQUIRE(hosts && hosts->size() == 4);
    check_host((*hosts)[0], "1.0.0.2", "rack1", "dc1");
    check_host((*hosts)[1], "1.0.0.3", "rack2", "dc1");
    check_host((*hosts)[2], "2.0.0.1", "rack1", "dc2");
    check_host((*hosts)[3], "2.0.0.3", "rack2", "dc2");
  }

  {
    const cass::CopyOnWriteHostVec& hosts = token_map.find_hosts(t3);
    BOOST_REQUIRE(hosts && hosts->size() == 4);
    check_host((*hosts)[0], "1.0.0.3", "rack2", "dc1");
    check_host((*hosts)[1], "2.0.0.1", "rack1", "dc2");
    check_host((*hosts)[2], "2.0.0.3", "rack2", "dc2");
    check_host((*hosts)[3], "1.0.0.1", "rack1", "dc1");
  }

  {
    const cass::CopyOnWriteHostVec& hosts = token_map.find_hosts(t4);
    BOOST_REQUIRE(hosts && hosts->size() == 4);
    check_host((*hosts)[0], "1.0.0.4", "rack2", "dc1");
    check_host((*hosts)[1], "2.0.0.1", "rack1", "dc2");
    check_host((*hosts)[2], "2.0.0.3", "rack2", "dc2");
    check_host((*hosts)[3], "1.0.0.1", "rack1", "dc1");
  }

  {
    const cass::CopyOnWriteHostVec& hosts = token_map.find_hosts(t5);
    BOOST_REQUIRE(hosts && hosts->size() == 4);
    check_host((*hosts)[0], "2.0.0.1", "rack1", "dc2");
    check_host((*hosts)[1], "2.0.0.3", "rack2", "dc2");
    check_host((*hosts)[2], "1.0.0.1", "rack1", "dc1");
    check_host((*hosts)[3], "1.0.0.3", "rack2", "dc1");
  }

  {
    const cass::CopyOnWriteHostVec& hosts = token_map.find_hosts(t6);
    BOOST_REQUIRE(hosts && hosts->size() == 4);
    check_host((*hosts)[0], "2.0.0.2", "rack1", "dc2");
    check_host((*hosts)[1], "2.0.0.3", "rack2", "dc2");
    check_host((*hosts)[2], "1.0.0.1", "rack1", "dc1");
    check_host((*hosts)[3], "1.0.0.3", "rack2", "dc1");
  }

  {
    const cass::CopyOnWriteHostVec& hosts = token_map.find_hosts(t7);
    BOOST_REQUIRE(hosts && hosts->size() == 4);
    check_host((*hosts)[0], "2.0.0.3", "rack2", "dc2");
    check_host((*hosts)[1], "1.0.0.1", "rack1", "dc1");
    check_host((*hosts)[2], "1.0.0.3", "rack2", "dc1");
    check_host((*hosts)[3], "2.0.0.1", "rack1", "dc2");
  }

  {
    const cass::CopyOnWriteHostVec& hosts = token_map.find_hosts(t8);
    BOOST_REQUIRE(hosts && hosts->size() == 4);
    check_host((*hosts)[0], "2.0.0.4", "rack2", "dc2");
    check_host((*hosts)[1], "1.0.0.1", "rack1", "dc1");
    check_host((*hosts)[2], "1.0.0.3", "rack2", "dc1");
    check_host((*hosts)[3], "2.0.0.1", "rack1", "dc2");
  }
}

BOOST_AUTO_TEST_CASE(network_topology_same_rack)
{
  MockTokenMap<cass::Murmur3Partitioner> token_map;

  ReplicationMap replication;
  replication["dc1"] = "2";
  replication["dc2"] = "1";

  token_map.init_network_topology_strategy(replication);

  MockTokenMap<cass::Murmur3Partitioner>::Token t1 = 100;
  MockTokenMap<cass::Murmur3Partitioner>::Token t2 = 200;
  MockTokenMap<cass::Murmur3Partitioner>::Token t3 = 300;

  token_map.add_token(t1, "1.0.0.1", "rack1", "dc1");
  token_map.add_token(t2, "1.0.0.2", "rack1", "dc1");
  token_map.add_token(t3, "1.0.0.3", "rack1", "dc1");

  MockTokenMap<cass::Murmur3Partitioner>::Token t4 = 400;
  MockTokenMap<cass::Murmur3Partitioner>::Token t5 = 500;
  MockTokenMap<cass::Murmur3Partitioner>::Token t6 = 600;

  token_map.add_token(t4, "2.0.0.1", "rack1", "dc2");
  token_map.add_token(t5, "2.0.0.2", "rack1", "dc2");
  token_map.add_token(t6, "2.0.0.3", "rack1", "dc2");

  token_map.build_replicas();

  {
    const cass::CopyOnWriteHostVec& hosts = token_map.find_hosts(t1);
    BOOST_REQUIRE(hosts && hosts->size() == 3);
    check_host((*hosts)[0], "1.0.0.1", "rack1", "dc1");
    check_host((*hosts)[1], "1.0.0.2", "rack1", "dc1");
    check_host((*hosts)[2], "2.0.0.1", "rack1", "dc2");
  }

  {
    const cass::CopyOnWriteHostVec& hosts = token_map.find_hosts(t2);
    BOOST_REQUIRE(hosts && hosts->size() == 3);
    check_host((*hosts)[0], "1.0.0.2", "rack1", "dc1");
    check_host((*hosts)[1], "1.0.0.3", "rack1", "dc1");
    check_host((*hosts)[2], "2.0.0.1", "rack1", "dc2");
  }

  {
    const cass::CopyOnWriteHostVec& hosts = token_map.find_hosts(t3);
    BOOST_REQUIRE(hosts && hosts->size() == 3);
    check_host((*hosts)[0], "1.0.0.3", "rack1", "dc1");
    check_host((*hosts)[1], "2.0.0.1", "rack1", "dc2");
    check_host((*hosts)[2], "1.0.0.1", "rack1", "dc1");
  }

  {
    const cass::CopyOnWriteHostVec& hosts = token_map.find_hosts(t4);
    BOOST_REQUIRE(hosts && hosts->size() == 3);
    check_host((*hosts)[0], "2.0.0.1", "rack1", "dc2");
    check_host((*hosts)[1], "1.0.0.1", "rack1", "dc1");
    check_host((*hosts)[2], "1.0.0.2", "rack1", "dc1");
  }

  {
    const cass::CopyOnWriteHostVec& hosts = token_map.find_hosts(t5);
    BOOST_REQUIRE(hosts && hosts->size() == 3);
    check_host((*hosts)[0], "2.0.0.2", "rack1", "dc2");
    check_host((*hosts)[1], "1.0.0.1", "rack1", "dc1");
    check_host((*hosts)[2], "1.0.0.2", "rack1", "dc1");
  }

  {
    const cass::CopyOnWriteHostVec& hosts = token_map.find_hosts(t6);
    BOOST_REQUIRE(hosts && hosts->size() == 3);
    check_host((*hosts)[0], "2.0.0.3", "rack1", "dc2");
    check_host((*hosts)[1], "1.0.0.1", "rack1", "dc1");
    check_host((*hosts)[2], "1.0.0.2", "rack1", "dc1");
  }
}

BOOST_AUTO_TEST_CASE(network_topology_not_enough_racks)
{
  MockTokenMap<cass::Murmur3Partitioner> token_map;

  ReplicationMap replication;
  replication["dc1"] = "3";

  token_map.init_network_topology_strategy(replication);

  MockTokenMap<cass::Murmur3Partitioner>::Token t1 = 100;
  MockTokenMap<cass::Murmur3Partitioner>::Token t2 = 200;
  MockTokenMap<cass::Murmur3Partitioner>::Token t3 = 300;
  MockTokenMap<cass::Murmur3Partitioner>::Token t4 = 400;

  token_map.add_token(t1, "1.0.0.1", "rack1", "dc1");
  token_map.add_token(t2, "1.0.0.2", "rack1", "dc1");
  token_map.add_token(t3, "1.0.0.3", "rack1", "dc1");
  token_map.add_token(t4, "1.0.0.4", "rack2", "dc1");

  token_map.build_replicas();

  {
    const cass::CopyOnWriteHostVec& hosts = token_map.find_hosts(t1);
    BOOST_REQUIRE(hosts && hosts->size() == 3);
    check_host((*hosts)[0], "1.0.0.1", "rack1", "dc1");
    check_host((*hosts)[1], "1.0.0.4", "rack2", "dc1");
    check_host((*hosts)[2], "1.0.0.2", "rack1", "dc1");
  }

  {
    const cass::CopyOnWriteHostVec& hosts = token_map.find_hosts(t2);
    BOOST_REQUIRE(hosts && hosts->size() == 3);
    check_host((*hosts)[0], "1.0.0.2", "rack1", "dc1");
    check_host((*hosts)[1], "1.0.0.4", "rack2", "dc1");
    check_host((*hosts)[2], "1.0.0.3", "rack1", "dc1");
  }

  {
    const cass::CopyOnWriteHostVec& hosts = token_map.find_hosts(t3);
    BOOST_REQUIRE(hosts && hosts->size() == 3);
    check_host((*hosts)[0], "1.0.0.3", "rack1", "dc1");
    check_host((*hosts)[1], "1.0.0.4", "rack2", "dc1");
    check_host((*hosts)[2], "1.0.0.1", "rack1", "dc1");
  }

  {
    const cass::CopyOnWriteHostVec& hosts = token_map.find_hosts(t4);
    BOOST_REQUIRE(hosts && hosts->size() == 3);
    check_host((*hosts)[0], "1.0.0.4", "rack2", "dc1");
    check_host((*hosts)[1], "1.0.0.1", "rack1", "dc1");
    check_host((*hosts)[2], "1.0.0.2", "rack1", "dc1");
  }
}

BOOST_AUTO_TEST_SUITE_END()
