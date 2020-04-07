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

#include "set.hpp"
#include "string.hpp"
#include "test_token_map_utils.hpp"
#include "vector.hpp"

using namespace datastax;
using namespace datastax::internal;
using namespace datastax::internal::core;

namespace {

static const CopyOnWriteHostVec NO_REPLICAS(NULL);

template <class Partitioner>
struct MockTokenMap {
  typedef typename ReplicationStrategy<Partitioner>::Token Token;
  typedef typename ReplicationStrategy<Partitioner>::TokenHost TokenHost;
  typedef typename ReplicationStrategy<Partitioner>::TokenHostVec TokenHostVec;
  typedef typename ReplicationStrategy<Partitioner>::TokenReplicas TokenReplicas;
  typedef typename ReplicationStrategy<Partitioner>::TokenReplicasVec TokenReplicasVec;

  struct TokenReplicasCompare {
    bool operator()(const TokenReplicas& lhs, const TokenReplicas& rhs) const {
      return lhs.first < rhs.first;
    }
  };

  HostSet hosts;
  IdGenerator dc_ids;
  IdGenerator rack_ids;

  ReplicationStrategy<Partitioner> strategy;
  TokenHostVec tokens;
  TokenReplicasVec replicas;
  DatacenterMap datacenters;

  void init_simple_strategy(size_t replication_factor) {
    DataType::ConstPtr varchar_data_type(new DataType(CASS_VALUE_TYPE_VARCHAR));

    ColumnMetadataVec column_metadata;
    column_metadata.push_back(ColumnMetadata("keyspace_name", varchar_data_type));
    column_metadata.push_back(ColumnMetadata(
        "replication", CollectionType::map(varchar_data_type, varchar_data_type, true)));
    RowResultResponseBuilder builder(column_metadata);

    ReplicationMap replication;
    replication["class"] = CASS_SIMPLE_STRATEGY;

    OStringStream ss;
    ss << replication_factor;
    replication["replication_factor"] = ss.str();

    builder.append_keyspace_row_v3("ks1", replication);
    builder.finish();

    ResultIterator iterator(builder.finish());
    EXPECT_TRUE(iterator.next());
    strategy.init(dc_ids, VersionNumber(3, 0, 0), iterator.row());
  }

  void init_network_topology_strategy(ReplicationMap& replication) {
    DataType::ConstPtr varchar_data_type(new DataType(CASS_VALUE_TYPE_VARCHAR));

    ColumnMetadataVec column_metadata;
    column_metadata.push_back(ColumnMetadata("keyspace_name", varchar_data_type));
    column_metadata.push_back(ColumnMetadata(
        "replication", CollectionType::map(varchar_data_type, varchar_data_type, true)));
    RowResultResponseBuilder builder(column_metadata);

    replication["class"] = CASS_NETWORK_TOPOLOGY_STRATEGY;
    builder.append_keyspace_row_v3("ks1", replication);
    builder.finish();

    ResultIterator iterator(builder.finish());
    EXPECT_TRUE(iterator.next());
    strategy.init(dc_ids, VersionNumber(3, 0, 0), iterator.row());
  }

  void add_token(Token token, const String& address, const String& rack = "",
                 const String& dc = "") {
    tokens.push_back(TokenHost(token, create_host(address, rack, dc)));
  }

  void build_replicas() {
    std::sort(tokens.begin(), tokens.end()); // We assume sorted tokens
    build_datacenters(hosts, datacenters);
    strategy.build_replicas(tokens, datacenters, replicas);
  }

  const CopyOnWriteHostVec& find_hosts(Token token) {
    typename TokenReplicasVec::const_iterator i =
        std::lower_bound(replicas.begin(), replicas.end(), TokenReplicas(token, NO_REPLICAS),
                         TokenReplicasCompare());
    if (i != replicas.end() && i->first == token) {
      return i->second;
    }
    return NO_REPLICAS;
  }

  Host* create_host(const String& address, const String& rack = "", const String& dc = "") {
    Host::Ptr host(new Host(Address(address, 9042)));
    host->set_rack_and_dc(rack, dc);
    host->set_rack_and_dc_ids(rack_ids.get(rack), dc_ids.get(dc));
    HostSet::iterator i = hosts.find(host);
    if (i != hosts.end()) {
      return i->get();
    } else {
      hosts.insert(host);
      return host.get();
    }
  }
};

void check_host(const SharedRefPtr<Host>& host, const String& ip, const String& rack = "",
                const String& dc = "") {
  EXPECT_EQ(host->address(), Address(ip, 9042));
  EXPECT_EQ(host->rack(), rack);
  EXPECT_EQ(host->dc(), dc);
}

} // namespace

TEST(ReplicationStrategyUnitTest, Simple) {
  MockTokenMap<Murmur3Partitioner> token_map;

  token_map.init_simple_strategy(3);

  MockTokenMap<Murmur3Partitioner>::Token t1 = 0;
  MockTokenMap<Murmur3Partitioner>::Token t2 = 100;
  MockTokenMap<Murmur3Partitioner>::Token t3 = 200;
  MockTokenMap<Murmur3Partitioner>::Token t4 = 300;

  token_map.add_token(t1, "1.0.0.1");
  token_map.add_token(t2, "1.0.0.2");
  token_map.add_token(t3, "1.0.0.3");
  token_map.add_token(t4, "1.0.0.4");

  token_map.build_replicas();

  {
    const CopyOnWriteHostVec& hosts = token_map.find_hosts(t1);
    ASSERT_TRUE(hosts && hosts->size() == 3);
    check_host((*hosts)[0], "1.0.0.1");
    check_host((*hosts)[1], "1.0.0.2");
    check_host((*hosts)[2], "1.0.0.3");
  }

  {
    const CopyOnWriteHostVec& hosts = token_map.find_hosts(t2);
    check_host((*hosts)[0], "1.0.0.2");
    check_host((*hosts)[1], "1.0.0.3");
    check_host((*hosts)[2], "1.0.0.4");
  }

  {
    const CopyOnWriteHostVec& hosts = token_map.find_hosts(t3);
    check_host((*hosts)[0], "1.0.0.3");
    check_host((*hosts)[1], "1.0.0.4");
    check_host((*hosts)[2], "1.0.0.1");
  }

  {
    const CopyOnWriteHostVec& hosts = token_map.find_hosts(t4);
    check_host((*hosts)[0], "1.0.0.4");
    check_host((*hosts)[1], "1.0.0.1");
    check_host((*hosts)[2], "1.0.0.2");
  }
}

TEST(ReplicationStrategyUnitTest, SimpleNumHostsLessThanReplicationFactor) {
  MockTokenMap<Murmur3Partitioner> token_map;

  token_map.init_simple_strategy(3);

  MockTokenMap<Murmur3Partitioner>::Token t1 = 0;

  // To reproduce the issue the number of tokens needs to be greater than
  // (or equal to) the RF because the RF is bounded by the number of tokens.
  token_map.add_token(t1, "1.0.0.1");
  token_map.add_token(100, "1.0.0.1");
  token_map.add_token(200, "1.0.0.1");
  token_map.add_token(300, "1.0.0.1");

  token_map.build_replicas();

  const CopyOnWriteHostVec& hosts = token_map.find_hosts(t1);
  ASSERT_TRUE(hosts && hosts->size() == 1);
  check_host((*hosts)[0], "1.0.0.1");
}

TEST(ReplicationStrategyUnitTest, NetworkTopology) {
  MockTokenMap<Murmur3Partitioner> token_map;

  ReplicationMap replication;
  replication["dc1"] = "2";
  replication["dc2"] = "2";

  token_map.init_network_topology_strategy(replication);

  MockTokenMap<Murmur3Partitioner>::Token t1 = 0;
  MockTokenMap<Murmur3Partitioner>::Token t2 = 100;
  MockTokenMap<Murmur3Partitioner>::Token t3 = 200;
  MockTokenMap<Murmur3Partitioner>::Token t4 = 300;

  token_map.add_token(t1, "1.0.0.1", "rack1", "dc1");
  token_map.add_token(t2, "1.0.0.2", "rack1", "dc1");
  token_map.add_token(t3, "1.0.0.3", "rack2", "dc1");
  token_map.add_token(t4, "1.0.0.4", "rack2", "dc1");

  MockTokenMap<Murmur3Partitioner>::Token t5 = 400;
  MockTokenMap<Murmur3Partitioner>::Token t6 = 500;
  MockTokenMap<Murmur3Partitioner>::Token t7 = 600;
  MockTokenMap<Murmur3Partitioner>::Token t8 = 700;

  token_map.add_token(t5, "2.0.0.1", "rack1", "dc2");
  token_map.add_token(t6, "2.0.0.2", "rack1", "dc2");
  token_map.add_token(t7, "2.0.0.3", "rack2", "dc2");
  token_map.add_token(t8, "2.0.0.4", "rack2", "dc2");

  token_map.build_replicas();

  {
    const CopyOnWriteHostVec& hosts = token_map.find_hosts(t1);
    ASSERT_TRUE(hosts && hosts->size() == 4);
    check_host((*hosts)[0], "1.0.0.1", "rack1", "dc1");
    check_host((*hosts)[1], "1.0.0.3", "rack2", "dc1");
    check_host((*hosts)[2], "2.0.0.1", "rack1", "dc2");
    check_host((*hosts)[3], "2.0.0.3", "rack2", "dc2");
  }

  {
    const CopyOnWriteHostVec& hosts = token_map.find_hosts(t2);
    ASSERT_TRUE(hosts && hosts->size() == 4);
    check_host((*hosts)[0], "1.0.0.2", "rack1", "dc1");
    check_host((*hosts)[1], "1.0.0.3", "rack2", "dc1");
    check_host((*hosts)[2], "2.0.0.1", "rack1", "dc2");
    check_host((*hosts)[3], "2.0.0.3", "rack2", "dc2");
  }

  {
    const CopyOnWriteHostVec& hosts = token_map.find_hosts(t3);
    ASSERT_TRUE(hosts && hosts->size() == 4);
    check_host((*hosts)[0], "1.0.0.3", "rack2", "dc1");
    check_host((*hosts)[1], "2.0.0.1", "rack1", "dc2");
    check_host((*hosts)[2], "2.0.0.3", "rack2", "dc2");
    check_host((*hosts)[3], "1.0.0.1", "rack1", "dc1");
  }

  {
    const CopyOnWriteHostVec& hosts = token_map.find_hosts(t4);
    ASSERT_TRUE(hosts && hosts->size() == 4);
    check_host((*hosts)[0], "1.0.0.4", "rack2", "dc1");
    check_host((*hosts)[1], "2.0.0.1", "rack1", "dc2");
    check_host((*hosts)[2], "2.0.0.3", "rack2", "dc2");
    check_host((*hosts)[3], "1.0.0.1", "rack1", "dc1");
  }

  {
    const CopyOnWriteHostVec& hosts = token_map.find_hosts(t5);
    ASSERT_TRUE(hosts && hosts->size() == 4);
    check_host((*hosts)[0], "2.0.0.1", "rack1", "dc2");
    check_host((*hosts)[1], "2.0.0.3", "rack2", "dc2");
    check_host((*hosts)[2], "1.0.0.1", "rack1", "dc1");
    check_host((*hosts)[3], "1.0.0.3", "rack2", "dc1");
  }

  {
    const CopyOnWriteHostVec& hosts = token_map.find_hosts(t6);
    ASSERT_TRUE(hosts && hosts->size() == 4);
    check_host((*hosts)[0], "2.0.0.2", "rack1", "dc2");
    check_host((*hosts)[1], "2.0.0.3", "rack2", "dc2");
    check_host((*hosts)[2], "1.0.0.1", "rack1", "dc1");
    check_host((*hosts)[3], "1.0.0.3", "rack2", "dc1");
  }

  {
    const CopyOnWriteHostVec& hosts = token_map.find_hosts(t7);
    ASSERT_TRUE(hosts && hosts->size() == 4);
    check_host((*hosts)[0], "2.0.0.3", "rack2", "dc2");
    check_host((*hosts)[1], "1.0.0.1", "rack1", "dc1");
    check_host((*hosts)[2], "1.0.0.3", "rack2", "dc1");
    check_host((*hosts)[3], "2.0.0.1", "rack1", "dc2");
  }

  {
    const CopyOnWriteHostVec& hosts = token_map.find_hosts(t8);
    ASSERT_TRUE(hosts && hosts->size() == 4);
    check_host((*hosts)[0], "2.0.0.4", "rack2", "dc2");
    check_host((*hosts)[1], "1.0.0.1", "rack1", "dc1");
    check_host((*hosts)[2], "1.0.0.3", "rack2", "dc1");
    check_host((*hosts)[3], "2.0.0.1", "rack1", "dc2");
  }
}

TEST(ReplicationStrategyUnitTest, NetworkTopologySameRack) {
  MockTokenMap<Murmur3Partitioner> token_map;

  ReplicationMap replication;
  replication["dc1"] = "2";
  replication["dc2"] = "1";

  token_map.init_network_topology_strategy(replication);

  MockTokenMap<Murmur3Partitioner>::Token t1 = 100;
  MockTokenMap<Murmur3Partitioner>::Token t2 = 200;
  MockTokenMap<Murmur3Partitioner>::Token t3 = 300;

  token_map.add_token(t1, "1.0.0.1", "rack1", "dc1");
  token_map.add_token(t2, "1.0.0.2", "rack1", "dc1");
  token_map.add_token(t3, "1.0.0.3", "rack1", "dc1");

  MockTokenMap<Murmur3Partitioner>::Token t4 = 400;
  MockTokenMap<Murmur3Partitioner>::Token t5 = 500;
  MockTokenMap<Murmur3Partitioner>::Token t6 = 600;

  token_map.add_token(t4, "2.0.0.1", "rack1", "dc2");
  token_map.add_token(t5, "2.0.0.2", "rack1", "dc2");
  token_map.add_token(t6, "2.0.0.3", "rack1", "dc2");

  token_map.build_replicas();

  {
    const CopyOnWriteHostVec& hosts = token_map.find_hosts(t1);
    ASSERT_TRUE(hosts && hosts->size() == 3);
    check_host((*hosts)[0], "1.0.0.1", "rack1", "dc1");
    check_host((*hosts)[1], "1.0.0.2", "rack1", "dc1");
    check_host((*hosts)[2], "2.0.0.1", "rack1", "dc2");
  }

  {
    const CopyOnWriteHostVec& hosts = token_map.find_hosts(t2);
    ASSERT_TRUE(hosts && hosts->size() == 3);
    check_host((*hosts)[0], "1.0.0.2", "rack1", "dc1");
    check_host((*hosts)[1], "1.0.0.3", "rack1", "dc1");
    check_host((*hosts)[2], "2.0.0.1", "rack1", "dc2");
  }

  {
    const CopyOnWriteHostVec& hosts = token_map.find_hosts(t3);
    ASSERT_TRUE(hosts && hosts->size() == 3);
    check_host((*hosts)[0], "1.0.0.3", "rack1", "dc1");
    check_host((*hosts)[1], "2.0.0.1", "rack1", "dc2");
    check_host((*hosts)[2], "1.0.0.1", "rack1", "dc1");
  }

  {
    const CopyOnWriteHostVec& hosts = token_map.find_hosts(t4);
    ASSERT_TRUE(hosts && hosts->size() == 3);
    check_host((*hosts)[0], "2.0.0.1", "rack1", "dc2");
    check_host((*hosts)[1], "1.0.0.1", "rack1", "dc1");
    check_host((*hosts)[2], "1.0.0.2", "rack1", "dc1");
  }

  {
    const CopyOnWriteHostVec& hosts = token_map.find_hosts(t5);
    ASSERT_TRUE(hosts && hosts->size() == 3);
    check_host((*hosts)[0], "2.0.0.2", "rack1", "dc2");
    check_host((*hosts)[1], "1.0.0.1", "rack1", "dc1");
    check_host((*hosts)[2], "1.0.0.2", "rack1", "dc1");
  }

  {
    const CopyOnWriteHostVec& hosts = token_map.find_hosts(t6);
    ASSERT_TRUE(hosts && hosts->size() == 3);
    check_host((*hosts)[0], "2.0.0.3", "rack1", "dc2");
    check_host((*hosts)[1], "1.0.0.1", "rack1", "dc1");
    check_host((*hosts)[2], "1.0.0.2", "rack1", "dc1");
  }
}

TEST(ReplicationStrategyUnitTest, NetworkTopologyNotEnoughRacks) {
  MockTokenMap<Murmur3Partitioner> token_map;

  ReplicationMap replication;
  replication["dc1"] = "3";

  token_map.init_network_topology_strategy(replication);

  MockTokenMap<Murmur3Partitioner>::Token t1 = 100;
  MockTokenMap<Murmur3Partitioner>::Token t2 = 200;
  MockTokenMap<Murmur3Partitioner>::Token t3 = 300;
  MockTokenMap<Murmur3Partitioner>::Token t4 = 400;

  token_map.add_token(t1, "1.0.0.1", "rack1", "dc1");
  token_map.add_token(t2, "1.0.0.2", "rack1", "dc1");
  token_map.add_token(t3, "1.0.0.3", "rack1", "dc1");
  token_map.add_token(t4, "1.0.0.4", "rack2", "dc1");

  token_map.build_replicas();

  {
    const CopyOnWriteHostVec& hosts = token_map.find_hosts(t1);
    ASSERT_TRUE(hosts && hosts->size() == 3);
    check_host((*hosts)[0], "1.0.0.1", "rack1", "dc1");
    check_host((*hosts)[1], "1.0.0.4", "rack2", "dc1");
    check_host((*hosts)[2], "1.0.0.2", "rack1", "dc1");
  }

  {
    const CopyOnWriteHostVec& hosts = token_map.find_hosts(t2);
    ASSERT_TRUE(hosts && hosts->size() == 3);
    check_host((*hosts)[0], "1.0.0.2", "rack1", "dc1");
    check_host((*hosts)[1], "1.0.0.4", "rack2", "dc1");
    check_host((*hosts)[2], "1.0.0.3", "rack1", "dc1");
  }

  {
    const CopyOnWriteHostVec& hosts = token_map.find_hosts(t3);
    ASSERT_TRUE(hosts && hosts->size() == 3);
    check_host((*hosts)[0], "1.0.0.3", "rack1", "dc1");
    check_host((*hosts)[1], "1.0.0.4", "rack2", "dc1");
    check_host((*hosts)[2], "1.0.0.1", "rack1", "dc1");
  }

  {
    const CopyOnWriteHostVec& hosts = token_map.find_hosts(t4);
    ASSERT_TRUE(hosts && hosts->size() == 3);
    check_host((*hosts)[0], "1.0.0.4", "rack2", "dc1");
    check_host((*hosts)[1], "1.0.0.1", "rack1", "dc1");
    check_host((*hosts)[2], "1.0.0.2", "rack1", "dc1");
  }
}
