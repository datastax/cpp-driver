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

#ifndef __CASS_REPLICATION_STRATEGY_HPP_INCLUDED__
#define __CASS_REPLICATION_STRATEGY_HPP_INCLUDED__

#include "buffer.hpp"
#include "host.hpp"
#include "ref_counted.hpp"
#include "schema_metadata.hpp"

#include <map>

namespace cass {

typedef std::vector<uint8_t> Token;
typedef std::map<Token, SharedRefPtr<Host> > TokenHostMap;
typedef std::map<Token, CopyOnWriteHostVec> TokenReplicaMap;

class ReplicationStrategy : public RefCounted<ReplicationStrategy> {
public:
  static SharedRefPtr<ReplicationStrategy> from_keyspace_meta(const KeyspaceMetadata& ks_meta);

  ReplicationStrategy(const std::string& strategy_class)
    : strategy_class_(strategy_class) {}

  virtual ~ReplicationStrategy() {}
  virtual bool equal(const KeyspaceMetadata& ks_meta) = 0;
  virtual void tokens_to_replicas(const TokenHostMap& primary, TokenReplicaMap* output) const = 0;

protected:
  std::string strategy_class_;
};


class NetworkTopologyStrategy : public ReplicationStrategy {
public:
  typedef std::map<std::string, size_t> DCReplicaCountMap;

  static const std::string STRATEGY_CLASS;

  NetworkTopologyStrategy(const std::string& strategy_class,
                          const SchemaMetadataField* strategy_options);
  virtual ~NetworkTopologyStrategy() {}

  virtual bool equal(const KeyspaceMetadata& ks_meta);
  virtual void tokens_to_replicas(const TokenHostMap& primary, TokenReplicaMap* output) const;

  // Testing only
  NetworkTopologyStrategy(const std::string& strategy_class,
                          const DCReplicaCountMap& replication_factors)
    : ReplicationStrategy(strategy_class)
    , replication_factors_(replication_factors) {}

private:
  static void build_dc_replicas(const SchemaMetadataField* strategy_options, DCReplicaCountMap* dc_replicas);
  DCReplicaCountMap replication_factors_;
};


class SimpleStrategy : public ReplicationStrategy {
public:
  static const std::string STRATEGY_CLASS;

  SimpleStrategy(const std::string& strategy_class,
                 const SchemaMetadataField* strategy_options);
  virtual ~SimpleStrategy() {}

  virtual bool equal(const KeyspaceMetadata& ks_meta);
  virtual void tokens_to_replicas(const TokenHostMap& primary, TokenReplicaMap* output) const;

  // Testing only
  SimpleStrategy(const std::string& strategy_class,
                 size_t replication_factor)
    : ReplicationStrategy(strategy_class)
    , replication_factor_(replication_factor) {}

private:
  static size_t get_replication_factor(const SchemaMetadataField* strategy_options);
  size_t replication_factor_;
};


class NonReplicatedStrategy : public ReplicationStrategy {
public:
  NonReplicatedStrategy(const std::string& strategy_class)
    : ReplicationStrategy(strategy_class) {}
  virtual ~NonReplicatedStrategy() {}

  virtual bool equal(const KeyspaceMetadata& ks_meta);
  virtual void tokens_to_replicas(const TokenHostMap& primary, TokenReplicaMap* output) const;
};

} // namespace cass

#endif
