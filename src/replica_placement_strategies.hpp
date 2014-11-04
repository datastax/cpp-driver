/*
  Copyright 2014 DataStax

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

#ifndef __CASS_REPLICA_PLACEMENT_STRATEGIES_HPP_INCLUDED__
#define __CASS_REPLICA_PLACEMENT_STRATEGIES_HPP_INCLUDED__

#include "buffer.hpp"
#include "host.hpp"
#include "ref_counted.hpp"
#include "schema_metadata.hpp"

#include <map>

namespace cass {

typedef std::vector<uint8_t> Token;
typedef std::map<Token, SharedRefPtr<Host> > TokenHostMap;
typedef std::map<Token, CopyOnWriteHostVec> TokenReplicaMap;

class ReplicaPlacementStrategy : public RefCounted<ReplicaPlacementStrategy> {
public:
  static SharedRefPtr<ReplicaPlacementStrategy> from_keyspace_meta(const KeyspaceMetadata& ks_meta);
  virtual ~ReplicaPlacementStrategy() {}

  virtual bool equals(const ReplicaPlacementStrategy& other) = 0;
  virtual void tokens_to_replicas(const TokenHostMap& primary, TokenReplicaMap* output) const = 0;
};


class NetworkTopologyStrategy : public ReplicaPlacementStrategy {
public:
  static const std::string STRATEGY_CLASS;
  NetworkTopologyStrategy(const KeyspaceMetadata::StrategyOptions& options);
  virtual ~NetworkTopologyStrategy() {}

  virtual bool equals(const ReplicaPlacementStrategy& other);
  virtual void tokens_to_replicas(const TokenHostMap& primary, TokenReplicaMap* output) const;

private:
  std::map<std::string, size_t> dc_replicas_;
};


class SimpleStrategy : public ReplicaPlacementStrategy {
public:
  static const std::string STRATEGY_CLASS;
  SimpleStrategy(const KeyspaceMetadata::StrategyOptions& options);
  virtual ~SimpleStrategy() {}

  virtual bool equals(const ReplicaPlacementStrategy& other);
  virtual void tokens_to_replicas(const TokenHostMap& primary, TokenReplicaMap* output) const;

private:
  size_t replication_factor_;
};


class NonReplicatedStrategy : public ReplicaPlacementStrategy {
public:
  virtual ~NonReplicatedStrategy() {}

  virtual bool equals(const ReplicaPlacementStrategy& other);
  virtual void tokens_to_replicas(const TokenHostMap& primary, TokenReplicaMap* output) const;
};

} // namespace cass

#endif
