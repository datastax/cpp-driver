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

#ifndef __CASS_TOKEN_MAP_HPP_INCLUDED__
#define __CASS_TOKEN_MAP_HPP_INCLUDED__

#include "buffer.hpp"
#include "copy_on_write_ptr.hpp"
#include "host.hpp"
#include "replication_strategy.hpp"
#include "scoped_ptr.hpp"
#include "schema_metadata.hpp"
#include "string_ref.hpp"

#include <map>
#include <vector>

namespace cass {

typedef std::vector<StringRef> TokenStringList;

class Partitioner {
public:
  virtual ~Partitioner() {}
  virtual Token token_from_string_ref(const StringRef& token_string_ref) const = 0;
  virtual Token hash(const uint8_t* data, size_t size) const = 0;
};

class TokenMap {
public:
  virtual ~TokenMap() {}

  void clear();
  void build();

  void set_partitioner(const std::string& partitioner_class);
  void update_host(SharedRefPtr<Host>& host, const TokenStringList& token_strings);
  void remove_host(SharedRefPtr<Host>& host);
  void update_keyspace(const std::string& ks_name, const KeyspaceMetadata& ks_meta);
  void drop_keyspace(const std::string& ks_name);
  const CopyOnWriteHostVec& get_replicas(const std::string& ks_name,
                                         const std::string& routing_key) const;

  // Testing only
  void set_replication_strategy(const std::string& ks_name,
                                const SharedRefPtr<ReplicationStrategy>& strategy);

private:
  void map_replicas(bool force = false);
  void map_keyspace_replicas(const std::string& ks_name,
                             const SharedRefPtr<ReplicationStrategy>& strategy,
                             bool force = false);
  bool purge_address(const Address& addr);

protected:
  TokenHostMap token_map_;

  typedef std::map<std::string, TokenReplicaMap> KeyspaceReplicaMap;
  KeyspaceReplicaMap keyspace_replica_map_;

  typedef std::map<std::string, SharedRefPtr<ReplicationStrategy> > KeyspaceStrategyMap;
  KeyspaceStrategyMap keyspace_strategy_map_;

  typedef std::set<Address> AddressSet;
  AddressSet mapped_addresses_;

  ScopedPtr<Partitioner> partitioner_;
};


class Murmur3Partitioner : public Partitioner {
public:
  static const std::string PARTITIONER_CLASS;

  virtual Token token_from_string_ref(const StringRef& token_string_ref) const;
  virtual Token hash(const uint8_t* data, size_t size) const;
};


class RandomPartitioner : public Partitioner {
public:
  static const std::string PARTITIONER_CLASS;

  virtual Token token_from_string_ref(const StringRef& token_string_ref) const;
  virtual Token hash(const uint8_t* data, size_t size) const;
};


class ByteOrderedPartitioner : public Partitioner {
public:
  static const std::string PARTITIONER_CLASS;

  virtual Token token_from_string_ref(const StringRef& token_string_ref) const;
  virtual Token hash(const uint8_t* data, size_t size) const;
};

} // namespace cass

#endif
