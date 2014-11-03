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

#ifndef __CASS_DHT_METADATA_HPP_INCLUDED__
#define __CASS_DHT_METADATA_HPP_INCLUDED__

#include "buffer.hpp"
#include "copy_on_write_ptr.hpp"
#include "host.hpp"
#include "replica_placement_strategies.hpp"
#include "scoped_ptr.hpp"
#include "schema_metadata.hpp"

#include "third_party/boost/boost/utility/string_ref.hpp"

#include <map>
#include <vector>

namespace cass {

typedef std::vector<boost::string_ref> TokenStringList;

class TokenMap {
public:
  virtual ~TokenMap() {}

  void build() { map_replicas(true); }

  void update_host(SharedRefPtr<Host>& host, const TokenStringList& token_strings);
  void remove_host(SharedRefPtr<Host>& host);
  void update_keyspace(const std::string& ks_name, const KeyspaceMetadata& ks_meta);
  void drop_keyspace(const std::string& ks_name);
  const COWHostVec& get_replicas(const std::string& ks_name,
                                              const BufferRefs& key_parts) const;

private:
  void map_replicas(bool force = false);
  void map_keyspace_replicas(const std::string& ks_name,
                             const ReplicaPlacementStrategy& rps,
                             bool force = false);
  bool purge_address(const Address& addr);

protected:
  virtual Token token_from_string_ref(const boost::string_ref& token_string_ref) const = 0;
  virtual Token hash(const BufferRefs& key_parts) const = 0;

  TokenHostMap token_map_;

  typedef std::map<std::string, TokenReplicaMap> KeyspaceReplicaMap;
  KeyspaceReplicaMap keyspace_replica_map_;

  typedef std::map<std::string, ScopedPtr<ReplicaPlacementStrategy> > KeyspaceStrategyMap;
  KeyspaceStrategyMap keyspace_strategy_map_;

  typedef std::set<Address> AddressSet;
  AddressSet mapped_addresses_;
};


class M3PTokenMap : public TokenMap {
public:
  static const std::string PARTIONER_CLASS;
  static bool less_than(const Token& a, const Token& b);

  virtual Token token_from_string_ref(const boost::string_ref& token_string_ref) const;
  virtual Token hash(const BufferRefs& key_parts) const;
};


class RPTokenMap : public TokenMap {
public:
  static const std::string PARTIONER_CLASS;
  static bool less_than(const Token& a, const Token& b);

  virtual Token token_from_string_ref(const boost::string_ref& token_string_ref) const;
  virtual Token hash(const BufferRefs& key_parts) const;
};


class BOPTokenMap : public TokenMap {
public:
  static const std::string PARTIONER_CLASS;
  static bool less_than(const Token& a, const Token& b);

  virtual Token token_from_string_ref(const boost::string_ref& token_string_ref) const;
  virtual Token hash(const BufferRefs& key_parts) const;
};


class DHTMetadata {
public:
  void clear() { token_map_.reset(); }
  void build();
  void set_partitioner(const std::string& partitioner_class);
  void update_host(SharedRefPtr<Host>& host, const TokenStringList& tokens);
  void remove_host(SharedRefPtr<Host>& host);
  void update_keyspace(const std::string& ks_name, const KeyspaceMetadata& ks_meta);
  void drop_keyspace(const std::string& ks_name);

  const COWHostVec& get_replicas(const std::string& ks_name, const BufferRefs& key_parts) const;

private:
  ScopedPtr<TokenMap> token_map_;
};

} // namespace cass

#endif
