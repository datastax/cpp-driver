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

#ifndef DATASTAX_INTERNAL_TOKEN_MAP_IMPL_HPP
#define DATASTAX_INTERNAL_TOKEN_MAP_IMPL_HPP

#include "collection_iterator.hpp"
#include "constants.hpp"
#include "dense_hash_map.hpp"
#include "dense_hash_set.hpp"
#include "deque.hpp"
#include "json.hpp"
#include "map_iterator.hpp"
#include "result_iterator.hpp"
#include "result_response.hpp"
#include "row.hpp"
#include "string_ref.hpp"
#include "token_map.hpp"
#include "value.hpp"
#include "vector.hpp"

#include <algorithm>
#include <assert.h>
#include <iomanip>
#include <ios>
#include <uv.h>

#define CASS_NETWORK_TOPOLOGY_STRATEGY "NetworkTopologyStrategy"
#define CASS_SIMPLE_STRATEGY "SimpleStrategy"

namespace std {

template <>
struct equal_to<datastax::internal::core::Host::Ptr> {
  bool operator()(const datastax::internal::core::Host::Ptr& lhs,
                  const datastax::internal::core::Host::Ptr& rhs) const {
    if (lhs == rhs) {
      return true;
    }
    if (!lhs || !rhs) {
      return false;
    }
    return lhs->address() == rhs->address();
  }
};

#if defined(HASH_IN_TR1) && !defined(_WIN32)
namespace tr1 {
#endif

template <>
struct hash<datastax::internal::core::Host::Ptr> {
  std::size_t operator()(const datastax::internal::core::Host::Ptr& host) const {
    if (!host) return 0;
    return hasher(host->address());
  }
  SPARSEHASH_HASH<datastax::internal::core::Address> hasher;
};

#if defined(HASH_IN_TR1) && !defined(_WIN32)
} // namespace tr1
#endif

} // namespace std

namespace datastax { namespace internal { namespace core {

class IdGenerator {
public:
  typedef DenseHashMap<String, uint32_t> IdMap;

  static const uint32_t EMPTY_KEY;
  static const uint32_t DELETED_KEY;

  IdGenerator() { ids_.set_empty_key(String()); }

  uint32_t get(const String& key) {
    if (key.empty()) {
      return 0;
    }

    IdMap::const_iterator i = ids_.find(key);
    if (i != ids_.end()) {
      return i->second;
    }

    // This will never generate a 0 identifier. So 0 can be used as
    // inalid or empty.
    uint32_t id = ids_.size() + 1;
    ids_[key] = id;
    return id;
  }

private:
  IdMap ids_;
};

struct Murmur3Partitioner {
  typedef int64_t Token;

  static Token from_string(const StringRef& str);
  static Token hash(const StringRef& str);
  static StringRef name() { return "Murmur3Partitioner"; }
};

struct RandomPartitioner {
  struct Token {
    uint64_t hi;
    uint64_t lo;

    bool operator<(const Token& other) const {
      return hi == other.hi ? lo < other.lo : hi < other.hi;
    }

    bool operator==(const Token& other) const { return hi == other.hi && lo == other.lo; }
  };

  static Token abs(Token token);
  static uint64_t encode(uint8_t* bytes);

  static Token from_string(const StringRef& str);
  static Token hash(const StringRef& str);
  static StringRef name() { return "RandomPartitioner"; }
};

class ByteOrderedPartitioner {
public:
  typedef Vector<uint8_t> Token;

  static Token from_string(const StringRef& str);
  static Token hash(const StringRef& str);
  static StringRef name() { return "ByteOrderedPartitioner"; }
};

inline std::ostream& operator<<(std::ostream& os, const RandomPartitioner::Token& token) {
  os << std::setfill('0') << std::setw(16) << std::hex << token.hi << std::setfill('0')
     << std::setw(16) << std::hex << token.lo;
  return os;
}

inline std::ostream& operator<<(std::ostream& os, const ByteOrderedPartitioner::Token& token) {
  for (ByteOrderedPartitioner::Token::const_iterator it = token.begin(), end = token.end();
       it != end; ++it) {
    os << std::hex << *it;
  }
  return os;
}

class HostSet : public DenseHashSet<Host::Ptr> {
public:
  HostSet() {
    set_empty_key(Host::Ptr(new Host(Address::EMPTY_KEY)));
    set_deleted_key(Host::Ptr(new Host(Address::DELETED_KEY)));
  }

  template <class InputIterator>
  HostSet(InputIterator first, InputIterator last)
      : DenseHashSet<Host::Ptr>(first, last, Host::Ptr(new Host(Address::EMPTY_KEY))) {
    set_deleted_key(Host::Ptr(new Host(Address::DELETED_KEY)));
  }
};

class RackSet : public DenseHashSet<uint32_t> {
public:
  RackSet() {
    set_empty_key(IdGenerator::EMPTY_KEY);
    set_deleted_key(IdGenerator::DELETED_KEY);
  }
};

struct Datacenter {
  Datacenter()
      : num_nodes(0) {}
  size_t num_nodes;
  RackSet racks;
};

class DatacenterMap : public DenseHashMap<uint32_t, Datacenter> {
public:
  DatacenterMap() {
    set_empty_key(IdGenerator::EMPTY_KEY);
    set_deleted_key(IdGenerator::DELETED_KEY);
  }
};

struct ReplicationFactor {
  ReplicationFactor()
      : count(0) {}
  size_t count;
  String name; // Used for logging the datacenter name
  bool operator==(const ReplicationFactor& other) const {
    return count == other.count && name == other.name;
  }
};

inline void build_datacenters(const HostSet& hosts, DatacenterMap& result) {
  result.clear();
  for (HostSet::const_iterator i = hosts.begin(), end = hosts.end(); i != end; ++i) {
    uint32_t dc = (*i)->dc_id();
    uint32_t rack = (*i)->rack_id();
    if (dc != 0 && rack != 0) {
      Datacenter& datacenter = result[dc];
      datacenter.racks.insert(rack);
      datacenter.num_nodes++;
    }
  }
}

class ReplicationFactorMap : public DenseHashMap<uint32_t, ReplicationFactor> {
public:
  ReplicationFactorMap() { set_empty_key(IdGenerator::EMPTY_KEY); }
};

template <class Partitioner>
class ReplicationStrategy {
public:
  typedef typename Partitioner::Token Token;

  typedef std::pair<Token, Host*> TokenHost;
  typedef Vector<TokenHost> TokenHostVec;

  typedef std::pair<Token, CopyOnWriteHostVec> TokenReplicas;
  typedef Vector<TokenReplicas> TokenReplicasVec;

  typedef Deque<typename TokenHostVec::const_iterator> TokenHostQueue;

  struct DatacenterRackInfo {
    DatacenterRackInfo()
        : replica_count(0)
        , replication_factor(0)
        , rack_count(0) {}
    size_t replica_count;
    size_t replication_factor;
    RackSet racks_observed;
    size_t rack_count;
    TokenHostQueue skipped_endpoints;
  };

  class DatacenterRackInfoMap : public DenseHashMap<uint32_t, DatacenterRackInfo> {
  public:
    DatacenterRackInfoMap() {
      DenseHashMap<uint32_t, DatacenterRackInfo>::set_empty_key(IdGenerator::EMPTY_KEY);
    }
  };

  enum Type { NETWORK_TOPOLOGY_STRATEGY, SIMPLE_STRATEGY, NON_REPLICATED };

  ReplicationStrategy()
      : type_(NON_REPLICATED) {}

  void init(IdGenerator& dc_ids, const VersionNumber& cassandra_version, const Row* row);

  bool operator!=(const ReplicationStrategy& other) const {
    return type_ != other.type_ || replication_factors_ != other.replication_factors_;
  }

  void build_replicas(const TokenHostVec& tokens, const DatacenterMap& datacenters,
                      TokenReplicasVec& result) const;

private:
  void build_replicas_network_topology(const TokenHostVec& tokens, const DatacenterMap& datacenters,
                                       TokenReplicasVec& result) const;
  void build_replicas_simple(const TokenHostVec& tokens, const DatacenterMap& datacenters,
                             TokenReplicasVec& result) const;
  void build_replicas_non_replicated(const TokenHostVec& tokens, const DatacenterMap& datacenters,
                                     TokenReplicasVec& result) const;

private:
  Type type_;
  ReplicationFactorMap replication_factors_;
};

template <class Partitioner>
void ReplicationStrategy<Partitioner>::init(IdGenerator& dc_ids,
                                            const VersionNumber& cassandra_version,
                                            const Row* row) {
  StringRef strategy_class;

  if (cassandra_version >= VersionNumber(3, 0, 0)) {
    const Value* value = row->get_by_name("replication");
    if (value && value->is_map() && is_string_type(value->primary_value_type()) &&
        is_string_type(value->secondary_value_type())) {
      MapIterator iterator(value);
      while (iterator.next()) {
        String key(iterator.key()->to_string());
        if (key == "class") {
          strategy_class = iterator.value()->to_string_ref();
        } else {
          String value(iterator.value()->to_string());
          size_t count = strtoul(value.c_str(), NULL, 10);
          if (count > 0) {
            ReplicationFactor replication_factor;
            replication_factor.count = count;
            replication_factor.name = key;
            if (key == "replication_factor") {
              replication_factors_[1] = replication_factor;
            } else {
              replication_factors_[dc_ids.get(key)] = replication_factor;
            }
          } else {
            LOG_WARN("Replication factor of 0 for option %s", key.c_str());
          }
        }
      }
    }
  } else {
    const Value* value;
    value = row->get_by_name("strategy_class");
    if (value && is_string_type(value->value_type())) {
      strategy_class = value->to_string_ref();
    }

    value = row->get_by_name("strategy_options");

    Vector<char> buf = value->decoder().as_vector();
    json::Document d;
    d.ParseInsitu(&buf[0]);

    if (!d.HasParseError() && d.IsObject()) {
      for (json::Value::ConstMemberIterator i = d.MemberBegin(); i != d.MemberEnd(); ++i) {
        String key(i->name.GetString(), i->name.GetStringLength());
        String value(i->value.GetString(), i->value.GetStringLength());
        size_t count = strtoul(value.c_str(), NULL, 10);
        if (count > 0) {
          ReplicationFactor replication_factor;
          replication_factor.count = count;
          replication_factor.name = key;
          if (key == "replication_factor") {
            replication_factors_[1] = replication_factor;
          } else {
            replication_factors_[dc_ids.get(key)] = replication_factor;
          }
        } else {
          LOG_WARN("Replication factor of 0 for option %s", key.c_str());
        }
      }
    }
  }

  if (ends_with(strategy_class, CASS_NETWORK_TOPOLOGY_STRATEGY)) {
    type_ = NETWORK_TOPOLOGY_STRATEGY;
  } else if (ends_with(strategy_class, CASS_SIMPLE_STRATEGY)) {
    type_ = SIMPLE_STRATEGY;
  }
}

template <class Partitioner>
void ReplicationStrategy<Partitioner>::build_replicas(const TokenHostVec& tokens,
                                                      const DatacenterMap& datacenters,
                                                      TokenReplicasVec& result) const {
  result.clear();
  result.reserve(tokens.size());

  switch (type_) {
    case NETWORK_TOPOLOGY_STRATEGY:
      build_replicas_network_topology(tokens, datacenters, result);
      break;
    case SIMPLE_STRATEGY:
      build_replicas_simple(tokens, datacenters, result);
      break;
    default:
      build_replicas_non_replicated(tokens, datacenters, result);
      break;
  }
}

// Adds unique replica. It returns true if the replica was added.
inline bool add_replica(CopyOnWriteHostVec& hosts, const Host::Ptr& host) {
  for (HostVec::const_reverse_iterator it = hosts->rbegin(); it != hosts->rend(); ++it) {
    if ((*it)->address() == host->address()) {
      return false; // Already in the replica set
    }
  }
  hosts->push_back(host);
  return true;
}

template <class Partitioner>
void ReplicationStrategy<Partitioner>::build_replicas_network_topology(
    const TokenHostVec& tokens, const DatacenterMap& datacenters, TokenReplicasVec& result) const {
  if (replication_factors_.empty()) {
    return;
  }

  DatacenterRackInfoMap dc_racks;
  dc_racks.resize(datacenters.size());

  size_t num_replicas = 0;

  // Populate the datacenter and rack information. Only considering valid
  // datacenters that actually have hosts. If there's a replication factor
  // for a datacenter that doesn't exist or has no node then it will not
  // be counted.
  for (ReplicationFactorMap::const_iterator i = replication_factors_.begin(),
                                            end = replication_factors_.end();
       i != end; ++i) {
    DatacenterMap::const_iterator j = datacenters.find(i->first);
    // Don't include datacenters that don't exist
    if (j != datacenters.end()) {
      // A replication factor cannot exceed the number of nodes in a datacenter
      size_t replication_factor = std::min<size_t>(i->second.count, j->second.num_nodes);
      num_replicas += replication_factor;
      DatacenterRackInfo dc_rack_info;
      dc_rack_info.replication_factor = replication_factor;
      dc_rack_info.rack_count = j->second.racks.size();
      dc_racks[j->first] = dc_rack_info;
    } else {
      LOG_WARN("No nodes in datacenter '%s'. Check your replication strategies.",
               i->second.name.c_str());
    }
  }

  if (num_replicas == 0) {
    return;
  }

  for (typename TokenHostVec::const_iterator i = tokens.begin(), end = tokens.end(); i != end;
       ++i) {
    Token token = i->first;
    typename TokenHostVec::const_iterator token_it = i;

    CopyOnWriteHostVec replicas(new HostVec());
    replicas->reserve(num_replicas);

    // Clear datacenter and rack information for the next token
    for (typename DatacenterRackInfoMap::iterator j = dc_racks.begin(), end = dc_racks.end();
         j != end; ++j) {
      j->second.replica_count = 0;
      j->second.racks_observed.clear();
      j->second.skipped_endpoints.clear();
    }

    for (typename TokenHostVec::const_iterator j = tokens.begin(), end = tokens.end();
         j != end && replicas->size() < num_replicas; ++j) {
      typename TokenHostVec::const_iterator curr_token_it = token_it;
      Host* host = curr_token_it->second;
      uint32_t dc = host->dc_id();
      uint32_t rack = host->rack_id();

      ++token_it;
      if (token_it == tokens.end()) {
        token_it = tokens.begin();
      }

      typename DatacenterRackInfoMap::iterator dc_rack_it = dc_racks.find(dc);
      if (dc_rack_it == dc_racks.end()) {
        continue;
      }

      DatacenterRackInfo& dc_rack_info = dc_rack_it->second;

      size_t& replica_count_this_dc = dc_rack_info.replica_count;
      const size_t replication_factor = dc_rack_info.replication_factor;

      if (replica_count_this_dc >= replication_factor) {
        continue;
      }

      RackSet& racks_observed_this_dc = dc_rack_info.racks_observed;
      const size_t rack_count_this_dc = dc_rack_info.rack_count;

      // First, attempt to distribute replicas over all possible racks in a
      // datacenter only then consider hosts in the same rack

      if (rack == 0 || racks_observed_this_dc.size() == rack_count_this_dc) {
        if (add_replica(replicas, Host::Ptr(host))) {
          ++replica_count_this_dc;
        }
      } else {
        TokenHostQueue& skipped_endpoints_this_dc = dc_rack_info.skipped_endpoints;
        if (racks_observed_this_dc.count(rack) > 0) {
          skipped_endpoints_this_dc.push_back(curr_token_it);
        } else {
          if (add_replica(replicas, Host::Ptr(host))) {
            ++replica_count_this_dc;
            racks_observed_this_dc.insert(rack);
          }

          // Once we visited every rack in the current datacenter then starting considering
          // hosts we've already skipped.
          if (racks_observed_this_dc.size() == rack_count_this_dc) {
            while (!skipped_endpoints_this_dc.empty() &&
                   replica_count_this_dc < replication_factor) {
              if (add_replica(replicas, Host::Ptr(skipped_endpoints_this_dc.front()->second))) {
                ++replica_count_this_dc;
              }
              skipped_endpoints_this_dc.pop_front();
            }
          }
        }
      }
    }

    result.push_back(TokenReplicas(token, replicas));
  }
}

template <class Partitioner>
void ReplicationStrategy<Partitioner>::build_replicas_simple(const TokenHostVec& tokens,
                                                             const DatacenterMap& not_used,
                                                             TokenReplicasVec& result) const {
  ReplicationFactorMap::const_iterator it = replication_factors_.find(1);
  if (it == replication_factors_.end()) {
    return;
  }
  const size_t num_tokens = tokens.size();
  const size_t num_replicas = std::min<size_t>(it->second.count, num_tokens);
  for (typename TokenHostVec::const_iterator i = tokens.begin(), end = tokens.end(); i != end;
       ++i) {
    CopyOnWriteHostVec replicas(new HostVec());
    replicas->reserve(num_replicas);
    typename TokenHostVec::const_iterator token_it = i;
    for (size_t j = 0; j < num_tokens && replicas->size() < num_replicas; ++j) {
      add_replica(replicas, Host::Ptr(Host::Ptr(token_it->second)));
      ++token_it;
      if (token_it == tokens.end()) {
        token_it = tokens.begin();
      }
    }
    result.push_back(TokenReplicas(i->first, replicas));
  }
}

template <class Partitioner>
void ReplicationStrategy<Partitioner>::build_replicas_non_replicated(
    const TokenHostVec& tokens, const DatacenterMap& not_used, TokenReplicasVec& result) const {
  for (typename TokenHostVec::const_iterator i = tokens.begin(); i != tokens.end(); ++i) {
    CopyOnWriteHostVec replicas(new HostVec(1, Host::Ptr(i->second)));
    result.push_back(TokenReplicas(i->first, replicas));
  }
}

template <class Partitioner>
class TokenMapImpl : public TokenMap {
public:
  typedef typename Partitioner::Token Token;

  typedef std::pair<Token, Host*> TokenHost;
  typedef Vector<TokenHost> TokenHostVec;

  struct TokenHostCompare {
    bool operator()(const TokenHost& lhs, const TokenHost& rhs) const {
      return lhs.first < rhs.first;
    }
  };

  struct RemoveTokenHostIf {
    RemoveTokenHostIf(const Host::Ptr& host)
        : host(host) {}

    bool operator()(const TokenHost& token) const {
      if (!token.second) {
        return false;
      }
      return token.second->address() == host->address();
    }

    const Host::Ptr& host;
  };

  typedef std::pair<Token, CopyOnWriteHostVec> TokenReplicas;
  typedef Vector<TokenReplicas> TokenReplicasVec;

  struct TokenReplicasCompare {
    bool operator()(const TokenReplicas& lhs, const TokenReplicas& rhs) const {
      return lhs.first < rhs.first;
    }
  };

  typedef DenseHashMap<String, TokenReplicasVec> KeyspaceReplicaMap;
  typedef DenseHashMap<String, ReplicationStrategy<Partitioner> > KeyspaceStrategyMap;

  TokenMapImpl()
      : no_replicas_dummy_(NULL) {
    replicas_.set_empty_key(String());
    replicas_.set_deleted_key(String(1, '\0'));
    strategies_.set_empty_key(String());
    strategies_.set_deleted_key(String(1, '\0'));
  }

  TokenMapImpl(const TokenMapImpl& other)
      : tokens_(other.tokens_)
      , hosts_(other.hosts_)
      , replicas_(other.replicas_)
      , strategies_(other.strategies_)
      , rack_ids_(other.rack_ids_)
      , dc_ids_(other.dc_ids_)
      , no_replicas_dummy_(NULL) {}

  virtual void add_host(const Host::Ptr& host);
  virtual void update_host_and_build(const Host::Ptr& host);
  virtual void remove_host_and_build(const Host::Ptr& host);

  virtual void add_keyspaces(const VersionNumber& cassandra_version, const ResultResponse* result);
  virtual void update_keyspaces_and_build(const VersionNumber& cassandra_version,
                                          const ResultResponse* result);
  virtual void drop_keyspace(const String& keyspace_name);

  virtual void build();

  virtual TokenMap::Ptr copy() const;

  virtual const CopyOnWriteHostVec& get_replicas(const String& keyspace_name,
                                                 const String& routing_key) const;

  virtual String dump(const String& keyspace_name) const;

public:
  // Testing only

  bool contains(const Token& token) const {
    for (typename TokenHostVec::const_iterator i = tokens_.begin(), end = tokens_.end(); i != end;
         ++i) {
      if (token == i->first) return true;
    }
    return false;
  }

  const TokenReplicasVec& token_replicas(const String& keyspace_name) const;

private:
  void update_keyspace(const VersionNumber& cassandra_version, const ResultResponse* result,
                       bool should_build_replicas);
  void remove_host_tokens(const Host::Ptr& host);
  void update_host_ids(const Host::Ptr& host);
  void build_replicas();

private:
  TokenHostVec tokens_;
  HostSet hosts_;
  DatacenterMap datacenters_;
  KeyspaceReplicaMap replicas_;
  KeyspaceStrategyMap strategies_;
  IdGenerator rack_ids_;
  IdGenerator dc_ids_;
  CopyOnWriteHostVec no_replicas_dummy_;
};

template <class Partitioner>
void TokenMapImpl<Partitioner>::add_host(const Host::Ptr& host) {
  update_host_ids(host);
  hosts_.insert(host);

  const Vector<String>& tokens(host->tokens());
  for (Vector<String>::const_iterator it = tokens.begin(), end = tokens.end(); it != end; ++it) {
    Token token = Partitioner::from_string(*it);
    tokens_.push_back(TokenHost(token, host.get()));
  }
}

template <class Partitioner>
void TokenMapImpl<Partitioner>::update_host_and_build(const Host::Ptr& host) {
  uint64_t start = uv_hrtime();
  remove_host_tokens(host);

  update_host_ids(host);
  hosts_.insert(host);

  TokenHostVec new_tokens;
  const Vector<String>& tokens(host->tokens());
  for (Vector<String>::const_iterator it = tokens.begin(), end = tokens.end(); it != end; ++it) {
    Token token = Partitioner::from_string(*it);
    new_tokens.push_back(TokenHost(token, host.get()));
  }

  std::sort(new_tokens.begin(), new_tokens.end());

  TokenHostVec merged(tokens_.size() + new_tokens.size());
  std::merge(tokens_.begin(), tokens_.end(), new_tokens.begin(), new_tokens.end(), merged.begin(),
             TokenHostCompare());
  tokens_ = merged;

  build_replicas();
  LOG_DEBUG("Updated token map with host %s (%u tokens). Rebuilt token map with %u hosts and %u "
            "tokens in %f ms",
            host->address_string().c_str(), (unsigned int)new_tokens.size(),
            (unsigned int)hosts_.size(), (unsigned int)tokens_.size(),
            (double)(uv_hrtime() - start) / (1000.0 * 1000.0));
}

template <class Partitioner>
void TokenMapImpl<Partitioner>::remove_host_and_build(const Host::Ptr& host) {
  if (hosts_.find(host) == hosts_.end()) return;
  uint64_t start = uv_hrtime();
  remove_host_tokens(host);
  hosts_.erase(host);
  build_replicas();
  LOG_DEBUG(
      "Removed host %s from token map. Rebuilt token map with %u hosts and %u tokens in %f ms",
      host->address_string().c_str(), (unsigned int)hosts_.size(), (unsigned int)tokens_.size(),
      (double)(uv_hrtime() - start) / (1000.0 * 1000.0));
}

template <class Partitioner>
void TokenMapImpl<Partitioner>::add_keyspaces(const VersionNumber& cassandra_version,
                                              const ResultResponse* result) {
  update_keyspace(cassandra_version, result, false);
}

template <class Partitioner>
void TokenMapImpl<Partitioner>::update_keyspaces_and_build(const VersionNumber& cassandra_version,
                                                           const ResultResponse* result) {
  update_keyspace(cassandra_version, result, true);
}

template <class Partitioner>
void TokenMapImpl<Partitioner>::drop_keyspace(const String& keyspace_name) {
  replicas_.erase(keyspace_name);
  strategies_.erase(keyspace_name);
}

template <class Partitioner>
void TokenMapImpl<Partitioner>::build() {
  uint64_t start = uv_hrtime();
  std::sort(tokens_.begin(), tokens_.end());
  build_replicas();
  LOG_DEBUG("Built token map with %u hosts and %u tokens in %f ms", (unsigned int)hosts_.size(),
            (unsigned int)tokens_.size(), (double)(uv_hrtime() - start) / (1000.0 * 1000.0));
}

template <class Partitioner>
TokenMap::Ptr TokenMapImpl<Partitioner>::copy() const {
  return Ptr(new TokenMapImpl<Partitioner>(*this));
}

template <class Partitioner>
const CopyOnWriteHostVec& TokenMapImpl<Partitioner>::get_replicas(const String& keyspace_name,
                                                                  const String& routing_key) const {
  typename KeyspaceReplicaMap::const_iterator ks_it = replicas_.find(keyspace_name);

  if (ks_it != replicas_.end()) {
    Token token = Partitioner::hash(routing_key);
    const TokenReplicasVec& replicas = ks_it->second;
    typename TokenReplicasVec::const_iterator replicas_it =
        std::upper_bound(replicas.begin(), replicas.end(), TokenReplicas(token, no_replicas_dummy_),
                         TokenReplicasCompare());
    if (replicas_it != replicas.end()) {
      return replicas_it->second;
    } else if (!replicas.empty()) {
      return replicas.front().second;
    }
  }

  return no_replicas_dummy_;
}

template <class Partitioner>
String TokenMapImpl<Partitioner>::dump(const String& keyspace_name) const {
  String result;
  typename KeyspaceReplicaMap::const_iterator ks_it = replicas_.find(keyspace_name);
  const TokenReplicasVec& replicas = ks_it->second;

  for (typename TokenReplicasVec::const_iterator it = replicas.begin(), end = replicas.end();
       it != end; ++it) {
    OStringStream ss;
    ss << std::setw(20) << it->first << " [ ";
    const CopyOnWriteHostVec& hosts = it->second;
    for (HostVec::const_iterator host_it = hosts->begin(), end = hosts->end(); host_it != end;
         ++host_it) {
      ss << (*host_it)->address_string() << " ";
    }
    ss << "]\n";
    result.append(ss.str());
  }
  return result;
}

template <class Partitioner>
const typename TokenMapImpl<Partitioner>::TokenReplicasVec&
TokenMapImpl<Partitioner>::token_replicas(const String& keyspace_name) const {
  typename KeyspaceReplicaMap::const_iterator ks_it = replicas_.find(keyspace_name);
  static TokenReplicasVec not_found;
  return ks_it != replicas_.end() ? ks_it->second : not_found;
}

template <class Partitioner>
void TokenMapImpl<Partitioner>::update_keyspace(const VersionNumber& cassandra_version,
                                                const ResultResponse* result,
                                                bool should_build_replicas) {
  ResultIterator rows(result);

  while (rows.next()) {
    String keyspace_name;
    const Row* row = rows.row();

    if (!row->get_string_by_name("keyspace_name", &keyspace_name)) {
      LOG_ERROR("Unable to get column value for 'keyspace_name'");
      continue;
    }

    ReplicationStrategy<Partitioner> strategy;

    strategy.init(dc_ids_, cassandra_version, row);

    typename KeyspaceStrategyMap::iterator i = strategies_.find(keyspace_name);
    if (i == strategies_.end() || i->second != strategy) {
      if (i == strategies_.end()) {
        strategies_[keyspace_name] = strategy;
      } else {
        i->second = strategy;
      }
      if (should_build_replicas) {
        uint64_t start = uv_hrtime();
        build_datacenters(hosts_, datacenters_);
        strategy.build_replicas(tokens_, datacenters_, replicas_[keyspace_name]);
        LOG_DEBUG("Updated token map with keyspace '%s'. Rebuilt token map with %u hosts and %u "
                  "tokens in %f ms",
                  keyspace_name.c_str(), (unsigned int)hosts_.size(), (unsigned int)tokens_.size(),
                  (double)(uv_hrtime() - start) / (1000.0 * 1000.0));
      }
    }
  }
}

template <class Partitioner>
void TokenMapImpl<Partitioner>::remove_host_tokens(const Host::Ptr& host) {
  typename TokenHostVec::iterator last =
      std::remove_copy_if(tokens_.begin(), tokens_.end(), tokens_.begin(), RemoveTokenHostIf(host));
  tokens_.resize(last - tokens_.begin());
}

template <class Partitioner>
void TokenMapImpl<Partitioner>::update_host_ids(const Host::Ptr& host) {
  host->set_rack_and_dc_ids(rack_ids_.get(host->rack()), dc_ids_.get(host->dc()));
}

template <class Partitioner>
void TokenMapImpl<Partitioner>::build_replicas() {
  build_datacenters(hosts_, datacenters_);
  for (typename KeyspaceStrategyMap::const_iterator i = strategies_.begin(),
                                                    end = strategies_.end();
       i != end; ++i) {
    const String& keyspace_name = i->first;
    const ReplicationStrategy<Partitioner>& strategy = i->second;
    strategy.build_replicas(tokens_, datacenters_, replicas_[keyspace_name]);
    LOG_TRACE("Replicas for keyspace '%s':\n%s", keyspace_name.c_str(),
              dump(keyspace_name).c_str());
  }
}

}}} // namespace datastax::internal::core

#endif
