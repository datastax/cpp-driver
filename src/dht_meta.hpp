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

#ifndef __CASS_DHT_META_HPP_INCLUDED__
#define __CASS_DHT_META_HPP_INCLUDED__

#include "host.hpp"
#include "scoped_ptr.hpp"
#include "schema_metadata.hpp"

#include "third_party/boost/boost/utility/string_ref.hpp"

#include <map>
#include <vector>

namespace cass {

typedef std::vector<boost::string_ref> TokenStringList;
typedef std::map<std::string, SharedRefPtr<Host> > TokenStringHostMap;

class TokenMap {
public:
  virtual ~TokenMap() {}

  virtual void clear() = 0;
  virtual void update(SharedRefPtr<Host>& host, const TokenStringList& tokens) = 0;
  virtual void update(const std::string& keyspace_name) = 0;
};

template <typename T>
class TypedTokenMap : public TokenMap {
public:
  virtual ~TypedTokenMap() {}

  virtual void clear() { token_map_.clear(); keyspace_token_map_.clear(); }

  void update(SharedRefPtr<Host>& host, const TokenStringList& token_strings) {
    for (TokenStringList::const_iterator i = token_strings.begin();
         i != token_strings.end(); ++i) {
      token_map_[token_from_string_ref(*i)] = host;
    }
  }

  void update(const std::string& keyspace_name/*, Strategy */) {
  }

  virtual T token_from_string(const std::string& token_string) = 0;
  virtual T token_from_string_ref(const boost::string_ref& token_string_ref) = 0;

protected:
  std::map<T, SharedRefPtr<Host> > token_map_;
  std::map<T, std::vector<SharedRefPtr<Host> > > keyspace_token_map_;
};

class M3PTokenMap : public TypedTokenMap<int64_t> {
public:
  static const std::string PARTIONER_CLASS;

  virtual int64_t token_from_string(const std::string& token_string);
  virtual int64_t token_from_string_ref(const boost::string_ref& token_string_ref);
};

class ReplicaPlacementStrategy {
public:
  static ReplicaPlacementStrategy* from_keyspace_meta(const KeyspaceMetadata& ks_meta);
};

class NetworkTopologyStrategy : public ReplicaPlacementStrategy {
public:
  NetworkTopologyStrategy(const StrategyOptionsMap& options) {/*TBD*/}
  static const std::string STRATEGY_CLASS;
};

class SimpleStrategy : public ReplicaPlacementStrategy {
public:
  SimpleStrategy(const StrategyOptionsMap& options) {/*TBD*/}
  static const std::string STRATEGY_CLASS;
};

class NonReplicatedStrategy : public ReplicaPlacementStrategy {
public:
};

class DHTMeta {
public:
  void clear() { token_map_.reset(); }
  void set_partitioner(const std::string& partitioner_class);
  void update_host(SharedRefPtr<Host>& host, const TokenStringList& tokens);
  void update_keyspace(const KeyspaceModel& ks_name);

private:
  ScopedPtr<TokenMap> token_map_;

//  const KeyspaceModelMap& ks_models_;
//  const HostMap& hosts_;
};

} // namespace cass

#endif
