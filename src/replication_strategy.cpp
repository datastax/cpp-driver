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

#include "replication_strategy.hpp"

#include "common.hpp"
#include "map_iterator.hpp"
#include "token_map.hpp"

#include "third_party/boost/boost/algorithm/string/predicate.hpp"

#include <map>
#include <set>

namespace cass {

SharedRefPtr<ReplicationStrategy> ReplicationStrategy::from_keyspace_meta(const KeyspaceMetadata& ks_meta) {
  std::string strategy_class = ks_meta.strategy_class();

  SharedRefPtr<ReplicationStrategy> strategy;
  if (boost::ends_with(strategy_class, NetworkTopologyStrategy::STRATEGY_CLASS)) {
    return SharedRefPtr<ReplicationStrategy>(
          new NetworkTopologyStrategy(strategy_class, ks_meta.strategy_options()));
  } else if (boost::ends_with(strategy_class, SimpleStrategy::STRATEGY_CLASS)) {
    return SharedRefPtr<ReplicationStrategy>(
          new SimpleStrategy(strategy_class, ks_meta.strategy_options()));
  } else {
    return SharedRefPtr<ReplicationStrategy>(new NonReplicatedStrategy(strategy_class));
  }
}


const std::string NetworkTopologyStrategy::STRATEGY_CLASS("NetworkTopologyStrategy");

NetworkTopologyStrategy::NetworkTopologyStrategy(const std::string& strategy_class,
                                                 const SchemaMetadataField* strategy_options)
  : ReplicationStrategy(strategy_class) {
  build_dc_replicas(strategy_options, &dc_replicas_);
}

bool NetworkTopologyStrategy::equal(const KeyspaceMetadata& ks_meta) {
  if (strategy_class_ != ks_meta.strategy_class()) return false;
  DCReplicaCountMap temp_dc_replicas;
  build_dc_replicas(ks_meta.strategy_options(), &temp_dc_replicas);
  return dc_replicas_ == temp_dc_replicas;
}

typedef std::map<std::string, std::set<std::string> > DCRackMap;
DCRackMap map_dc_racks(const TokenHostMap& token_hosts) {
  DCRackMap dc_racks;
  for (TokenHostMap::const_iterator i = token_hosts.begin();
       i != token_hosts.end(); ++i) {
    const std::string& dc = i->second->dc();
    const std::string& rack = i->second->rack();
    if (!dc.empty() &&  !rack.empty()) {
      dc_racks[dc].insert(rack);
    }
  }
  return dc_racks;
}

void NetworkTopologyStrategy::tokens_to_replicas(const TokenHostMap& primary, TokenReplicaMap* output) const {
  DCRackMap dc_rack_map = map_dc_racks(primary);
  output->clear();
  for (TokenHostMap::const_iterator i = primary.begin(); i != primary.end(); ++i) {
    DCReplicaCountMap dc_replica_count;
    typedef std::map<std::string, std::set<std::string> > DCRackSetMap;
    DCRackSetMap dc_racks_observed;
    typedef std::map<std::string, std::list<SharedRefPtr<Host> > > DCHostListMap;
    DCHostListMap dc_skipped_endpoints;
    for (DCReplicaCountMap::const_iterator j = dc_replicas_.begin(); j != dc_replicas_.end(); ++j) {
      dc_replica_count[j->first] = 0;
    }

    CopyOnWriteHostVec token_replicas(new HostVec());
    TokenHostMap::const_iterator j = i;
    size_t token_count = 0;
    do {
      const SharedRefPtr<Host>& host = j->second;
      const std::string& dc = host->dc();
      DCReplicaCountMap::const_iterator dc_repl_itr;
      dc_replicas_.find(dc);
      if (dc.empty() || (dc_repl_itr = dc_replicas_.find(dc)) == dc_replicas_.end()) {
        continue;
      }

      const size_t target_replicas = dc_repl_itr->second;
      size_t& dc_replicas_found = dc_replica_count[dc];
      if (dc_replicas_found >= target_replicas) {
        continue;
      }

      const size_t dc_rack_count = dc_rack_map[dc].size();
      std::set<std::string>& racks_observed_this_dc = dc_racks_observed[dc];
      const std::string& rack = host->rack();
      if (rack.empty() || racks_observed_this_dc.size() == dc_rack_count) {
        token_replicas->push_back(host);
        ++dc_replicas_found;
      } else {
        std::list<SharedRefPtr<Host> >& skipped_endpoints_this_dc = dc_skipped_endpoints[dc];
        if (racks_observed_this_dc.find(rack) != racks_observed_this_dc.end()) {
          skipped_endpoints_this_dc.push_back(host);
        } else {
          token_replicas->push_back(host);
          ++dc_replicas_found;
          racks_observed_this_dc.insert(rack);
          if (racks_observed_this_dc.size() == dc_rack_count) {
            while (!skipped_endpoints_this_dc.empty() && dc_replicas_found < target_replicas) {
              token_replicas->push_back(skipped_endpoints_this_dc.front());
              skipped_endpoints_this_dc.pop_front();
              ++dc_replicas_found;
            }
          }
        }
      }

      if (dc_replica_count == dc_replicas_) {
        break;
      }

      ++j;
      if (j == primary.end()) {
        j = primary.begin();
      }
      ++token_count;
    } while(token_count < primary.size());

    output->insert(std::make_pair(i->first, token_replicas));
  }
}

void NetworkTopologyStrategy::build_dc_replicas(const SchemaMetadataField* strategy_options,
                                                NetworkTopologyStrategy::DCReplicaCountMap* output) {
  if (strategy_options != NULL) {
    MapIterator itr(strategy_options->value());
    while (itr.next()) {
      boost::string_ref key = itr.key()->buffer().to_string_ref();
      boost::string_ref value = itr.value()->buffer().to_string_ref();
      if (key != "class") {
        size_t replica_count = strtoul(value.to_string().c_str(), NULL, 10);
        if (replica_count > 0) {
          (*output)[key.to_string()] = replica_count;
        }
      }
    }
  }
}


const std::string SimpleStrategy::STRATEGY_CLASS("SimpleStrategy");

SimpleStrategy::SimpleStrategy(const std::string& strategy_class,
                               const SchemaMetadataField* strategy_options)
  : ReplicationStrategy(strategy_class)
  , replication_factor_(0) {
  replication_factor_ = get_replication_factor(strategy_options);
  if (replication_factor_ == 0) {
    // TODO: Global logging
  }
}

bool SimpleStrategy::equal(const KeyspaceMetadata& ks_meta) {
  if (strategy_class_ != ks_meta.strategy_class()) return false;
  return replication_factor_ == get_replication_factor(ks_meta.strategy_options());
}

void SimpleStrategy::tokens_to_replicas(const TokenHostMap& primary, TokenReplicaMap* output) const {
  size_t target_replicas = std::min<size_t>(replication_factor_, primary.size());
  output->clear();
  for (TokenHostMap::const_iterator i = primary.begin(); i != primary.end(); ++i) {
    CopyOnWriteHostVec token_replicas(new HostVec());
    TokenHostMap::const_iterator j = i;
    do {
      token_replicas->push_back(j->second);
      ++j;
      if (j == primary.end()) {
        j = primary.begin();
      }
    } while(token_replicas->size() < target_replicas);
    output->insert(std::make_pair(i->first, token_replicas));
  }
}

size_t SimpleStrategy::get_replication_factor(const SchemaMetadataField* strategy_options) {
  if (strategy_options != NULL) {
    MapIterator itr(strategy_options->value());
    while (itr.next()) {
      boost::string_ref key = itr.key()->buffer().to_string_ref();
      boost::string_ref value = itr.value()->buffer().to_string_ref();
      if (key == "replication_factor") {
        return strtoul(value.to_string().c_str(), NULL, 10);
      }
    }
  }

  return 0;
}

bool NonReplicatedStrategy::equal(const KeyspaceMetadata& ks_meta) {
  if (strategy_class_ != ks_meta.strategy_class()) return false;
  return true;
}

void NonReplicatedStrategy::tokens_to_replicas(const TokenHostMap& primary, TokenReplicaMap* output) const {
  output->clear();
  for (TokenHostMap::const_iterator i = primary.begin(); i != primary.end(); ++i) {
    CopyOnWriteHostVec token_replicas(new HostVec(1, i->second));
    output->insert(std::make_pair(i->first, token_replicas));
  }
}

}
