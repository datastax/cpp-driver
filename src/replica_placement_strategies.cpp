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

#include "replica_placement_strategies.hpp"

#include "common.hpp"
#include "token_map.hpp"

#include "third_party/boost/boost/algorithm/string/predicate.hpp"

#include <map>
#include <set>

namespace cass {

SharedRefPtr<ReplicaPlacementStrategy> ReplicaPlacementStrategy::from_keyspace_meta(const KeyspaceMetadata& ks_meta) {
  const std::string& strategy_class = ks_meta.strategy();

  if (boost::ends_with(strategy_class, NetworkTopologyStrategy::STRATEGY_CLASS)) {
    return SharedRefPtr<ReplicaPlacementStrategy>(new NetworkTopologyStrategy(ks_meta.strategy_options()));
  } else
  if (boost::ends_with(strategy_class, SimpleStrategy::STRATEGY_CLASS)) {
    return SharedRefPtr<ReplicaPlacementStrategy>(new SimpleStrategy(ks_meta.strategy_options()));
  } else {
    return SharedRefPtr<ReplicaPlacementStrategy>(new NonReplicatedStrategy());
  }
}


const std::string NetworkTopologyStrategy::STRATEGY_CLASS("NetworkTopologyStrategy");

NetworkTopologyStrategy::NetworkTopologyStrategy(const KeyspaceMetadata::StrategyOptions& options) {
  for (KeyspaceMetadata::StrategyOptions::const_iterator i = options.begin();
       i != options.end(); ++i) {
    if (i->first != "class") {
      size_t replica_count = strtoull(i->second.c_str(), NULL, 10);
      if (replica_count > 0) {
        dc_replicas_[i->first] = replica_count;
      }
    }
  }
}

bool NetworkTopologyStrategy::equals(const ReplicaPlacementStrategy& other) {
  const NetworkTopologyStrategy* other_nts = dynamic_cast<const NetworkTopologyStrategy*>(&other);
  return (other_nts != NULL) &&
         (dc_replicas_ == other_nts->dc_replicas_);
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
    typedef std::map<std::string, size_t> DCCountMap;
    DCCountMap dc_replica_count;
    typedef std::map<std::string, std::set<std::string> > DCRackSetMap;
    DCRackSetMap dc_racks_observed;
    typedef std::map<std::string, std::list<SharedRefPtr<Host> > > DCHostListMap;
    DCHostListMap dc_skipped_endpoints;
    for (DCCountMap::const_iterator j = dc_replicas_.begin(); j != dc_replicas_.end(); ++j) {
      dc_replica_count[j->first] = 0;
    }

    HostVec* token_replicas = new HostVec();
    TokenHostMap::const_iterator j = i;
    size_t token_count = 0;
    do {
      const SharedRefPtr<Host>& host = j->second;
      const std::string& dc = host->dc();
      DCCountMap::const_iterator dc_repl_itr;
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

    output->insert(std::make_pair(i->first, CopyOnWriteHostVec(token_replicas)));
  }
}


const std::string SimpleStrategy::STRATEGY_CLASS("SimpleStrategy");

SimpleStrategy::SimpleStrategy(const KeyspaceMetadata::StrategyOptions& options)
  : replication_factor_(0) {
  KeyspaceMetadata::StrategyOptions::const_iterator i
      = options.find("replication_factor");
  if (i != options.end()) {
    replication_factor_ = strtoull(i->second.c_str(), NULL, 10);
  }
}

bool SimpleStrategy::equals(const ReplicaPlacementStrategy& other) {
  const SimpleStrategy* other_ss = dynamic_cast<const SimpleStrategy*>(&other);
  return (other_ss != NULL) &&
         (replication_factor_ == other_ss->replication_factor_);
}

void SimpleStrategy::tokens_to_replicas(const TokenHostMap& primary, TokenReplicaMap* output) const {
  size_t target_replicas = std::min<size_t>(replication_factor_, primary.size());
  output->clear();
  for (TokenHostMap::const_iterator i = primary.begin(); i != primary.end(); ++i) {
    HostVec* token_replicas = new HostVec();
    TokenHostMap::const_iterator j = i;
    do {
      token_replicas->push_back(j->second);
      ++j;
      if (j == primary.end()) {
        j = primary.begin();
      }
    } while(token_replicas->size() < target_replicas);
    output->insert(std::make_pair(i->first, CopyOnWriteHostVec(token_replicas)));
  }
}


bool NonReplicatedStrategy::equals(const ReplicaPlacementStrategy& other) {
  const NonReplicatedStrategy* other_nrs = dynamic_cast<const NonReplicatedStrategy*>(&other);
  return (other_nrs != NULL);
}

void NonReplicatedStrategy::tokens_to_replicas(const TokenHostMap& primary, TokenReplicaMap* output) const {
  output->clear();
  for (TokenHostMap::const_iterator i = primary.begin(); i != primary.end(); ++i) {
    HostVec* token_replicas = new HostVec();
    token_replicas->resize(1);
    (*token_replicas)[0] = i->second;
    output->insert(std::make_pair(i->first, CopyOnWriteHostVec(token_replicas)));
  }
}

}
