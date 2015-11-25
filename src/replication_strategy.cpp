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

#include "replication_strategy.hpp"

#include "logger.hpp"
#include "map_iterator.hpp"
#include "metadata.hpp"
#include "token_map.hpp"
#include "utils.hpp"

#include <map>
#include <set>

namespace cass {

static void build_dc_replicas(const KeyspaceMetadata::OptionsMap& strategy_options,
                              NetworkTopologyStrategy::DCReplicaCountMap* output) {
  for (KeyspaceMetadata::OptionsMap::const_iterator i = strategy_options.begin(),
       end = strategy_options.end();
       i != end; ++i) {
    if (i->first != "class") {
      size_t replication_factor = strtoul(i->second.to_string().c_str(), NULL, 10);
      if (replication_factor > 0) {
        (*output)[i->first.to_string()] = replication_factor;
      }
    }
  }
}

static size_t get_replication_factor(const KeyspaceMetadata::OptionsMap& strategy_options) {
  size_t replication_factor = 0;
  KeyspaceMetadata::OptionsMap::const_iterator i = strategy_options.find("replication_factor");
  if (i != strategy_options.end()) {
    replication_factor = strtoul(i->second.to_string().c_str(), NULL, 10);
  }
  if (replication_factor == 0) {
    LOG_WARN("Replication factor of 0");
  }
  return replication_factor;
}


SharedRefPtr<ReplicationStrategy> ReplicationStrategy::from_keyspace_meta(const KeyspaceMetadata& ks_meta) {
  StringRef strategy_class = ks_meta.strategy_class();

  SharedRefPtr<ReplicationStrategy> strategy;
  if (ends_with(strategy_class, NetworkTopologyStrategy::STRATEGY_CLASS)) {
    NetworkTopologyStrategy::DCReplicaCountMap replication_factors;
    build_dc_replicas(ks_meta.strategy_options(), &replication_factors);
    return SharedRefPtr<ReplicationStrategy>(new NetworkTopologyStrategy(strategy_class.to_string(),
                                                                         replication_factors));
  } else if (ends_with(strategy_class, SimpleStrategy::STRATEGY_CLASS)) {
    size_t replication_factor = get_replication_factor(ks_meta.strategy_options());
    return SharedRefPtr<ReplicationStrategy>(new SimpleStrategy(strategy_class.to_string(), replication_factor));
  } else {
    return SharedRefPtr<ReplicationStrategy>(new NonReplicatedStrategy(strategy_class.to_string()));
  }
}

const std::string NetworkTopologyStrategy::STRATEGY_CLASS("NetworkTopologyStrategy");

bool NetworkTopologyStrategy::equal(const KeyspaceMetadata& ks_meta) {
  if (ks_meta.strategy_class() != strategy_class_) return false;
  DCReplicaCountMap temp_rfs;
  build_dc_replicas(ks_meta.strategy_options(), &temp_rfs);
  return replication_factors_ == temp_rfs;
}

typedef std::map<std::string, std::set<std::string> > DCRackMap;
static DCRackMap racks_in_dcs(const TokenHostMap& token_hosts) {
  DCRackMap racks;
  for (TokenHostMap::const_iterator i = token_hosts.begin();
       i != token_hosts.end(); ++i) {
    const std::string& dc = i->second->dc();
    const std::string& rack = i->second->rack();
    if (!dc.empty() &&  !rack.empty()) {
      racks[dc].insert(rack);
    }
  }
  return racks;
}

void NetworkTopologyStrategy::tokens_to_replicas(const TokenHostMap& primary, TokenReplicaMap* output) const {
  DCRackMap racks = racks_in_dcs(primary);

  output->clear();

  for (TokenHostMap::const_iterator i = primary.begin(); i != primary.end(); ++i) {
    DCReplicaCountMap replica_counts;
    std::map<std::string, std::set<std::string> > racks_observed;
    std::map<std::string, std::list<SharedRefPtr<Host> > > skipped_endpoints;

    CopyOnWriteHostVec replicas(new HostVec());
    TokenHostMap::const_iterator j = i;
    for (size_t count = 0; count < primary.size() && replica_counts != replication_factors_; ++count) {
      const SharedRefPtr<Host>& host = j->second;
      const std::string& dc = host->dc();

      ++j;
      if (j == primary.end()) {
        j = primary.begin();
      }

      DCReplicaCountMap::const_iterator rf_it =  replication_factors_.find(dc);
      if (dc.empty() || rf_it == replication_factors_.end()) {
        continue;
      }

      const size_t rf = rf_it->second;
      size_t& replica_count_this_dc = replica_counts[dc];
      if (replica_count_this_dc >= rf) {
        continue;
      }

      const size_t rack_count_this_dc = racks[dc].size();
      std::set<std::string>& racks_observed_this_dc = racks_observed[dc];
      const std::string& rack = host->rack();

      if (rack.empty() || racks_observed_this_dc.size() == rack_count_this_dc) {
        ++replica_count_this_dc;
        replicas->push_back(host);
      } else {
        if (racks_observed_this_dc.count(rack) > 0) {
          skipped_endpoints[dc].push_back(host);
        } else {
          ++replica_count_this_dc;
          replicas->push_back(host);
          racks_observed_this_dc.insert(rack);

          if (racks_observed_this_dc.size() == rack_count_this_dc) {
            std::list<SharedRefPtr<Host> >& skipped_endpoints_this_dc = skipped_endpoints[dc];
            while (!skipped_endpoints_this_dc.empty() && replica_count_this_dc < rf) {
              ++replica_count_this_dc;
              replicas->push_back(skipped_endpoints_this_dc.front());
              skipped_endpoints_this_dc.pop_front();
            }
          }
        }
      }
    }

    output->insert(std::make_pair(i->first, replicas));
  }
}

const std::string SimpleStrategy::STRATEGY_CLASS("SimpleStrategy");

bool SimpleStrategy::equal(const KeyspaceMetadata& ks_meta) {
  if (ks_meta.strategy_class() != strategy_class_) return false;
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
    } while (token_replicas->size() < target_replicas);
    output->insert(std::make_pair(i->first, token_replicas));
  }
}

bool NonReplicatedStrategy::equal(const KeyspaceMetadata& ks_meta) {
  return ks_meta.strategy_class() == strategy_class_;
}

void NonReplicatedStrategy::tokens_to_replicas(const TokenHostMap& primary, TokenReplicaMap* output) const {
  output->clear();
  for (TokenHostMap::const_iterator i = primary.begin(); i != primary.end(); ++i) {
    CopyOnWriteHostVec token_replicas(new HostVec(1, i->second));
    output->insert(std::make_pair(i->first, token_replicas));
  }
}

}
