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

#include "dht_meta.hpp"

#include <string>

namespace cass {


int64_t nstrtoll(const char* p, size_t n) {
    int c;
    const unsigned char* s = (unsigned char*)p;
    do {
        c = *s++;
    } while (isspace(c));

    int64_t sign = 1;
    if (c == '-') {
      sign = -1;
      ++s;
    }

    int64_t value = 0;
    while(n-- && isdigit(c = *s++)) {
        value *= 10;
        value += c - '0';
    }
    return sign*value;
}

const std::string M3PTokenMap::PARTIONER_CLASS("Murmur3Partitioner");

int64_t M3PTokenMap::token_from_string(const std::string& token_string) {
  return nstrtoll(token_string.data(), token_string.size());
}

int64_t M3PTokenMap::token_from_string_ref(const boost::string_ref& token_string_ref) {
  return nstrtoll(token_string_ref.data(), token_string_ref.size());
}

//const std::string XXTokenMap::PARTITIONER_CLASS("RandomPartitioner");
//const std::string XXTokenMap::PARTITIONER_CLASS("OrderedPartitioner");

ReplicaPlacementStrategy* ReplicaPlacementStrategy::from_keyspace_meta(const KeyspaceMetadata& ks_meta) {
  const std::string& strategy_class = ks_meta.strategy_class_;

  if (string_ends_with(strategy_class, NetworkTopologyStrategy::STRATEGY_CLASS)) {
    return new NetworkTopologyStrategy(ks_meta.strategy_options_);
  } else
  if (string_ends_with(strategy_class, SimpleStrategy::STRATEGY_CLASS)) {
    return new SimpleStrategy(ks_meta.strategy_options_);
  } else {
    return new NonReplicatedStrategy();
  }
}

const std::string NetworkTopologyStrategy::STRATEGY_CLASS("NetworkTopologyStrategy");
const std::string SimpleStrategy::STRATEGY_CLASS("SimpleStrategy");

void DHTMeta::set_partitioner(const std::string& partitioner_class) {
  if (token_map_.get() != NULL) {
    return;
  }

  if (string_ends_with(partitioner_class, M3PTokenMap::PARTIONER_CLASS)) {
    token_map_.reset(new M3PTokenMap());
  }

  if (token_map_.get() == NULL) {
    //TODO: global logging
  }
}

void DHTMeta::update_host(SharedRefPtr<Host>& host, const TokenStringList& tokens) {
  if (token_map_.get() != NULL) {
    token_map_->update(host, tokens);
  }
}

void DHTMeta::update_keyspace(const KeyspaceModel& ksm) {
  if (token_map_.get() != NULL) {
    //TODO: create RPS based on meta, guard against empty and not-changing
    token_map_->update(ksm.meta().name_);
  }
}

}
