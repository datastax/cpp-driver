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

#include "common.hpp"
#include "token_map.hpp"
#include "logger.hpp"
#include "md5.hpp"
#include "murmur3.hpp"
#include "scoped_ptr.hpp"

#include <uv.h>

#include <algorithm>
#include <limits>
#include <string>

namespace cass {

static const CopyOnWriteHostVec NO_REPLICAS(new HostVec());

static int64_t parse_int64(const char* p, size_t n) {
  int c;
  const char* s = p;
  for (; n != 0 && isspace(c = *s); ++s, --n) {}

  if (n == 0) {
    return 0;
  }

  int64_t sign = 1;
  if (c == '-') {
    sign = -1;
    ++s; --n;
  }

  int64_t value = 0;
  for (; n != 0  && isdigit(c = *s); ++s, --n) {
    value *= 10;
    value += c - '0';
  }

  return sign * value;
}

static void parse_int128(const char* p, size_t n, uint8_t* output) {
  // no sign handling because C* uses [0, 2^127]
  int c;
  const char* s = p;

  for (; n != 0 && isspace(c = *s); ++s, --n) {}

  if (n == 0) {
    memset(output, 0, sizeof(uint64_t) * 2);
    return;
  }

  uint64_t hi = 0;
  uint64_t lo = 0;
  uint64_t hi_tmp;
  uint64_t lo_tmp;
  uint64_t lo_tmp2;
  for (; n != 0  && isdigit(c = *s); ++s, --n) {
    hi_tmp = hi;
    lo_tmp = lo;

    //value *= 10;
    lo = lo_tmp << 1;
    hi = (lo_tmp >> 63) + (hi_tmp << 1);
    lo_tmp2 = lo;
    lo += lo_tmp << 3;
    hi += (lo_tmp >> 61) + (hi_tmp << 3) + (lo < lo_tmp2 ? 1 : 0);

    //value += c - '0';
    lo_tmp = lo;
    lo += c - '0';
    hi += (lo < lo_tmp) ? 1 : 0;
  }

  encode_uint64(output, hi);
  encode_uint64(output + sizeof(uint64_t), lo);
}

void TokenMap::clear() {
  mapped_addresses_.clear();
  token_map_.clear();
  keyspace_replica_map_.clear();
  keyspace_strategy_map_.clear();
  partitioner_.reset();
}

void TokenMap::build() {
  if (!partitioner_) {
    LOG_WARN("No partitioner set, not building map");
    return;
  }

  map_replicas(true);
}

void TokenMap::set_partitioner(const std::string& partitioner_class) {
  // Only set the partition once
  if (partitioner_) return;

  if (ends_with(partitioner_class, Murmur3Partitioner::PARTITIONER_CLASS)) {
    partitioner_.reset(new Murmur3Partitioner());
  } else if (ends_with(partitioner_class, RandomPartitioner::PARTITIONER_CLASS)) {
    partitioner_.reset(new RandomPartitioner());
  } else if (ends_with(partitioner_class, ByteOrderedPartitioner::PARTITIONER_CLASS)) {
    partitioner_.reset(new ByteOrderedPartitioner());
  } else {
    LOG_WARN("Unsupported partitioner class '%s'", partitioner_class.c_str());
  }
}

void TokenMap::update_host(SharedRefPtr<Host>& host, const TokenStringList& token_strings) {
  if (!partitioner_) return;

  // There's a chance to avoid purging if tokens are the same as existing; deemed
  // not worth the complexity because:
  // 1.) Updates should only happen on "new" host, or "moved"
  // 2.) Moving should only occur on non-vnode clusters, in which case the
  //     token map is relatively small and easy to purge/repopulate
  purge_address(host->address());

  for (TokenStringList::const_iterator i = token_strings.begin();
       i != token_strings.end(); ++i) {
    token_map_[partitioner_->token_from_string_ref(*i)] = host;
  }
  mapped_addresses_.insert(host->address());
  map_replicas();
}

void TokenMap::remove_host(SharedRefPtr<Host>& host) {
  if (!partitioner_) return;

  if (purge_address(host->address())) {
    map_replicas();
  }
}

void TokenMap::update_keyspace(const std::string& ks_name, const KeyspaceMetadata& ks_meta) {
  if (!partitioner_) return;

  KeyspaceStrategyMap::iterator i = keyspace_strategy_map_.find(ks_name);
  if (i == keyspace_strategy_map_.end() || !i->second->equal(ks_meta)) {
    SharedRefPtr<ReplicationStrategy> strategy(ReplicationStrategy::from_keyspace_meta(ks_meta));
    map_keyspace_replicas(ks_name, strategy);
    if (i == keyspace_strategy_map_.end()) {
      keyspace_strategy_map_[ks_name] = strategy;
    } else {
      i->second = strategy;
    }
  }
}

void TokenMap::drop_keyspace(const std::string& ks_name) {
  if (!partitioner_) return;

  keyspace_replica_map_.erase(ks_name);
  keyspace_strategy_map_.erase(ks_name);
}

const CopyOnWriteHostVec& TokenMap::get_replicas(const std::string& ks_name,
                                                 const std::string& routing_key) const {
  if (!partitioner_) return NO_REPLICAS;

  KeyspaceReplicaMap::const_iterator tokens_it = keyspace_replica_map_.find(ks_name);
  if (tokens_it != keyspace_replica_map_.end()) {
    const TokenReplicaMap& tokens_to_replicas = tokens_it->second;

    const Token t = partitioner_->hash(reinterpret_cast<const uint8_t*>(routing_key.data()), routing_key.size());
    TokenReplicaMap::const_iterator replicas_it = tokens_to_replicas.upper_bound(t);

    if (replicas_it != tokens_to_replicas.end()) {
      return replicas_it->second;
    } else {
      if (!tokens_to_replicas.empty()) {
        return tokens_to_replicas.begin()->second;
      }
    }
  }
  return NO_REPLICAS;
}

void TokenMap::set_replication_strategy(const std::string& ks_name,
                                        const SharedRefPtr<ReplicationStrategy>& strategy) {
  keyspace_strategy_map_[ks_name] = strategy;
  map_keyspace_replicas(ks_name, strategy);
}

void TokenMap::map_replicas(bool force) {
  if (keyspace_replica_map_.empty() && !force) {// do nothing ahead of first build
    return;
  }
  for (KeyspaceStrategyMap::const_iterator i = keyspace_strategy_map_.begin();
       i != keyspace_strategy_map_.end(); ++i) {
    map_keyspace_replicas(i->first, i->second, force);
  }
}

void TokenMap::map_keyspace_replicas(const std::string& ks_name,
                                     const SharedRefPtr<ReplicationStrategy>& strategy,
                                     bool force) {
  if (keyspace_replica_map_.empty() && !force) {// do nothing ahead of first build
    return;
  }
  strategy->tokens_to_replicas(token_map_, &keyspace_replica_map_[ks_name]);
}

bool TokenMap::purge_address(const Address& addr) {
  AddressSet::iterator addr_itr = mapped_addresses_.find(addr);
  if (addr_itr == mapped_addresses_.end()) {
    return false;
  }

  TokenHostMap::iterator i = token_map_.begin();
  while (i != token_map_.end()) {
    if (addr.compare(i->second->address()) == 0) {
      TokenHostMap::iterator to_erase = i++;
      token_map_.erase(to_erase);
    } else {
      ++i;
    }
  }

  mapped_addresses_.erase(addr_itr);
  return true;
}


const std::string Murmur3Partitioner::PARTITIONER_CLASS("Murmur3Partitioner");

Token Murmur3Partitioner::token_from_string_ref(const StringRef& token_string_ref) const {
  Token token(sizeof(int64_t), 0);
  int64_t token_value = parse_int64(token_string_ref.data(), token_string_ref.size());
  encode_uint64(&token[0], static_cast<uint64_t>(token_value) + std::numeric_limits<uint64_t>::max() / 2);
  return token;
}

Token Murmur3Partitioner::hash(const uint8_t* data, size_t size) const {
  Token token(sizeof(int64_t), 0);
  int64_t token_value = MurmurHash3_x64_128(data, size, 0);
  if (token_value == std::numeric_limits<int64_t>::min()) {
    token_value = std::numeric_limits<int64_t>::max();
  }
  encode_uint64(&token[0], static_cast<uint64_t>(token_value) + std::numeric_limits<uint64_t>::max() / 2);
  return token;
}

const std::string RandomPartitioner::PARTITIONER_CLASS("RandomPartitioner");

Token RandomPartitioner::token_from_string_ref(const StringRef& token_string_ref) const {
  Token token(sizeof(uint64_t) * 2, 0);
  parse_int128(token_string_ref.data(), token_string_ref.size(), &token[0]);
  return token;
}

Token RandomPartitioner::hash(const uint8_t* data, size_t size) const {
  Md5 hash;
  hash.update(data, size);

  Token token(sizeof(uint64_t) * 2, 0);
  hash.final(&token[0]);
  return token;
}

const std::string ByteOrderedPartitioner::PARTITIONER_CLASS("ByteOrderedPartitioner");

Token ByteOrderedPartitioner::token_from_string_ref(const StringRef& token_string_ref) const {
  const uint8_t* data = reinterpret_cast<const uint8_t*>(token_string_ref.data());
  size_t size = token_string_ref.size();
  return Token(data, data + size);
}

Token ByteOrderedPartitioner::hash(const uint8_t* data, size_t size) const {
  const uint8_t* first = static_cast<const uint8_t*>(data);
  Token token(first, first + size);
  return token;
}

}
