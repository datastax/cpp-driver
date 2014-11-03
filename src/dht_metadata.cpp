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

#include "dht_metadata.hpp"
#include "murmur3.hpp"

#include <algorithm>
#include <string>

namespace cass {

static const COWHostVec EMPTY_REPLICA_COW_PTR(new HostVec());

static void parse_int64(const char* p, size_t n, int64_t* out) {
  int c;
  const unsigned char* s = (unsigned char*)p;
  for (; n != 0 && isspace(c = *s); ++s, --n) {}

  if (n == 0) {
    *out = 0;
    return;
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
  *out = sign*value;
}

static void parse_int128(const char* p, size_t n, uint64_t output[]) {
  // no sign handling because C* uses [0, 2^127]
  int c;
  const unsigned char* s = (unsigned char*)p;
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
  encode_int64((char*)output, hi);
  encode_int64((char*)(output+1), lo);
}


void TokenMap::update_host(SharedRefPtr<Host>& host, const TokenStringList& token_strings) {
  // There's a chance to avoid purging if tokens are the same as existing; deemed
  // not worth the complexity because:
  // 1.) Updates should only happen on "new" host, or "moved"
  // 2.) Moving should only occur on non-vnode clusters, in which case the
  //     token map is relatively small and easy to purge/repopulate
  purge_address(host->address());

  for (TokenStringList::const_iterator i = token_strings.begin();
       i != token_strings.end(); ++i) {
    token_map_[token_from_string_ref(*i)] = host;
  }
  map_replicas();
}

void TokenMap::remove_host(SharedRefPtr<Host>& host) {
  if (purge_address(host->address())) {
    map_replicas();
  }
}

void TokenMap::update_keyspace(const std::string& ks_name, const KeyspaceMetadata& ks_meta) {
  ScopedPtr<ReplicaPlacementStrategy> rps_now(ReplicaPlacementStrategy::from_keyspace_meta(ks_meta));
  KeyspaceStrategyMap::iterator i = keyspace_strategy_map_.find(ks_name);
  if (i == keyspace_strategy_map_.end() ||
      !i->second->equals(*rps_now)) {
    map_keyspace_replicas(ks_name, *rps_now);
    if (i == keyspace_strategy_map_.end()) {
      keyspace_strategy_map_[ks_name].reset(rps_now.release());
    } else {
      i->second.reset(rps_now.release());
    }
  }
}

void TokenMap::drop_keyspace(const std::string& ks_name) {
  keyspace_replica_map_.erase(ks_name);
  keyspace_strategy_map_.erase(ks_name);
}

const COWHostVec& TokenMap::get_replicas(const std::string& ks_name, const BufferRefs& key_parts) const {
  KeyspaceReplicaMap::const_iterator i = keyspace_replica_map_.find(ks_name);
  if (i != keyspace_replica_map_.end()) {
    const Token t = hash(key_parts);
    TokenReplicaMap::const_iterator j = i->second.upper_bound(t);
    if (j != i->second.end()) {
      return j->second;
    } else {
      if (!i->second.empty()) {
        return i->second.begin()->second;
      }
    }
  }
  return EMPTY_REPLICA_COW_PTR;
}

void TokenMap::map_replicas(bool force) {
  if (keyspace_replica_map_.empty() && !force) {// do nothing ahead of first build
    return;
  }
  for (KeyspaceStrategyMap::const_iterator i = keyspace_strategy_map_.begin();
       i != keyspace_strategy_map_.end(); ++i) {
    map_keyspace_replicas(i->first, *i->second, force);
  }
}

void TokenMap::map_keyspace_replicas(const std::string& ks_name, const ReplicaPlacementStrategy& rps, bool force) {
  if (keyspace_replica_map_.empty() && !force) {// do nothing ahead of first build
    return;
  }
  rps.tokens_to_replicas(token_map_, &keyspace_replica_map_[ks_name]);
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


const std::string M3PTokenMap::PARTIONER_CLASS("Murmur3Partitioner");

bool M3PTokenMap::compare(const Token& l, const Token& r) {
  assert(l.data.size() == r.data.size() && r.data.size() == sizeof(int64_t));
  return *reinterpret_cast<const int64_t*>(&l.data[0]) < *reinterpret_cast<const int64_t*>(&r.data[0]);
}

Token M3PTokenMap::token_from_string_ref(const boost::string_ref& token_string_ref) const {
  Token out(M3PTokenMap::compare);
  out.data.assign(sizeof(int64_t), 0);
  parse_int64(token_string_ref.data(), token_string_ref.size(), (int64_t*)&out.data[0]);
  return out;
}

Token M3PTokenMap::hash(const BufferRefs& key_parts) const {
  Murmur3 hash;
  for (BufferRefs::const_iterator i = key_parts.begin(); i != key_parts.end(); ++i) {
    hash.update(i->data(), i->size());
  }
  Token out(M3PTokenMap::compare);
  out.data.assign(sizeof(int64_t), 0);
  hash.final((int64_t*)&out.data[0], NULL);
  return out;
}


const std::string RPTokenMap::PARTIONER_CLASS("RandomPartitioner");

bool RPTokenMap::compare(const Token& l, const Token& r) {
  assert(l.data.size() == r.data.size() && r.data.size() == 16);
  return (l < r);
}

Token RPTokenMap::token_from_string_ref(const boost::string_ref& token_string_ref) const {
  Token out(RPTokenMap::compare);
  out.data.assign(sizeof(uint64_t) * 2, 0);
  parse_int128(token_string_ref.data(), token_string_ref.size(), (uint64_t*)&out.data[0]);
  return out;
}

Token RPTokenMap::hash(const BufferRefs& key_parts) const {
  // TODO: needs md5 from CPP-102-final. TBD after merge/rebase
//  Md5 hash;
//  for (BufferRefs::const_iterator i = key_parts.begin(); i != key_parts.end(); ++i) {
//    hash.update(i->data(), i->size());
//  }
//  Token out(RPTokenMap::compare);
//  out.data.assign(sizeof(int64_t), 0);
//  hash.final((int64_t*)&out.data[0], NULL);
//  return out;
  return Token(RPTokenMap::compare);
}


const std::string BOPTokenMap::PARTIONER_CLASS("ByteOrderedPartitioner");

bool BOPTokenMap::compare(const Token& l, const Token& r) {
  return l.data < r.data;
}

Token BOPTokenMap::token_from_string_ref(const boost::string_ref& token_string_ref) const {
  Token out(BOPTokenMap::compare);
  out.data.resize(token_string_ref.size());
  memcpy(&out.data[0], token_string_ref.data(), token_string_ref.size());
  return out;
}

Token BOPTokenMap::hash(const BufferRefs& key_parts) const {
  size_t total_size = 0;
  for (BufferRefs::const_iterator i = key_parts.begin();
       i != key_parts.end(); ++i) {
    total_size += i->size();
  }
  Token out(BOPTokenMap::compare);
  out.data.resize(total_size);
  uint8_t* base = &out.data[0];
  size_t offset = 0;
  for (BufferRefs::const_iterator i = key_parts.begin();
       i != key_parts.end(); ++i) {
    memcpy(base + offset, i->data(), i->size());
    offset += i->size();
  }
  return out;
}


void DHTMetadata::build() {
  if (token_map_.get()) {
    token_map_->build();
  }
}

void DHTMetadata::set_partitioner(const std::string& partitioner_class) {
  if (token_map_.get()) {
    return;
  }

  if (string_ends_with(partitioner_class, M3PTokenMap::PARTIONER_CLASS)) {
    token_map_.reset(new M3PTokenMap());
  }

  if (!token_map_) {
    //TODO: global logging
  }
}

void DHTMetadata::update_host(SharedRefPtr<Host>& host, const TokenStringList& tokens) {
  if (token_map_) {
    token_map_->update_host(host, tokens);
  }
}

void DHTMetadata::remove_host(SharedRefPtr<Host>& host) {
  if (token_map_) {
    token_map_->remove_host(host);
  }
}

void DHTMetadata::update_keyspace(const std::string& ks_name, const KeyspaceMetadata& ks_meta) {
  if (token_map_) {
    token_map_->update_keyspace(ks_name, ks_meta);
  }
}

void DHTMetadata::drop_keyspace(const std::string& ks_name) {
  if (token_map_) {
    token_map_->drop_keyspace(ks_name);
  }
}

const COWHostVec& DHTMetadata::get_replicas(const std::string& ks_name, const BufferRefs& key_parts) const {
  if (token_map_) {
    return token_map_->get_replicas(ks_name, key_parts);
  }
  return EMPTY_REPLICA_COW_PTR;
}

}
