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

#ifndef DATASTAX_INTERNAL_PREPARED_HPP
#define DATASTAX_INTERNAL_PREPARED_HPP

#include "buffer.hpp"
#include "dense_hash_map.hpp"
#include "external.hpp"
#include "metadata.hpp"
#include "prepare_request.hpp"
#include "ref_counted.hpp"
#include "request.hpp"
#include "result_response.hpp"
#include "scoped_lock.hpp"
#include "scoped_ptr.hpp"
#include "string.hpp"

#include <uv.h>

namespace datastax { namespace internal { namespace core {

class Prepared : public RefCounted<Prepared> {
public:
  typedef SharedRefPtr<const Prepared> ConstPtr;

  Prepared(const ResultResponse::Ptr& result, const PrepareRequest::ConstPtr& prepare_request,
           const Metadata::SchemaSnapshot& schema_metadata);

  const ResultResponse::ConstPtr& result() const { return result_; }
  const String& id() const { return id_; }
  const String& query() const { return query_; }
  const String& keyspace() const { return keyspace_; }
  const RequestSettings& request_settings() const { return request_settings_; }
  const ResultResponse::PKIndexVec& key_indices() const { return key_indices_; }

private:
  ResultResponse::ConstPtr result_;
  String id_;
  String query_;
  String keyspace_;
  RequestSettings request_settings_;
  ResultResponse::PKIndexVec key_indices_;
};

class PreparedMetadata {
public:
  class Entry : public RefCounted<Entry> {
  public:
    typedef SharedRefPtr<const Entry> Ptr;
    typedef Vector<Ptr> Vec;

    Entry(const String& query, const String& keyspace, const String& result_metadata_id,
          const ResultResponse::ConstPtr& result)
        : query_(query)
        , keyspace_(keyspace)
        , result_metadata_id_(sizeof(uint16_t) + result_metadata_id.size())
        , result_(result) {
      result_metadata_id_.encode_string(0, result_metadata_id.data(),
                                        static_cast<uint16_t>(result_metadata_id.size()));
    }

    const String& query() const { return query_; }
    const String& keyspace() const { return keyspace_; }
    const Buffer& result_metadata_id() const { return result_metadata_id_; }
    const ResultResponse::ConstPtr& result() const { return result_; }

  private:
    String query_;
    String keyspace_;
    Buffer result_metadata_id_;
    ResultResponse::ConstPtr result_;
  };

  PreparedMetadata() {
    metadata_.set_empty_key(String());
    uv_rwlock_init(&rwlock_);
  }

  ~PreparedMetadata() { uv_rwlock_destroy(&rwlock_); }

  Entry::Ptr get(const String& prepared_id) const {
    ScopedReadLock rl(&rwlock_);
    Map::const_iterator i = metadata_.find(prepared_id);
    if (i != metadata_.end()) {
      return i->second;
    }
    return Entry::Ptr();
  }

  void set(const String& prepared_id, const PreparedMetadata::Entry::Ptr& entry) {
    ScopedWriteLock wl(&rwlock_);
    metadata_[prepared_id] = entry;
  }

  Entry::Vec copy() const {
    ScopedReadLock rl(&rwlock_);
    Entry::Vec temp;
    temp.reserve(metadata_.size());
    for (Map::const_iterator it = metadata_.begin(), end = metadata_.end(); it != end; ++it) {
      temp.push_back(it->second);
    }
    return temp;
  }

private:
  typedef DenseHashMap<String, Entry::Ptr> Map;

  mutable uv_rwlock_t rwlock_;
  Map metadata_;
};

}}} // namespace datastax::internal::core

EXTERNAL_TYPE(datastax::internal::core::Prepared, CassPrepared)

#endif
