/*
  Copyright (c) 2014-2016 DataStax

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

#ifndef __CASS_STATEMENT_HPP_INCLUDED__
#define __CASS_STATEMENT_HPP_INCLUDED__

#include "abstract_data.hpp"
#include "constants.hpp"
#include "external.hpp"
#include "macros.hpp"
#include "prepared.hpp"
#include "request.hpp"
#include "result_metadata.hpp"
#include "result_response.hpp"
#include "retry_policy.hpp"
#include "scoped_ptr.hpp"

#include <vector>
#include <string>

namespace cass {

class RequestCallback;

class Statement : public RoutableRequest, public AbstractData {
public:
  typedef SharedRefPtr<Statement> Ptr;

  Statement(const char* query, size_t query_length,
            size_t values_count)
      : RoutableRequest(CQL_OPCODE_QUERY)
      , AbstractData(values_count)
      , query_or_id_(sizeof(int32_t) + query_length)
      , flags_(0)
      , page_size_(-1) {
    // <query> [long string]
    query_or_id_.encode_long_string(0, query, query_length);
  }

  Statement(const Prepared* prepared)
      : RoutableRequest(CQL_OPCODE_EXECUTE,
                        prepared->result()->keyspace().to_string())
      , AbstractData(prepared->result()->column_count())
      , query_or_id_(sizeof(uint16_t) + prepared->id().size())
      , flags_(0)
      , page_size_(-1) {
    // <id> [short bytes] (or [string])
    const std::string& id = prepared->id();
    query_or_id_.encode_string(0, id.data(), id.size());
  }

  virtual ~Statement() { }

  bool skip_metadata() const {
    return flags_ & CASS_QUERY_FLAG_SKIP_METADATA;
  }

  void set_skip_metadata(bool skip_metadata) {
    if (skip_metadata) {
      flags_ |= CASS_QUERY_FLAG_SKIP_METADATA;
    } else {
      flags_ &= ~CASS_QUERY_FLAG_SKIP_METADATA;
    }
  }

  void set_has_names_for_values(bool has_names_for_values) {
    if (has_names_for_values) {
      flags_ |= CASS_QUERY_FLAG_NAMES_FOR_VALUES;
    } else {
      flags_ &= ~CASS_QUERY_FLAG_NAMES_FOR_VALUES;
    }
  }

  bool has_names_for_values() const {
    return flags_ & CASS_QUERY_FLAG_NAMES_FOR_VALUES;
  }

  int32_t page_size() const { return page_size_; }

  void set_page_size(int32_t page_size) { page_size_ = page_size; }

  const std::string& paging_state() const { return paging_state_; }

  void set_paging_state(const std::string& paging_state) {
    paging_state_ = paging_state;
  }

  uint8_t kind() const {
    return opcode() == CQL_OPCODE_QUERY ? CASS_BATCH_KIND_QUERY
                                        : CASS_BATCH_KIND_PREPARED;
  }

  void add_key_index(size_t index) { key_indices_.push_back(index); }

  virtual bool get_routing_key(std::string* routing_key, EncodingCache* cache) const {
    return calculate_routing_key(key_indices_, routing_key, cache);
  }

  int32_t encode_batch(int version, RequestCallback* callback, BufferVec* bufs) const;

protected:
  int32_t encode_v1(RequestCallback* callback, BufferVec* bufs) const;

  int32_t encode_begin(int version, uint16_t element_count,
                       RequestCallback* callback, BufferVec* bufs) const;
  int32_t encode_values(int version, RequestCallback* callback, BufferVec* bufs) const;
  int32_t encode_end(int version, RequestCallback* callback, BufferVec* bufs) const;

  bool calculate_routing_key(const std::vector<size_t>& key_indices,
                             std::string* routing_key, EncodingCache* cache) const;

private:
  Buffer query_or_id_;
  int32_t flags_;
  int32_t page_size_;
  std::string paging_state_;
  std::vector<size_t> key_indices_;

private:
  DISALLOW_COPY_AND_ASSIGN(Statement);
};

} // namespace cass

EXTERNAL_TYPE(cass::Statement, CassStatement)

#endif
