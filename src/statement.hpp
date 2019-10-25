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

#ifndef DATASTAX_INTERNAL_STATEMENT_HPP
#define DATASTAX_INTERNAL_STATEMENT_HPP

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
#include "string.hpp"
#include "vector.hpp"

namespace datastax { namespace internal { namespace core {

class RequestCallback;

class Statement
    : public RoutableRequest
    , public AbstractData {
public:
  typedef SharedRefPtr<Statement> Ptr;

  Statement(const char* query, size_t query_length, size_t values_count);

  Statement(const Prepared* prepared);

  virtual ~Statement() {}

  // Used to get the original query string from a simple statement. To get the
  // query from a execute request (bound statement) cast it and get it from the
  // prepared object.
  String query() const;

  void set_has_names_for_values(bool has_names_for_values) {
    if (has_names_for_values) {
      flags_ |= CASS_QUERY_FLAG_NAMES_FOR_VALUES;
    } else {
      flags_ &= ~CASS_QUERY_FLAG_NAMES_FOR_VALUES;
    }
  }

  bool has_names_for_values() const { return (flags_ & CASS_QUERY_FLAG_NAMES_FOR_VALUES) != 0; }

  int32_t page_size() const { return page_size_; }

  void set_page_size(int32_t page_size) { page_size_ = page_size; }

  const String& paging_state() const { return paging_state_; }

  void set_paging_state(const String& paging_state) { paging_state_ = paging_state; }

  uint8_t kind() const {
    return opcode() == CQL_OPCODE_QUERY ? CASS_BATCH_KIND_QUERY : CASS_BATCH_KIND_PREPARED;
  }

  void add_key_index(size_t index) { key_indices_.push_back(index); }

  virtual bool get_routing_key(String* routing_key) const {
    return calculate_routing_key(key_indices_, routing_key);
  }

  int32_t encode_batch(ProtocolVersion version, RequestCallback* callback, BufferVec* bufs) const;

protected:
  bool with_keyspace(ProtocolVersion version) const;

  int32_t encode_query_or_id(BufferVec* bufs) const;
  int32_t encode_begin(ProtocolVersion version, uint16_t element_count, RequestCallback* callback,
                       BufferVec* bufs) const;
  int32_t encode_values(ProtocolVersion version, RequestCallback* callback, BufferVec* bufs) const;
  int32_t encode_end(ProtocolVersion version, RequestCallback* callback, BufferVec* bufs) const;

  bool calculate_routing_key(const Vector<size_t>& key_indices, String* routing_key) const;

private:
  Buffer query_or_id_;
  int32_t flags_;
  int32_t page_size_;
  String paging_state_;
  Vector<size_t> key_indices_;

private:
  DISALLOW_COPY_AND_ASSIGN(Statement);
};

}}} // namespace datastax::internal::core

EXTERNAL_TYPE(datastax::internal::core::Statement, CassStatement)

#endif
