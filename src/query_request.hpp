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

#ifndef __CASS_QUERY_REQUEST_HPP_INCLUDED__
#define __CASS_QUERY_REQUEST_HPP_INCLUDED__

#include "constants.hpp"
#include "hash_table.hpp"
#include "statement.hpp"

#include <string>
#include <vector>

namespace cass {

class QueryRequest : public Statement {
public:
  struct ValueName : HashTableEntry<ValueName> {
    ValueName() { }

    ValueName(const std::string& name)
      : name(name)
      , buf(sizeof(uint16_t) + name.size()) {
      buf.encode_string(0, name.data(), name.size());
    }

    std::string name;
    Buffer buf;
  };

  explicit QueryRequest(size_t value_count = 0)
    : Statement(CQL_OPCODE_QUERY, CASS_BATCH_KIND_QUERY,
                value_count)
    , value_names_(value_count) { }

  QueryRequest(const std::string& query,
               size_t value_count = 0)
    : Statement(CQL_OPCODE_QUERY, CASS_BATCH_KIND_QUERY,
                value_count)
    , query_(query)
    , value_names_(value_count) { }

  QueryRequest(const char* query, size_t query_length,
               size_t value_count = 0)
    : Statement(CQL_OPCODE_QUERY, CASS_BATCH_KIND_QUERY,
                value_count)
    , query_(query, query_length)
    , value_names_(value_count) { }

  virtual int32_t encode_batch(int version, BufferVec* bufs, RequestCallback* callback) const;

private:
  virtual size_t get_indices(StringRef name,
                             IndexVec* indices);

  virtual const DataType::ConstPtr& get_type(size_t index) const {
    return DataType::NIL;
  }

private:
  int32_t copy_buffers_with_names(int version, BufferVec* bufs, EncodingCache* cache) const;

  int encode(int version, RequestCallback* callback, BufferVec* bufs) const;
  int internal_encode_v1(RequestCallback* callback, BufferVec* bufs) const;
  int internal_encode(int version, RequestCallback* callback, BufferVec* bufs) const;

private:
  std::string query_;
  CaseInsensitiveHashTable<ValueName> value_names_;
};

} // namespace cass

#endif
