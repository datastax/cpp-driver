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

#ifndef __CASS_QUERY_REQUEST_HPP_INCLUDED__
#define __CASS_QUERY_REQUEST_HPP_INCLUDED__

#include "constants.hpp"
#include "hash_index.hpp"
#include "statement.hpp"

#include <string>
#include <vector>

namespace cass {

class QueryRequest : public Statement {
public:
  struct ValueName : HashIndex::Entry {
    ValueName(const std::string& name)
      : buf(sizeof(uint16_t) + name.size()) {
      buf.encode_string(0, name.data(), name.size());
    }

    StringRef to_string_ref() const {
      return StringRef(buf.data() + sizeof(uint16_t),
                       buf.size() - sizeof(uint16_t));
    }

    Buffer buf;
  };

  typedef std::vector<ValueName> ValueNameVec;

  explicit QueryRequest(size_t value_count = 0)
    : Statement(CQL_OPCODE_QUERY, CASS_BATCH_KIND_QUERY,
                value_count) { }

  QueryRequest(const std::string& query,
               size_t value_count = 0)
    : Statement(CQL_OPCODE_QUERY, CASS_BATCH_KIND_QUERY,
                value_count)
    , query_(query) { }

  QueryRequest(const char* query, size_t query_length,
               size_t value_count = 0)
    : Statement(CQL_OPCODE_QUERY, CASS_BATCH_KIND_QUERY,
                value_count)
      , query_(query, query_length) { }

  virtual int32_t encode_batch(int version, BufferVec* bufs, EncodingCache* cache) const;

private:
  virtual size_t get_indices(StringRef name,
                             HashIndex::IndexVec* indices);

  virtual const SharedRefPtr<DataType>& get_type(size_t index) const {
    return DataType::NIL;
  }

private:
  int32_t copy_buffers_with_names(int version, BufferVec* bufs, EncodingCache* cache) const;

  int encode(int version, BufferVec* bufs, EncodingCache* cache) const;
  int internal_encode_v1(BufferVec* bufs) const;
  int internal_encode(int version, BufferVec* bufs, EncodingCache* cache) const;

private:
  std::string query_;
  ValueNameVec value_names_;
  ScopedPtr<HashIndex> value_names_index_;
};

} // namespace cass

#endif
