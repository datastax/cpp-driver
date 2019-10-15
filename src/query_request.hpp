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

#ifndef DATASTAX_INTERNAL_QUERY_REQUEST_HPP
#define DATASTAX_INTERNAL_QUERY_REQUEST_HPP

#include "constants.hpp"
#include "hash_table.hpp"
#include "statement.hpp"
#include "string.hpp"
#include "vector.hpp"

namespace datastax { namespace internal { namespace core {

class QueryRequest : public Statement {
public:
  QueryRequest(const String& query, size_t value_count = 0)
      : Statement(query.data(), query.size(), value_count) {}

  QueryRequest(const char* query, size_t query_length, size_t value_count)
      : Statement(query, query_length, value_count) {}

  virtual int encode(ProtocolVersion version, RequestCallback* callback, BufferVec* bufs) const;

private:
  int32_t encode_values_with_names(ProtocolVersion version, RequestCallback* callback,
                                   BufferVec* bufs) const;

  virtual size_t get_indices(StringRef name, IndexVec* indices);

  virtual const DataType::ConstPtr& get_type(size_t index) const { return DataType::NIL; }

private:
  struct ValueName : HashTableEntry<ValueName> {
    ValueName() {}

    ValueName(const String& name)
        : name(name)
        , buf(sizeof(uint16_t) + name.size()) {
      buf.encode_string(0, name.data(), static_cast<uint16_t>(name.size()));
    }

    String name;
    Buffer buf;
  };
  typedef CaseInsensitiveHashTable<ValueName> ValueNameHashTable;
  ScopedPtr<ValueNameHashTable> value_names_;
};

}}} // namespace datastax::internal::core

#endif
