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

#ifndef DATASTAX_INTERNAL_RESULT_METADATA_HPP
#define DATASTAX_INTERNAL_RESULT_METADATA_HPP

#include "cassandra.h"
#include "data_type.hpp"
#include "hash_table.hpp"
#include "ref_counted.hpp"
#include "string_ref.hpp"

#include <uv.h>

#include <algorithm>

namespace datastax { namespace internal { namespace core {

struct ColumnDefinition : public HashTableEntry<ColumnDefinition> {
  StringRef name;
  StringRef keyspace;
  StringRef table;
  DataType::ConstPtr data_type;
};

class ResultMetadata : public RefCounted<ResultMetadata> {
public:
  typedef SharedRefPtr<ResultMetadata> Ptr;

  ResultMetadata(size_t column_count, const RefBuffer::Ptr& buffer);

  const ColumnDefinition& get_column_definition(size_t index) const { return defs_[index]; }

  size_t get_indices(StringRef name, IndexVec* result) const;

  size_t column_count() const { return defs_.size(); }

  void add(const ColumnDefinition& meta);

private:
  CaseInsensitiveHashTable<ColumnDefinition> defs_;
  RefBuffer::Ptr buffer_;

private:
  DISALLOW_COPY_AND_ASSIGN(ResultMetadata);
};

}}} // namespace datastax::internal::core

#endif
