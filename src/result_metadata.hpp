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

#ifndef __CASS_METADATA_HPP_INCLUDED__
#define __CASS_METADATA_HPP_INCLUDED__

#include "cassandra.h"
#include "data_type.hpp"
#include "fixed_vector.hpp"
#include "hash_index.hpp"
#include "list.hpp"
#include "ref_counted.hpp"
#include "string_ref.hpp"

#include <uv.h>

#include <algorithm>
#include <map>
#include <string>
#include <vector>

namespace cass {

struct ColumnDefinition : public HashIndex::Entry {
  StringRef keyspace;
  StringRef table;
  SharedRefPtr<DataType> data_type;
};

class ResultMetadata : public RefCounted<ResultMetadata> {
public:
  ResultMetadata(size_t column_count);

  const ColumnDefinition& get_column_definition(size_t index) const { return defs_[index]; }

  size_t get_indices(StringRef name, HashIndex::IndexVec* result) const;

  size_t column_count() const { return defs_.size(); }

  void insert(ColumnDefinition& meta);

private:
  static const size_t FIXED_COLUMN_META_SIZE = 16;

  FixedVector<ColumnDefinition, FIXED_COLUMN_META_SIZE> defs_;
  HashIndex index_;

private:
  DISALLOW_COPY_AND_ASSIGN(ResultMetadata);
};

} // namespace cass

#endif
