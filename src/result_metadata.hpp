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

#ifndef __CASS_METADATA_HPP_INCLUDED__
#define __CASS_METADATA_HPP_INCLUDED__

#include "cassandra.h"
#include "list.hpp"
#include "fixed_vector.hpp"
#include "ref_counted.hpp"

#include "third_party/boost/boost/cstdint.hpp"
#include "third_party/boost/boost/utility/string_ref.hpp"

#include <algorithm>
#include <map>
#include <string>
#include <vector>

namespace cass {

struct ColumnDefinition {
  ColumnDefinition()
      : index(0)
      , next(NULL)
      , type(CASS_VALUE_TYPE_UNKNOWN)
      , keyspace(NULL)
      , keyspace_size(0)
      , table(NULL)
      , table_size(0)
      , name(NULL)
      , name_size(0)
      , class_name(NULL)
      , class_name_size(0)
      , collection_primary_type(CASS_VALUE_TYPE_UNKNOWN)
      , collection_primary_class(NULL)
      , collection_primary_class_size(0)
      , collection_secondary_type(CASS_VALUE_TYPE_UNKNOWN)
      , collection_secondary_class(NULL)
      , collection_secondary_class_size(0) {}

  size_t index;
  ColumnDefinition* next;

  uint16_t type;
  char* keyspace;
  size_t keyspace_size;

  char* table;
  size_t table_size;

  char* name;
  size_t name_size;

  char* class_name;
  size_t class_name_size;

  uint16_t collection_primary_type;
  char* collection_primary_class;

  size_t collection_primary_class_size;
  uint16_t collection_secondary_type;

  char* collection_secondary_class;
  size_t collection_secondary_class_size;
};

class ResultMetadata : public RefCounted<ResultMetadata> {
public:
  typedef FixedVector<size_t, 16> IndexVec;

  ResultMetadata(size_t column_count);

  const ColumnDefinition& get(size_t index) const { return defs_[index]; }

  size_t get(boost::string_ref name, IndexVec* result) const;

  size_t column_count() const { return defs_.size(); }

  void insert(ColumnDefinition& meta);

private:
  static const size_t FIXED_COLUMN_META_SIZE = 16;

  FixedVector<ColumnDefinition, FIXED_COLUMN_META_SIZE> defs_;
  FixedVector<ColumnDefinition*, 2 * FIXED_COLUMN_META_SIZE> index_;
  size_t index_mask_;

private:
  DISALLOW_COPY_AND_ASSIGN(ResultMetadata);
};

} // namespace cass

#endif
