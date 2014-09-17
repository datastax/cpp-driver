/*
  Copyright (c) 2014 DataStax

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

#ifndef __CASS_VALUE_HPP_INCLUDED__
#define __CASS_VALUE_HPP_INCLUDED__

#include "cassandra.h"
#include "buffer_piece.hpp"
#include "result_metadata.hpp"

namespace cass {

class Value {
public:
  Value()
      : type_(CASS_VALUE_TYPE_UNKNOWN)
      , def_(NULL)
      , count_(0) {}

  Value(CassValueType type, char* data, size_t size)
      : type_(type)
      , def_(NULL)
      , count_(0)
      , buffer_(data, size) {}

  Value(const ColumnDefinition* definition, int32_t count, char* data, size_t size)
    : type_(static_cast<CassValueType>(definition->type))
    , def_(definition)
    , count_(count)
    , buffer_(data, size) {}

  CassValueType type() const { return type_; }

  CassValueType primary_type() const {
    if (def_ == NULL) {
      return CASS_VALUE_TYPE_UNKNOWN;
    }
    return static_cast<CassValueType>(def_->collection_primary_type);
  }

  CassValueType secondary_type() const {
    if (def_ == NULL) {
      return CASS_VALUE_TYPE_UNKNOWN;
    }
    return static_cast<CassValueType>(def_->collection_secondary_type);
  }

  bool is_null() const {
    return buffer().size() < 0;
  }

  bool is_collection() const {
    return type_ == CASS_VALUE_TYPE_LIST ||
           type_ == CASS_VALUE_TYPE_MAP  ||
           type_ == CASS_VALUE_TYPE_SET;
  }

  int32_t count() const {
    return count_;
  }

  const BufferPiece& buffer() const {
    return buffer_;
  }

private:
  CassValueType type_;
  const ColumnDefinition* def_;
  int32_t count_;
  BufferPiece buffer_;
};

typedef std::vector<Value> ValueVec;

} // namespace cass

#endif
