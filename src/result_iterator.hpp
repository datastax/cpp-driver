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

#ifndef __CASS_RESULT_ITERATOR_HPP_INCLUDED__
#define __CASS_RESULT_ITERATOR_HPP_INCLUDED__

#include "iterator.hpp"
#include "result.hpp"
#include "row.hpp"
#include "buffer_piece.hpp"

namespace cass {

struct ResultIterator : Iterator {
  Result*             result;
  int32_t             row_position;
  char*               position;
  Row                 row;

  ResultIterator(Result* result)
    : Iterator(CASS_ITERATOR_TYPE_RESULT)
    , result(result)
    , row_position(0)
    , position(result->rows)
    , row(result->column_count) { }

  char*
  parse_row(
      char* row,
      std::vector<Value>& output) {
    char* buffer = row;
    output.clear();

    for (int i = 0; i < result->column_count; ++i) {
      int32_t size  = 0;
      buffer        = decode_int(buffer, size);

      const Result::ColumnMetaData& metadata = result->column_metadata[i];
      cass_value_type_t type = static_cast<cass_value_type_t>(metadata.type);

      if(type == CASS_VALUE_TYPE_MAP ||
         type == CASS_VALUE_TYPE_LIST ||
         type == CASS_VALUE_TYPE_SET) {
        uint16_t count = 0;
        Value value(type, decode_short(buffer, count), size - sizeof(uint16_t));
        value.count = count;
        value.primary_type = static_cast<cass_value_type_t>(metadata.collection_primary_type);
        value.secondary_type = static_cast<cass_value_type_t>(metadata.collection_secondary_type);
      } else {
        output.push_back(Value(type, buffer, size));
      }

      buffer += size;
    }
    return buffer;
  }

  virtual bool
  next() {
    if (row_position++ >= result->row_count) {
      return false;
    }
    position = parse_row(position, row);
    return true;
  }
};

} // namespace cass

#endif
