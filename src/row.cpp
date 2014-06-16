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

#include "cassandra.hpp"
#include "row.hpp"
#include "result_response.hpp"

extern "C" {

const CassValue* cass_row_get_column(const CassRow* row, cass_size_t index) {
  const cass::Row* internal_row = row->from();
  if (index >= internal_row->size()) {
    return nullptr;
  }
  return CassValue::to(&(*internal_row)[index]);
}

} // extern "C"

namespace cass {

char* decode_row(char* rows, const ResultResponse* result, Row& output) {
  char* buffer = rows;
  output.clear();

  for (int i = 0; i < result->column_count; ++i) {
    int32_t size = 0;
    buffer = decode_int(buffer, size);

    const ResultResponse::ColumnMetaData& metadata = result->column_metadata[i];
    CassValueType type = static_cast<CassValueType>(metadata.type);

    if (size >= 0) {
      if (type == CASS_VALUE_TYPE_MAP || type == CASS_VALUE_TYPE_LIST ||
          type == CASS_VALUE_TYPE_SET) {
        uint16_t count = 0;
        Value value(type, decode_short(buffer, count), size - sizeof(uint16_t));
        value.count = count;
        value.primary_type =
            static_cast<CassValueType>(metadata.collection_primary_type);
        value.secondary_type =
            static_cast<CassValueType>(metadata.collection_secondary_type);
        output.push_back(value);
      } else {
        output.push_back(Value(type, buffer, size));
      }
      buffer += size;
    } else { // null value
      output.push_back(Value());
    }
  }
  return buffer;
}
}
