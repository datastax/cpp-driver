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

#include "row.hpp"

#include "result_metadata.hpp"
#include "result_response.hpp"
#include "types.hpp"

extern "C" {

const CassValue* cass_row_get_column(const CassRow* row, cass_size_t index) {
  if (index >= row->values.size()) {
    return NULL;
  }
  return CassValue::to(&row->values[index]);
}

const CassValue* cass_row_get_column_by_name(const CassRow* row,
                                             const char* name) {

  return CassValue::to(row->get_by_name(name));
}

} // extern "C"

namespace cass {

char* decode_row(char* rows, const ResultResponse* result, ValueVec& output) {
  char* buffer = rows;
  output.clear();

  for (int i = 0; i < result->column_count(); ++i) {
    int32_t size = 0;
    buffer = decode_int32(buffer, size);

    const ColumnDefinition& def = result->metadata()->get(i);
    CassValueType type = static_cast<CassValueType>(def.type);

    if (size >= 0) {
      if (type == CASS_VALUE_TYPE_MAP || type == CASS_VALUE_TYPE_LIST ||
          type == CASS_VALUE_TYPE_SET) {
        uint16_t count = 0;
        char* data = decode_uint16(buffer, count);
        output.push_back(Value(&def, count, data, size - sizeof(uint16_t)));
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

const Value* Row::get_by_name(const boost::string_ref& name) const {
  cass::ResultMetadata::IndexVec indices;
  if (result_->find_column_indices(name, &indices) == 0) {
    return NULL;
  }
  return &values[indices[0]];
}

}
