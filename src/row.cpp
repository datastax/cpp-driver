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

#include "row.hpp"

#include "external.hpp"
#include "result_metadata.hpp"
#include "result_response.hpp"
#include "serialization.hpp"
#include "string_ref.hpp"

extern "C" {

const CassValue* cass_row_get_column(const CassRow* row, size_t index) {
  if (index >= row->values.size()) {
    return NULL;
  }
  return CassValue::to(&row->values[index]);
}

const CassValue* cass_row_get_column_by_name(const CassRow* row,
                                             const char* name) {

  return cass_row_get_column_by_name_n(row, name, strlen(name));
}

const CassValue* cass_row_get_column_by_name_n(const CassRow* row,
                                               const char* name,
                                               size_t name_length) {

  return CassValue::to(row->get_by_name(cass::StringRef(name, name_length)));
}

} // extern "C"

namespace cass {

char* decode_row(char* rows, const ResultResponse* result, OutputValueVec& output) {
  char* buffer = rows;
  output.clear();

  const int protocol_version = result->protocol_version();

  for (int i = 0; i < result->column_count(); ++i) {
    int32_t size = 0;
    buffer = decode_int32(buffer, size);

    const ColumnDefinition& def = result->metadata()->get_column_definition(i);

    if (size >= 0) {
      output.push_back(Value(protocol_version, def.data_type, buffer, size));
      buffer += size;
    } else { // null value
      output.push_back(Value(def.data_type));
    }
  }
  return buffer;
}

const Value* Row::get_by_name(const StringRef& name) const {
  IndexVec indices;
  if (result_->metadata()->get_indices(name, &indices) == 0) {
    return NULL;
  }
  return &values[indices[0]];
}

bool Row::get_string_by_name(const StringRef& name, std::string* out) const {
  const Value* value = get_by_name(name);
  if (value == NULL ||
      value->size() < 0) {
    return false;
  }
  out->assign(value->data(), value->size());
  return true;
}

}
