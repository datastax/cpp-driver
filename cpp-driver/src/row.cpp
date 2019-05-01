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

#include "row.hpp"

#include "external.hpp"
#include "result_metadata.hpp"
#include "result_response.hpp"
#include "serialization.hpp"
#include "string_ref.hpp"

using namespace datastax;
using namespace datastax::internal::core;

extern "C" {

const CassValue* cass_row_get_column(const CassRow* row, size_t index) {
  if (index >= row->values.size()) {
    return NULL;
  }
  return CassValue::to(&row->values[index]);
}

const CassValue* cass_row_get_column_by_name(const CassRow* row, const char* name) {

  return cass_row_get_column_by_name_n(row, name, SAFE_STRLEN(name));
}

const CassValue* cass_row_get_column_by_name_n(const CassRow* row, const char* name,
                                               size_t name_length) {

  return CassValue::to(row->get_by_name(StringRef(name, name_length)));
}

} // extern "C"

namespace datastax { namespace internal { namespace core {

bool decode_row(Decoder& decoder, const ResultResponse* result, OutputValueVec& output) {
  output.clear();
  output.reserve(result->column_count());
  for (int i = 0; i < result->column_count(); ++i) {
    Value value;
    const ColumnDefinition& def = result->metadata()->get_column_definition(i);
    CHECK_RESULT(decoder.decode_value(def.data_type, value));
    output.push_back(value);
  }

  return true;
}

}}} // namespace datastax::internal::core

const Value* Row::get_by_name(const StringRef& name) const {
  IndexVec indices;
  if (result_->metadata()->get_indices(name, &indices) == 0) {
    return NULL;
  }
  return &values[indices[0]];
}

bool Row::get_string_by_name(const StringRef& name, String* out) const {
  const Value* value = get_by_name(name);
  if (value == NULL || value->is_null()) {
    return false;
  }
  *out = value->decoder().as_string();
  return true;
}
