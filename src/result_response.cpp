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
#include "result_response.hpp"

extern "C" {

void cass_result_free(const CassResult* result) {
  delete result->from();
}

size_t cass_result_row_count(const CassResult* result) {
  if (result->kind == CASS_RESULT_KIND_ROWS) {
    return result->row_count;
  }
  return 0;
}

size_t cass_result_column_count(const CassResult* result) {
  if (result->kind == CASS_RESULT_KIND_ROWS) {
    return result->column_count;
  }
  return 0;
}

CassValueType cass_result_column_type(const CassResult* result, size_t index) {
  if (result->kind == CASS_RESULT_KIND_ROWS &&
      index < result->column_metadata.size()) {
    return static_cast<CassValueType>(result->column_metadata[index].type);
  }
  return CASS_VALUE_TYPE_UNKNOWN;
}

const CassRow* cass_result_first_row(const CassResult* result) {
  if (result->kind == CASS_RESULT_KIND_ROWS && result->row_count > 0) {
    return CassRow::to(&result->first_row);
  }
  return nullptr;
}
}
