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

#include "dse.h"

#include "dse_date_range.hpp"
#include "dse_line_string.hpp"
#include "dse_point.hpp"
#include "dse_polygon.hpp"

using namespace datastax::internal::enterprise;

extern "C" {

CassError cass_user_type_set_dse_point(CassUserType* user_type, size_t index, cass_double_t x,
                                       cass_double_t y) {
  Bytes bytes = encode_point(x, y);
  return cass_user_type_set_custom(user_type, index, DSE_POINT_TYPE, bytes.data(), bytes.size());
}

CassError cass_user_type_set_dse_point_by_name(CassUserType* user_type, const char* name,
                                               cass_double_t x, cass_double_t y) {
  return cass_user_type_set_dse_point_by_name_n(user_type, name, SAFE_STRLEN(name), x, y);
}

CassError cass_user_type_set_dse_point_by_name_n(CassUserType* user_type, const char* name,
                                                 size_t name_length, cass_double_t x,
                                                 cass_double_t y) {
  Bytes bytes = encode_point(x, y);
  return cass_user_type_set_custom_by_name_n(user_type, name, name_length, DSE_POINT_TYPE,
                                             sizeof(DSE_POINT_TYPE) - 1, bytes.data(),
                                             bytes.size());
}

CassError cass_user_type_set_dse_line_string(CassUserType* user_type, size_t index,
                                             const DseLineString* line_string) {
  return cass_user_type_set_custom(user_type, index, DSE_LINE_STRING_TYPE,
                                   line_string->bytes().data(), line_string->bytes().size());
}

CassError cass_user_type_set_dse_line_string_by_name(CassUserType* user_type, const char* name,
                                                     const DseLineString* line_string) {
  return cass_user_type_set_dse_line_string_by_name_n(user_type, name, SAFE_STRLEN(name),
                                                      line_string);
}

CassError cass_user_type_set_dse_line_string_by_name_n(CassUserType* user_type, const char* name,
                                                       size_t name_length,
                                                       const DseLineString* line_string) {
  return cass_user_type_set_custom_by_name_n(
      user_type, name, name_length, DSE_LINE_STRING_TYPE, sizeof(DSE_LINE_STRING_TYPE) - 1,
      line_string->bytes().data(), line_string->bytes().size());
}

CassError cass_user_type_set_dse_polygon(CassUserType* user_type, size_t index,
                                         const DsePolygon* polygon) {
  return cass_user_type_set_custom(user_type, index, DSE_POLYGON_TYPE, polygon->bytes().data(),
                                   polygon->bytes().size());
}

CassError cass_user_type_set_dse_polygon_by_name(CassUserType* user_type, const char* name,
                                                 const DsePolygon* polygon) {
  return cass_user_type_set_dse_polygon_by_name_n(user_type, name, SAFE_STRLEN(name), polygon);
}

CassError cass_user_type_set_dse_polygon_by_name_n(CassUserType* user_type, const char* name,
                                                   size_t name_length, const DsePolygon* polygon) {
  return cass_user_type_set_custom_by_name_n(user_type, name, name_length, DSE_POLYGON_TYPE,
                                             sizeof(DSE_POLYGON_TYPE) - 1, polygon->bytes().data(),
                                             polygon->bytes().size());
}

CassError cass_user_type_set_dse_date_range(CassUserType* user_type, size_t index,
                                            const DseDateRange* range) {
  Bytes bytes = encode_date_range(range);
  return cass_user_type_set_custom(user_type, index, DSE_DATE_RANGE_TYPE, bytes.data(),
                                   bytes.size());
}

CassError cass_user_type_set_dse_date_range_by_name(CassUserType* user_type, const char* name,
                                                    const DseDateRange* range) {
  return cass_user_type_set_dse_date_range_by_name_n(user_type, name, SAFE_STRLEN(name), range);
}

CassError cass_user_type_set_dse_date_range_by_name_n(CassUserType* user_type, const char* name,
                                                      size_t name_length,
                                                      const DseDateRange* range) {
  Bytes bytes = encode_date_range(range);
  return cass_user_type_set_custom_by_name_n(user_type, name, name_length, DSE_DATE_RANGE_TYPE,
                                             sizeof(DSE_DATE_RANGE_TYPE) - 1, bytes.data(),
                                             bytes.size());
}

} // extern "C"
