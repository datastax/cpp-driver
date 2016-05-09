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

#include "dse.h"

#include "external_types.hpp"
#include "serialization.hpp"

namespace dse {

inline Bytes encode_point(cass_double_t x, cass_double_t y) {
  Bytes bytes;

  bytes.reserve(WKB_HEADER_SIZE +       // Header
                sizeof(cass_double_t) + // X
                sizeof(cass_double_t)); // Y

  encode_header_append(WKB_GEOMETRY_TYPE_POINT, bytes);
  encode_append(x, bytes);
  encode_append(y, bytes);

  return bytes;
}

inline Bytes encode_circle(cass_double_t x, cass_double_t y,
                           cass_double_t radius) {
  Bytes bytes;

  bytes.reserve(WKB_HEADER_SIZE +       // Header
                sizeof(cass_double_t) + // X
                sizeof(cass_double_t) + // Y
                sizeof(cass_double_t)); // Radius

  encode_header_append(WKB_GEOMETRY_TYPE_CIRCLE, bytes);
  encode_append(x, bytes);
  encode_append(x, bytes);
  encode_append(radius, bytes);

  return bytes;
}

} // namespace dse

extern "C" {

CassError cass_statement_bind_dse_point(CassStatement* statement,
                                        size_t index,
                                        cass_double_t x, cass_double_t y) {
  dse::Bytes bytes = dse::encode_point(x, y);
  return cass_statement_bind_custom(statement, index, DSE_POINT_TYPE,
                                    bytes.data(), bytes.size());
}

CassError cass_statement_bind_dse_point_by_name(CassStatement* statement,
                                                const char* name,
                                                cass_double_t x, cass_double_t y) {
  return cass_statement_bind_dse_point_by_name_n(statement,
                                                 name, strlen(name),
                                                 x, y);
}

CassError cass_statement_bind_dse_point_by_name_n(CassStatement* statement,
                                                  const char* name, size_t name_length,
                                                  cass_double_t x, cass_double_t y) {
  dse::Bytes bytes = dse::encode_point(x, y);
  return cass_statement_bind_custom_by_name_n(statement,
                                              name, name_length,
                                              DSE_POINT_TYPE, sizeof(DSE_POINT_TYPE) - 1,
                                              bytes.data(), bytes.size());
}


CassError cass_statement_bind_dse_circle(CassStatement* statement,
                                         size_t index,
                                         cass_double_t x, cass_double_t y,
                                         cass_double_t radius) {
  dse::Bytes bytes = dse::encode_circle(x, y, radius);
  return cass_statement_bind_custom(statement, index, DSE_CIRCLE_TYPE,
                                    bytes.data(), bytes.size());
}

CassError cass_statement_bind_dse_circle_by_name(CassStatement* statement,
                                                 const char* name,
                                                 cass_double_t x, cass_double_t y,
                                                 cass_double_t radius) {
  return cass_statement_bind_dse_circle_by_name_n(statement,
                                                  name, strlen(name),
                                                  x, y, radius);
}

CassError cass_statement_bind_dse_circle_by_name_n(CassStatement* statement,
                                                   const char* name, size_t name_length,
                                                   cass_double_t x, cass_double_t y,
                                                   cass_double_t radius) {
  dse::Bytes bytes = dse::encode_circle(x, y, radius);
  return cass_statement_bind_custom_by_name_n(statement,
                                              name, name_length,
                                              DSE_CIRCLE_TYPE, sizeof(DSE_CIRCLE_TYPE) - 1,
                                              bytes.data(), bytes.size());
}

CassError cass_statement_bind_dse_line_string(CassStatement* statement,
                                              size_t index,
                                              const DseLineString* line_string) {
  return cass_statement_bind_custom(statement, index, DSE_LINE_STRING_TYPE,
                                    line_string->bytes().data(), line_string->bytes().size());
}

CassError cass_statement_bind_dse_line_string_by_name(CassStatement* statement,
                                                      const char* name,
                                                      const DseLineString* line_string) {
  return cass_statement_bind_custom_by_name(statement, name, DSE_LINE_STRING_TYPE,
                                            line_string->bytes().data(), line_string->bytes().size());
}

CassError cass_statement_bind_dse_line_string_by_name_n(CassStatement* statement,
                                                        const char* name, size_t name_length,
                                                        const DseLineString* line_string) {
  return cass_statement_bind_custom_by_name_n(statement,
                                              name, name_length,
                                              DSE_LINE_STRING_TYPE, sizeof(DSE_LINE_STRING_TYPE) - 1,
                                              line_string->bytes().data(), line_string->bytes().size());
}

CassError cass_statement_bind_dse_polygon(CassStatement* statement,
                                          size_t index,
                                           const DsePolygon* polygon) {
  return cass_statement_bind_custom(statement, index, DSE_POLYGON_TYPE,
                                    polygon->bytes().data(), polygon->bytes().size());
}

CassError cass_statement_bind_dse_polygon_by_name(CassStatement* statement,
                                                  const char* name,
                                                  const DsePolygon* polygon) {
  return cass_statement_bind_custom_by_name(statement, name, DSE_POLYGON_TYPE,
                                            polygon->bytes().data(), polygon->bytes().size());
}

CassError cass_statement_bind_dse_polygon_by_name_n(CassStatement* statement,
                                                    const char* name, size_t name_length,
                                                    const DsePolygon* polygon) {
  return cass_statement_bind_custom_by_name_n(statement,
                                              name, name_length,
                                              DSE_POLYGON_TYPE, sizeof(DSE_POLYGON_TYPE) - 1,
                                              polygon->bytes().data(), polygon->bytes().size());
}

} // extern "C"
