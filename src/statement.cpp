/*
  Copyright (c) 2014-2016 DataStax
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
