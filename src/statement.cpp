/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include "dse.h"

#include "date_range.hpp"
#include "point.hpp"
#include "line_string.hpp"
#include "polygon.hpp"

#include "statement.hpp"

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
                                                 name, SAFE_STRLEN(name),
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

CassError cass_statement_bind_dse_date_range(CassStatement* statement,
                                             size_t index,
                                             const DseDateRange* range) {
  dse::Bytes bytes = dse::encode_date_range(range);
  return cass_statement_bind_custom(statement, index, DSE_DATE_RANGE_TYPE, bytes.data(), bytes.size());
}

CassError cass_statement_bind_dse_date_range_by_name(CassStatement* statement,
                                                     const char* name,
                                                     const DseDateRange* range) {
  return cass_statement_bind_dse_date_range_by_name_n(statement, name, SAFE_STRLEN(name), range);
}

CassError cass_statement_bind_dse_date_range_by_name_n(CassStatement* statement,
                                                       const char* name, size_t name_length,
                                                       const DseDateRange* range) {
  dse::Bytes bytes = dse::encode_date_range(range);
  return cass_statement_bind_custom_by_name_n(statement, name, name_length,
                                              DSE_DATE_RANGE_TYPE, sizeof(DSE_DATE_RANGE_TYPE) - 1,
                                              bytes.data(), bytes.size());
}

CassError cass_statement_set_execute_as_n(CassStatement* statement,
                                          const char* name, size_t name_length) {
  statement->set_custom_payload("ProxyExecute", reinterpret_cast<const uint8_t *>(name), name_length);
  return CASS_OK;
}

CassError cass_statement_set_execute_as(CassStatement* statement,
                                        const char* name) {
  return cass_statement_set_execute_as_n(statement, name, SAFE_STRLEN(name));
}

} // extern "C"
