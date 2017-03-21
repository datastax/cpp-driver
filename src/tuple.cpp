/*
  Copyright (c) 2016 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include "dse.h"

#include "date_range.hpp"
#include "point.hpp"
#include "line_string.hpp"
#include "polygon.hpp"

CassError cass_tuple_set_dse_point(CassTuple* tuple,
                                   size_t index,
                                   cass_double_t x, cass_double_t y) {
  dse::Bytes bytes = dse::encode_point(x, y);
  return cass_tuple_set_custom(tuple, index, DSE_POINT_TYPE,
                               bytes.data(), bytes.size());
}

CassError cass_tuple_set_dse_line_string(CassTuple* tuple,
                                         size_t index,
                                         const DseLineString* line_string) {
  return cass_tuple_set_custom(tuple, index, DSE_LINE_STRING_TYPE,
                               line_string->bytes().data(), line_string->bytes().size());
}

CassError cass_tuple_set_dse_polygon(CassTuple* tuple,
                                     size_t index,
                                     const DsePolygon* polygon) {
  return cass_tuple_set_custom(tuple, index, DSE_POLYGON_TYPE,
                               polygon->bytes().data(), polygon->bytes().size());
}

CassError cass_tuple_set_dse_date_range(CassTuple* tuple,
                                        size_t index,
                                        const DseDateRange* range) {
  dse::Bytes bytes = dse::encode_date_range(range);
  return cass_tuple_set_custom(tuple, index, DSE_DATE_RANGE_TYPE,
                               bytes.data(), bytes.size());
}
