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

CassError cass_collection_append_dse_point(CassCollection* collection,
                                           cass_double_t x, cass_double_t y) {
  dse::Bytes bytes = dse::encode_point(x, y);
  return cass_collection_append_custom(collection, DSE_POINT_TYPE,
                                       bytes.data(), bytes.size());
}

CassError cass_collection_append_dse_line_string(CassCollection* collection,
                                                 const DseLineString* line_string) {
  return cass_collection_append_custom(collection, DSE_LINE_STRING_TYPE,
                                       line_string->bytes().data(), line_string->bytes().size());
}

CassError cass_collection_append_dse_polygon(CassCollection* collection,
                                             const DsePolygon* polygon) {
  return cass_collection_append_custom(collection, DSE_POLYGON_TYPE,
                                       polygon->bytes().data(), polygon->bytes().size());
}

CassError cass_collection_append_dse_date_range(CassCollection* collection,
                                                const DseDateRange* range) {
  dse::Bytes bytes = dse::encode_date_range(range);
  return cass_collection_append_custom(collection, DSE_DATE_RANGE_TYPE,
                                       bytes.data(), bytes.size());
}
