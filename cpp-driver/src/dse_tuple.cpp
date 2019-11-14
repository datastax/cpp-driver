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

CassError cass_tuple_set_dse_point(CassTuple* tuple, size_t index, cass_double_t x,
                                   cass_double_t y) {
  Bytes bytes = encode_point(x, y);
  return cass_tuple_set_custom(tuple, index, DSE_POINT_TYPE, bytes.data(), bytes.size());
}

CassError cass_tuple_set_dse_line_string(CassTuple* tuple, size_t index,
                                         const DseLineString* line_string) {
  return cass_tuple_set_custom(tuple, index, DSE_LINE_STRING_TYPE, line_string->bytes().data(),
                               line_string->bytes().size());
}

CassError cass_tuple_set_dse_polygon(CassTuple* tuple, size_t index, const DsePolygon* polygon) {
  return cass_tuple_set_custom(tuple, index, DSE_POLYGON_TYPE, polygon->bytes().data(),
                               polygon->bytes().size());
}

CassError cass_tuple_set_dse_date_range(CassTuple* tuple, size_t index, const DseDateRange* range) {
  Bytes bytes = encode_date_range(range);
  return cass_tuple_set_custom(tuple, index, DSE_DATE_RANGE_TYPE, bytes.data(), bytes.size());
}

} // extern "C"
