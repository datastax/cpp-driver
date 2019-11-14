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

#include "macros.hpp"
#include "string_ref.hpp"

#include "dse_serialization.hpp"
#include "dse_validate.hpp"

using namespace datastax::internal;
using namespace datastax::internal::enterprise;

extern "C" {

CassError cass_value_get_dse_point(const CassValue* value, cass_double_t* x, cass_double_t* y) {
  const cass_byte_t* pos;
  size_t size;

  CassError rc = validate_data_type(value, DSE_POINT_TYPE);
  if (rc != CASS_OK) return rc;

  rc = cass_value_get_bytes(value, &pos, &size);
  if (rc != CASS_OK) return rc;

  if (size < WKB_HEADER_SIZE + 2 * sizeof(cass_double_t)) {
    return CASS_ERROR_LIB_NOT_ENOUGH_DATA;
  }

  WkbByteOrder byte_order;
  if (decode_header(pos, &byte_order) != WKB_GEOMETRY_TYPE_POINT) {
    return CASS_ERROR_LIB_INVALID_DATA;
  }
  pos += WKB_HEADER_SIZE;

  *x = decode_double(pos, byte_order);
  pos += sizeof(cass_double_t);

  *y = decode_double(pos, byte_order);

  return CASS_OK;
}

CassError cass_value_get_dse_date_range(const CassValue* value, DseDateRange* range) {
  size_t size = 0;
  size_t expected_size = 0;
  const char* pos = NULL;
  const char* end = NULL;
  DateRangeBoundType range_type;
  DseDateRangeBound* first_bound = NULL;
  int8_t decoded_byte = 0;

  CassError rc = validate_data_type(value, DSE_DATE_RANGE_TYPE);
  if (rc != CASS_OK) return rc;

  rc = cass_value_get_string(value, &pos, &size);
  if (rc != CASS_OK) return rc;

  if (size == 0) {
    return CASS_ERROR_LIB_NOT_ENOUGH_DATA;
  }

  end = pos + size;

  // The format of the data is <type int8>[<from_time int64><from_precision int8>[<to_time
  // int64><to_precision int8>]] Depending on the type of range, we may have a subset of the
  // remaining fields. This translates to having 0, 1, or 2 bounds. If we have one bound, it may be
  // an upper or lower bound.

  range_type = static_cast<DateRangeBoundType>(*pos++);

  range->is_single_date =
      static_cast<cass_bool_t>(range_type == DATE_RANGE_BOUND_TYPE_SINGLE_DATE ||
                               range_type == DATE_RANGE_BOUND_TYPE_SINGLE_DATE_OPEN);
  range->lower_bound = dse_date_range_bound_unbounded();
  range->upper_bound = dse_date_range_bound_unbounded();

  switch (range_type) {
    case DATE_RANGE_BOUND_TYPE_BOTH_OPEN_RANGE:
    case DATE_RANGE_BOUND_TYPE_SINGLE_DATE_OPEN:
      expected_size = sizeof(int8_t);
      break;
    case DATE_RANGE_BOUND_TYPE_SINGLE_DATE:
    case DATE_RANGE_BOUND_TYPE_OPEN_RANGE_HIGH:
    case DATE_RANGE_BOUND_TYPE_OPEN_RANGE_LOW:
      // type, from_time, from_precision
      expected_size = sizeof(int8_t) + sizeof(int64_t) + sizeof(int8_t);
      first_bound = (range_type == DATE_RANGE_BOUND_TYPE_OPEN_RANGE_LOW) ? &(range->upper_bound)
                                                                         : &(range->lower_bound);
      break;
    case DATE_RANGE_BOUND_TYPE_CLOSED_RANGE:
      // type, from_time, from_precision, to_time, to_precision
      expected_size =
          sizeof(int8_t) + sizeof(int64_t) + sizeof(int8_t) + sizeof(int64_t) + sizeof(int8_t);
      first_bound = &(range->lower_bound);
      break;
    default:
      return CASS_ERROR_LIB_INVALID_DATA;
  }

  if (size < expected_size) {
    return CASS_ERROR_LIB_NOT_ENOUGH_DATA;
  }

  if (pos == end) {
    return CASS_OK;
  }

  // We have at least one bound; write to the attribute that was chosen earlier.
  pos = decode_int64(const_cast<char*>(pos), first_bound->time_ms);
  pos = decode_int8(const_cast<char*>(pos), decoded_byte);
  first_bound->precision = static_cast<DseDateRangePrecision>(decoded_byte);

  if (pos == end) {
    return CASS_OK;
  }

  // This is the second bound; must be upper.
  pos = decode_int64(const_cast<char*>(pos), range->upper_bound.time_ms);
  decode_int8(const_cast<char*>(pos), decoded_byte);
  range->upper_bound.precision = static_cast<DseDateRangePrecision>(decoded_byte);

  return CASS_OK;
}

} // extern "C"
