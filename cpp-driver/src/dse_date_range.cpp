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

#include "dse_date_range.hpp"

extern "C" {

DseDateRangeBound dse_date_range_bound_init(DseDateRangePrecision precision, cass_int64_t time_ms) {
  DseDateRangeBound bound;
  bound.precision = precision;
  bound.time_ms = time_ms;
  return bound;
}

DseDateRangeBound dse_date_range_bound_unbounded() {
  DseDateRangeBound bound;
  bound.precision = DSE_DATE_RANGE_PRECISION_UNBOUNDED;
  bound.time_ms = -1;
  return bound;
}

cass_bool_t dse_date_range_bound_is_unbounded(DseDateRangeBound bound) {
  return static_cast<cass_bool_t>(bound.precision == DSE_DATE_RANGE_PRECISION_UNBOUNDED);
}

DseDateRange* dse_date_range_init(DseDateRange* range, DseDateRangeBound lower_bound,
                                  DseDateRangeBound upper_bound) {
  range->lower_bound = lower_bound;
  range->upper_bound = upper_bound;
  range->is_single_date = cass_false;
  return range;
}

DseDateRange* dse_date_range_init_single_date(DseDateRange* range, DseDateRangeBound date) {
  range->lower_bound = date;
  range->is_single_date = cass_true;
  return range;
}

} // extern "C"

namespace datastax { namespace internal { namespace enterprise {

Bytes encode_date_range(const DseDateRange* range) {
  Bytes bytes;
  DateRangeBoundType range_type;
  char* pos = NULL;

  if (range->is_single_date) {
    if (dse_date_range_bound_is_unbounded(range->lower_bound)) {
      range_type = DATE_RANGE_BOUND_TYPE_SINGLE_DATE_OPEN;
    } else {
      range_type = DATE_RANGE_BOUND_TYPE_SINGLE_DATE;
    }
  } else if (dse_date_range_bound_is_unbounded(range->lower_bound) &&
             dse_date_range_bound_is_unbounded(range->upper_bound)) {
    range_type = DATE_RANGE_BOUND_TYPE_BOTH_OPEN_RANGE;
  } else if (dse_date_range_bound_is_unbounded(range->upper_bound)) {
    range_type = DATE_RANGE_BOUND_TYPE_OPEN_RANGE_HIGH;
  } else if (dse_date_range_bound_is_unbounded(range->lower_bound)) {
    range_type = DATE_RANGE_BOUND_TYPE_OPEN_RANGE_LOW;
  } else {
    range_type = DATE_RANGE_BOUND_TYPE_CLOSED_RANGE;
  }

  switch (range_type) {
    case DATE_RANGE_BOUND_TYPE_BOTH_OPEN_RANGE:
    case DATE_RANGE_BOUND_TYPE_SINGLE_DATE_OPEN:
      bytes.resize(sizeof(int8_t));
      pos = reinterpret_cast<char*>(&bytes[0]);
      encode_int8(pos, range_type);
      break;
    case DATE_RANGE_BOUND_TYPE_SINGLE_DATE:
    case DATE_RANGE_BOUND_TYPE_OPEN_RANGE_HIGH:
      // type, from_time, from_precision
      bytes.resize(sizeof(int8_t) + sizeof(int64_t) + sizeof(int8_t));
      pos = reinterpret_cast<char*>(&bytes[0]);
      pos = encode_int8(pos, range_type);
      pos = encode_int64(pos, range->lower_bound.time_ms);
      encode_int8(pos, range->lower_bound.precision);
      break;
    case DATE_RANGE_BOUND_TYPE_OPEN_RANGE_LOW:
      // type, to_time, to_precision
      bytes.resize(sizeof(int8_t) + sizeof(int64_t) + sizeof(int8_t));
      pos = reinterpret_cast<char*>(&bytes[0]);
      pos = encode_int8(pos, range_type);
      pos = encode_int64(pos, range->upper_bound.time_ms);
      encode_int8(pos, range->upper_bound.precision);
      break;
    case DATE_RANGE_BOUND_TYPE_CLOSED_RANGE:
      // type, from_time, from_precision, to_time, to_precision
      bytes.resize(sizeof(int8_t) + sizeof(int64_t) + sizeof(int8_t) + sizeof(int64_t) +
                   sizeof(int8_t));
      pos = reinterpret_cast<char*>(&bytes[0]);
      pos = encode_int8(pos, range_type);
      pos = encode_int64(pos, range->lower_bound.time_ms);
      pos = encode_int8(pos, range->lower_bound.precision);
      pos = encode_int64(pos, range->upper_bound.time_ms);
      encode_int8(pos, range->upper_bound.precision);
      break;
  }
  return bytes;
}

}}} // namespace datastax::internal::enterprise
