/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include "serialization.hpp"
#include "cpp-driver/src/serialization.hpp"

namespace dse {

Bytes encode_date_range(const DseDateRange *range) {
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
      cass::encode_int8(pos, range_type);
      break;
    case DATE_RANGE_BOUND_TYPE_SINGLE_DATE:
    case DATE_RANGE_BOUND_TYPE_OPEN_RANGE_HIGH:
      // type, from_time, from_precision
      bytes.resize(sizeof(int8_t) + sizeof(int64_t) + sizeof(int8_t));
      pos = reinterpret_cast<char*>(&bytes[0]);
      pos = cass::encode_int8(pos, range_type);
      pos = cass::encode_int64(pos, range->lower_bound.time_ms);
      cass::encode_int8(pos, range->lower_bound.precision);
      break;
    case DATE_RANGE_BOUND_TYPE_OPEN_RANGE_LOW:
      // type, to_time, to_precision
      bytes.resize(sizeof(int8_t) + sizeof(int64_t) + sizeof(int8_t));
      pos = reinterpret_cast<char*>(&bytes[0]);
      pos = cass::encode_int8(pos, range_type);
      pos = cass::encode_int64(pos, range->upper_bound.time_ms);
      cass::encode_int8(pos, range->upper_bound.precision);
      break;
    case DATE_RANGE_BOUND_TYPE_CLOSED_RANGE:
      // type, from_time, from_precision, to_time, to_precision
      bytes.resize(sizeof(int8_t) + sizeof(int64_t) + sizeof(int8_t) + sizeof(int64_t) + sizeof(int8_t));
      pos = reinterpret_cast<char*>(&bytes[0]);
      pos = cass::encode_int8(pos, range_type);
      pos = cass::encode_int64(pos, range->lower_bound.time_ms);
      pos = cass::encode_int8(pos, range->lower_bound.precision);
      pos = cass::encode_int64(pos, range->upper_bound.time_ms);
      cass::encode_int8(pos, range->upper_bound.precision);
      break;
  }
  return bytes;
}

}

DseDateRangeBound dse_date_range_bound_init(DseDateRangePrecision precision,
                                            cass_int64_t time_ms) {
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

DseDateRange* dse_date_range_init(DseDateRange* range,
                                  DseDateRangeBound lower_bound,
                                  DseDateRangeBound upper_bound) {
  range->lower_bound = lower_bound;
  range->upper_bound = upper_bound;
  range->is_single_date = cass_false;
  return range;
}

DseDateRange* dse_date_range_init_single_date(DseDateRange* range,
                                              DseDateRangeBound date) {
  range->lower_bound = date;
  range->is_single_date = cass_true;
  return range;
}
