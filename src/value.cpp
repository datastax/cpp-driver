/*
  Copyright (c) 2014-2016 DataStax
*/

#include "dse.h"

#include "macros.hpp"
#include "string_ref.hpp"

#include "serialization.hpp"
#include "validate.hpp"

extern "C" {

CassError cass_value_get_dse_point(const CassValue* value,
                                   cass_double_t* x, cass_double_t* y) {
  const cass_byte_t* pos;
  size_t size;

  CassError rc = dse::validate_data_type(value, DSE_POINT_TYPE);
  if (rc != CASS_OK) return rc;

  rc = cass_value_get_bytes(value, &pos, &size);
  if (rc != CASS_OK) return rc;

  if (size < WKB_HEADER_SIZE + 2 * sizeof(cass_double_t)) {
    return CASS_ERROR_LIB_NOT_ENOUGH_DATA;
  }

  dse::WkbByteOrder byte_order;
  if (dse::decode_header(pos, &byte_order) != dse::WKB_GEOMETRY_TYPE_POINT) {
    return CASS_ERROR_LIB_INVALID_DATA;
  }
  pos += WKB_HEADER_SIZE;

  *x = dse::decode_double(pos, byte_order);
  pos += sizeof(cass_double_t);

  *y = dse::decode_double(pos, byte_order);

  return CASS_OK;
}

} // extern "C"
