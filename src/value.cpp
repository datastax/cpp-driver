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

CassError cass_value_get_dse_circle(const CassValue* value,
                                    cass_double_t* x, cass_double_t* y,
                                    cass_double_t* radius) {
  const cass_byte_t* pos;
  size_t size;

  CassError rc = dse::validate_data_type(value, DSE_CIRCLE_TYPE);
  if (rc != CASS_OK) return rc;

  rc = cass_value_get_bytes(value, &pos, &size);
  if (rc != CASS_OK) return rc;

  if (size < WKB_HEADER_SIZE + 3 * sizeof(cass_double_t)) {
    return CASS_ERROR_LIB_NOT_ENOUGH_DATA;
  }

  dse::WkbByteOrder byte_order;
  if (dse::decode_header(pos, &byte_order) != dse::WKB_GEOMETRY_TYPE_CIRCLE) {
    return CASS_ERROR_LIB_INVALID_DATA;
  }
  pos += WKB_HEADER_SIZE;

  *x = dse::decode_double(pos, byte_order);
  pos += sizeof(cass_double_t);

  *y = dse::decode_double(pos, byte_order);
  pos += sizeof(cass_double_t);

  *radius = dse::decode_double(pos, byte_order);

  return CASS_OK;
}

} // extern "C"
