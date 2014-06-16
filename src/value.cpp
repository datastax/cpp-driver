/*
  Copyright (c) 2014 DataStax

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

#include "cassandra.hpp"
#include "value.hpp"

extern "C" {

CassError cass_value_get_int32(const CassValue* value, cass_int32_t* output) {
  if (value->type != CASS_VALUE_TYPE_INT) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }
  cass::decode_int(value->buffer.data(), *output);
  return CASS_OK;
}

CassError cass_value_get_int64(const CassValue* value, cass_int64_t* output) {
  if (value->type != CASS_VALUE_TYPE_BIGINT &&
      value->type != CASS_VALUE_TYPE_COUNTER &&
      value->type != CASS_VALUE_TYPE_TIMESTAMP) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }
  cass::decode_int64(value->buffer.data(), *output);
  return CASS_OK;
}

CassError cass_value_get_float(const CassValue* value, cass_float_t* output) {
  if (value->type != CASS_VALUE_TYPE_FLOAT) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }
  cass::decode_float(value->buffer.data(), *output);
  return CASS_OK;
}

CassError cass_value_get_double(const CassValue* value, cass_double_t* output) {
  if (value->type != CASS_VALUE_TYPE_DOUBLE) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }
  cass::decode_double(value->buffer.data(), *output);
  return CASS_OK;
}

CassError cass_value_get_bool(const CassValue* value, cass_bool_t* output) {
  uint8_t byte;
  cass::decode_byte(value->buffer.data(), byte);
  *output = static_cast<cass_bool_t>(byte);
  return CASS_OK;
}

CassError cass_value_get_uuid(const CassValue* value, CassUuid output) {
  if (value->type != CASS_VALUE_TYPE_UUID &&
      value->type != CASS_VALUE_TYPE_TIMEUUID) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }
  memcpy(output, value->buffer.data(), sizeof(CassUuid));
  return CASS_OK;
}

CassError cass_value_get_inet(const CassValue* value, CassInet* output) {
  if (value->type != CASS_VALUE_TYPE_INET) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }
  output->address_length = value->buffer.size();
  memcpy(output->address, value->buffer.data(), value->buffer.size());
  return CASS_OK;
}

CassError cass_value_get_string(const CassValue* value, CassString* output) {
  output->data = value->buffer.data();
  output->length = value->buffer.size();
  return CASS_OK;
}

CassError cass_value_get_bytes(const CassValue* value, CassBytes* output) {
  output->data = reinterpret_cast<cass_byte_t*>(value->buffer.data());
  output->size = value->buffer.size();
  return CASS_OK;
}

CassError cass_value_get_decimal(const CassValue* value, CassDecimal* output) {
  if (value->type != CASS_VALUE_TYPE_DECIMAL) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }
  const char* buffer = cass::decode_int(value->buffer.data(), output->scale);
  output->varint.data = reinterpret_cast<const cass_byte_t*>(buffer);
  output->varint.size = value->buffer.size() - sizeof(int32_t);
  return CASS_OK;
}

CassValueType cass_value_type(const CassValue* value) {
  return value->type;
}

cass_bool_t cass_value_is_null(const CassValue* value) {
  return static_cast<cass_bool_t>(value->buffer.size() < 0);
}

cass_bool_t cass_value_is_collection(const CassValue* value) {
  CassValueType type = value->type;
  return static_cast<cass_bool_t>(type == CASS_VALUE_TYPE_LIST ||
                                  type == CASS_VALUE_TYPE_MAP ||
                                  type == CASS_VALUE_TYPE_SET);
}

cass_size_t cass_value_item_count(const CassValue* collection) {
  return collection->count;
}

CassValueType cass_value_primary_sub_type(const CassValue* collection) {
  return collection->primary_type;
}

CassValueType cass_value_secondary_sub_type(const CassValue* collection) {
  return collection->secondary_type;
}

} // extern "C"
