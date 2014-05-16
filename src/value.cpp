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

cass_code_t
cass_value_get_int32(const cass_value_t* value,
                   cass_int32_t* output) {
  if(value->type != CASS_VALUE_TYPE_INT) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cass::decode_int(value->buffer.data(), *output);
  return CASS_OK;
}

cass_code_t
cass_value_get_int64(const cass_value_t* value,
                      cass_int64_t* output) {
  if(value->type != CASS_VALUE_TYPE_BIGINT &&
     value->type != CASS_VALUE_TYPE_COUNTER &&
     value->type != CASS_VALUE_TYPE_TIMESTAMP) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cass::decode_int64(value->buffer.data(), *output);
  return CASS_OK;
}

cass_code_t
cass_value_get_float(const cass_value_t* value,
                     cass_float_t* output) {
  if(value->type != CASS_VALUE_TYPE_FLOAT) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cass::decode_float(value->buffer.data(), *output);
  return CASS_OK;
}

cass_code_t
cass_value_get_double(const cass_value_t* value,
                      cass_double_t* output) {
  if(value->type != CASS_VALUE_TYPE_DOUBLE) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cass::decode_double(value->buffer.data(), *output);
  return CASS_OK;
}

cass_code_t
cass_value_get_bool(const cass_value_t* value,
                    cass_bool_t* output) {
  uint8_t byte;
  cass::decode_byte(value->buffer.data(), byte);
  *output = static_cast<cass_bool_t>(byte);
  return CASS_OK;
}

cass_code_t
cass_value_get_uuid(const cass_value_t* value,
                    cass_uuid_t output) {
  if(value->type != CASS_VALUE_TYPE_UUID) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  memcpy(output, value->buffer.data(), sizeof(cass_uuid_t));
  return CASS_OK;
}

cass_code_t
cass_value_get_string(const cass_value_t* value,
                      char* output,
                      size_t output_len,
                      cass_size_t* copied) {
  if(value->type != CASS_VALUE_TYPE_ASCII &&
     value->type != CASS_VALUE_TYPE_VARCHAR &&
     value->type != CASS_VALUE_TYPE_TEXT) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cass_size_t size = std::min(output_len, value->buffer.size());
  memcpy(output, value->buffer.data(), size);
  if(copied != NULL) {
    *copied = size;
  }
  return CASS_OK;
}

cass_code_t
cass_value_get_bytes(const cass_value_t* value,
                    cass_byte_t* output,
                    cass_size_t output_len,
                    cass_size_t* copied) {
  if(value->type != CASS_VALUE_TYPE_BLOB &&
     value->type != CASS_VALUE_TYPE_CUSTOM &&
     value->type != CASS_VALUE_TYPE_VARINT) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cass_size_t size = std::min(output_len, value->buffer.size());
  memcpy(output, value->buffer.data(), size);
  if(copied != NULL) {
    *copied = size;
  }
  return CASS_OK;
}

cass_code_t
cass_value_get_decimal(const cass_value_t* value,
                       cass_uint32_t* scale,
                       cass_byte_t* output, cass_size_t output_length,
                       cass_size_t* copied) {
  return CASS_OK;
}

const cass_byte_t*
cass_value_get_data(const cass_value_t* value) {
  return reinterpret_cast<const cass_byte_t*>(value->buffer.data());
}

cass_size_t
cass_value_get_size(const cass_value_t* value) {
  return value->buffer.size();
}


cass_value_type_t
cass_value_type(const cass_value_t* value) {
  return value->type;
}

cass_bool_t
cass_value_is_collection(const cass_value_t* value) {
  cass_value_type_t type = value->type;
  return type == CASS_VALUE_TYPE_LIST ||
         type == CASS_VALUE_TYPE_MAP ||
         type == CASS_VALUE_TYPE_SET;
}

cass_size_t
cass_value_item_count(const cass_value_t* collection) {
  return collection->count;
}

cass_value_type_t
cass_value_primary_sub_type(const cass_value_t* collection) {
  return collection->primary_type;
}

cass_value_type_t
cass_value_secondary_sub_type(const cass_value_t* collection) {
  return collection->secondary_type;
}

} // extern "C"
