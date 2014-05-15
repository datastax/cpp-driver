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
cass_value_get_int(cass_value_t* value,
                   cass_int32_t* output) {
  if(value->type != CASS_VALUE_TYPE_INT) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  // TODO(mpenick): const-correctness is a noble goal! :)
  cass::decode_int(const_cast<char*>(value->buffer.data()), *output);
  return CASS_OK;
}

cass_code_t
cass_value_get_bigint(cass_value_t* value,
                      cass_int64_t* output) {
  if(value->type != CASS_VALUE_TYPE_BIGINT) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cass::decode_int64(const_cast<char*>(value->buffer.data()), *output);
  return CASS_OK;
}

cass_code_t
cass_value_get_float(cass_value_t* value,
                     cass_float_t* output) {
  if(value->type != CASS_VALUE_TYPE_FLOAT) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cass::decode_float(const_cast<char*>(value->buffer.data()), *output);
  return CASS_OK;
}

cass_code_t
cass_value_get_double(cass_value_t* value,
                      cass_double_t* output) {
  if(value->type != CASS_VALUE_TYPE_DOUBLE) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cass::decode_double(const_cast<char*>(value->buffer.data()), *output);
  return CASS_OK;
}

cass_code_t
cass_value_get_bool(cass_value_t* value,
                    cass_bool_t* output) {
  return CASS_OK;
}

cass_code_t
cass_value_get_timestamp(cass_value_t* value,
                         cass_int64_t* output) {
  if(value->type != CASS_VALUE_TYPE_TIMESTAMP) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cass::decode_int64(const_cast<char*>(value->buffer.data()), *output);
  return CASS_OK;
}

cass_code_t
cass_value_get_uuid(cass_value_t* value,
                    cass_uuid_t output) {
  if(value->type != CASS_VALUE_TYPE_UUID) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  memcpy(output, value->buffer.data(), sizeof(cass_uuid_t));
  return CASS_OK;
}

cass_code_t
cass_value_get_counter(cass_value_t* value,
                       cass_int64_t* output) {
  if(value->type != CASS_VALUE_TYPE_COUNTER) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cass::decode_int64(const_cast<char*>(value->buffer.data()), *output);
  return CASS_OK;
}

cass_code_t
cass_value_get_string(cass_value_t* value,
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
cass_value_get_blob(cass_value_t* value,
                    cass_uint8_t* output,
                    cass_size_t output_len,
                    cass_size_t* copied) {
  if(value->type != CASS_VALUE_TYPE_BLOB) {
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
cass_value_get_decimal(cass_value_t* value,
                       cass_uint32_t* scale,
                       cass_uint8_t* output, cass_size_t output_length,
                       cass_size_t* copied) {
  return CASS_OK;
}

cass_code_t
cass_value_get_varint(cass_value_t* value,
                      cass_uint8_t* output,
                      cass_size_t output_len,
                      cass_size_t* copied) {
  if(value->type != CASS_VALUE_TYPE_VARINT) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cass_size_t size = std::min(output_len, value->buffer.size());
  memcpy(output, value->buffer.data(), size);
  if(copied != NULL) {
    *copied = size;
  }
  return CASS_OK;
}

cass_value_type_t
cass_value_type(cass_value_t* value) {
  return value->type;
}

cass_bool_t
cass_value_is_collection(cass_value_t* value) {
  cass_value_type_t type = value->type;
  return type == CASS_VALUE_TYPE_LIST ||
         type == CASS_VALUE_TYPE_MAP ||
         type == CASS_VALUE_TYPE_SET;
}

cass_size_t
cass_value_item_count(cass_value_t* collection) {
  return collection->count;
}

cass_value_type_t
cass_value_primary_sub_type(cass_value_t* collection) {
  return collection->primary_type;
}

cass_value_type_t
cass_value_secondary_sub_type(cass_value_t* collection) {
  return collection->secondary_type;
}

} // extern "C"
