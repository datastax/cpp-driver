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

#include "value.hpp"

#include "collection_iterator.hpp"
#include "data_type.hpp"
#include "external.hpp"
#include "serialization.hpp"

extern "C" {

const CassDataType* cass_value_data_type(const CassValue* value) {
  return CassDataType::to(value->data_type().get());
}

CassError cass_value_get_int8(const CassValue* value, cass_int8_t* output) {
  if (value == NULL || value->is_null()) return CASS_ERROR_LIB_NULL_VALUE;
  if (value->value_type() != CASS_VALUE_TYPE_TINY_INT) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }
  cass::decode_int8(value->data(), *output);
  return CASS_OK;
}

CassError cass_value_get_int16(const CassValue* value, cass_int16_t* output) {
  if (value == NULL || value->is_null()) return CASS_ERROR_LIB_NULL_VALUE;
  if (value->value_type() != CASS_VALUE_TYPE_SMALL_INT) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }
  cass::decode_int16(value->data(), *output);
  return CASS_OK;
}

CassError cass_value_get_int32(const CassValue* value, cass_int32_t* output) {
  if (value == NULL || value->is_null()) return CASS_ERROR_LIB_NULL_VALUE;
  if (value->value_type() != CASS_VALUE_TYPE_INT) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }
  cass::decode_int32(value->data(), *output);
  return CASS_OK;

}

CassError cass_value_get_uint32(const CassValue* value, cass_uint32_t* output) {
  if (value == NULL || value->is_null()) return CASS_ERROR_LIB_NULL_VALUE;
  if (value->value_type() != CASS_VALUE_TYPE_DATE) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }
  cass::decode_uint32(value->data(), *output);
  return CASS_OK;
}

CassError cass_value_get_int64(const CassValue* value, cass_int64_t* output) {
  if (value == NULL || value->is_null()) return CASS_ERROR_LIB_NULL_VALUE;
  if (!cass::is_int64_type(value->value_type())) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }
  cass::decode_int64(value->data(), *output);
  return CASS_OK;
}

CassError cass_value_get_float(const CassValue* value, cass_float_t* output) {
  if (value == NULL || value->is_null()) return CASS_ERROR_LIB_NULL_VALUE;
  if (value->value_type() != CASS_VALUE_TYPE_FLOAT) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }
  cass::decode_float(value->data(), *output);
  return CASS_OK;
}

CassError cass_value_get_double(const CassValue* value, cass_double_t* output) {
  if (value == NULL || value->is_null()) return CASS_ERROR_LIB_NULL_VALUE;
  if (value->value_type() != CASS_VALUE_TYPE_DOUBLE) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }
  cass::decode_double(value->data(), *output);
  return CASS_OK;
}

CassError cass_value_get_bool(const CassValue* value, cass_bool_t* output) {
  if (value == NULL || value->is_null()) return CASS_ERROR_LIB_NULL_VALUE;
  uint8_t byte;
  if (value->value_type() != CASS_VALUE_TYPE_BOOLEAN) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }
  cass::decode_byte(value->data(), byte);
  *output = static_cast<cass_bool_t>(byte);
  return CASS_OK;
}

CassError cass_value_get_uuid(const CassValue* value, CassUuid* output) {
  if (value == NULL || value->is_null()) return CASS_ERROR_LIB_NULL_VALUE;
  if (!cass::is_uuid_type(value->value_type())) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }
  cass::decode_uuid(value->data(), output);
  return CASS_OK;
}

CassError cass_value_get_inet(const CassValue* value, CassInet* output) {
  if (value == NULL || value->is_null()) return CASS_ERROR_LIB_NULL_VALUE;
  if (value->value_type() != CASS_VALUE_TYPE_INET) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }
  output->address_length = value->size();
  memcpy(output->address, value->data(), value->size());
  return CASS_OK;
}

CassError cass_value_get_string(const CassValue* value,
                                const char** output,
                                size_t* output_length) {
  if (value == NULL || value->is_null()) return CASS_ERROR_LIB_NULL_VALUE;
  *output = value->data();
  *output_length = value->size();
  return CASS_OK;
}

CassError cass_value_get_bytes(const CassValue* value,
                               const cass_byte_t** output,
                               size_t* output_size) {
  if (value == NULL || value->is_null()) return CASS_ERROR_LIB_NULL_VALUE;
  *output = reinterpret_cast<cass_byte_t*>(value->data());
  *output_size = value->size();
  return CASS_OK;
}

static const cass_byte_t* decode_vint(const cass_byte_t* input, const cass_byte_t* end, cass_int64_t* output)
{
  int num_extra_bytes;
  int i;
  cass_byte_t first_byte = *input++;
  if (first_byte <= 127) {
    // If this is a multibyte vint, at least the MSB of the first byte
    // will be set. Since that's not the case, this is a one-byte value.
    *output = first_byte;
  } else {
    // The number of consecutive most significant bits of the first-byte tell us how
    // many additional bytes are in this vint. Count them like this:
    // 1. Invert the firstByte so that all leading 1s become 0s.
    // 2. Count the number of leading zeros; num_leading_zeros assumes a 64-bit long.
    // 3. We care about leading 0s in the byte, not int, so subtract out the
    //    appropriate number of extra bits (56 for a 64-bit int).

    // We mask out high-order bits to prevent sign-extension as the value is placed in a 64-bit arg
    // to the num_leading_zeros function.
    num_extra_bytes = cass::num_leading_zeros(~first_byte & 0xff) - 56;

    // Error out if we don't have num_extra_bytes left in our data.
    if (input + num_extra_bytes > end) {
      // There aren't enough bytes. This duration object is not fully defined.
      return NULL;
    }

    // Build up the vint value one byte at a time from the data bytes.
    // The firstByte contains size as well as the most significant bits of
    // the value. Extract just the value.
    *output = first_byte & (0xff >> num_extra_bytes);
    for (i = 0; i < num_extra_bytes; ++i) {
      cass_byte_t b = *input++;
      *output <<= 8;
      *output |= b & 0xff;
    }
  }
  return input;
}

CassError cass_value_get_duration(const CassValue* value, cass_int32_t* months, cass_int32_t* days, cass_int32_t* nanos)
{
  cass_int32_t *outs[3];
  int ctr;
  size_t data_size = 0;
  const cass_byte_t* cur_byte = NULL;
  const cass_byte_t* end = NULL;

  if (value == NULL || value->is_null()) return CASS_ERROR_LIB_NULL_VALUE;
  if (!cass_value_is_duration(value)) return CASS_ERROR_LIB_INVALID_VALUE_TYPE;

  // Package up the out-args in an array. Duration's always have months, then days, then nanos.
  outs[0] = months;
  outs[1] = days;
  outs[2] = nanos;

  cass_value_get_bytes(value, &cur_byte, &data_size);
  end = cur_byte + data_size;

  for (ctr = 0; ctr < 3 && cur_byte != end; ++ctr) {
    cass_int64_t decoded = 0;
    cur_byte = decode_vint(cur_byte, end, &decoded);
    if (cur_byte == NULL)
      return CASS_ERROR_LIB_BAD_PARAMS;
    *outs[ctr] = static_cast<cass_int32_t>(cass::decode_zig_zag(decoded));
  }

  if (ctr < 3) {
    // There aren't enough bytes. This duration object is not fully defined.
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  return CASS_OK;
}

CassError cass_value_get_decimal(const CassValue* value,
                                 const cass_byte_t** varint,
                                 size_t* varint_size,
                                 cass_int32_t* scale) {
  if (value == NULL || value->is_null()) return CASS_ERROR_LIB_NULL_VALUE;
  if (value->value_type() != CASS_VALUE_TYPE_DECIMAL) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }
  const char* buffer = cass::decode_int32(value->data(), *scale);
  *varint = reinterpret_cast<const cass_byte_t*>(buffer);
  *varint_size = value->size() - sizeof(int32_t);
  return CASS_OK;
}

CassValueType cass_value_type(const CassValue* value) {
  return value->value_type();
}

cass_bool_t cass_value_is_null(const CassValue* value) {
  return static_cast<cass_bool_t>(value->is_null());
}

cass_bool_t cass_value_is_collection(const CassValue* value) {
  return static_cast<cass_bool_t>(value->is_collection());
}

cass_bool_t cass_value_is_duration(const CassValue* value) {
  cass::IsValidDataType<cass::CassDuration> is_valid;
  cass::CassDuration dummy(0, 0, 0);
  return static_cast<cass_bool_t>(is_valid(dummy, value->data_type()));
}

size_t cass_value_item_count(const CassValue* collection) {
  return collection->count();
}

CassValueType cass_value_primary_sub_type(const CassValue* collection) {
  return collection->primary_value_type();
}

CassValueType cass_value_secondary_sub_type(const CassValue* collection) {
  return collection->secondary_value_type();
}

} // extern "C"


namespace cass {

Value::Value(int protocol_version,
             const DataType::ConstPtr& data_type,
             char* data, int32_t size)
  : protocol_version_(protocol_version)
  , data_type_(data_type) {
  if (size > 0 && data_type->is_collection()) {
    data_ = decode_size(protocol_version, data, count_);
    if (protocol_version_ >= 3) {
      size_ = size - sizeof(int32_t);
    } else {
      size_ = size - sizeof(uint16_t);
    }
  } else {
    if (data_type->is_tuple()) {
      SharedRefPtr<const CompositeType> composite_type(data_type);
      count_ = composite_type->types().size();
    } else if (data_type->is_user_type()) {
      UserType::ConstPtr user_type(data_type);
      count_ = user_type->fields().size();
    } else {
      count_ = 0;
    }
    data_ = data;
    size_ = size;
  }
}

bool Value::as_bool() const {
  assert(!is_null() && value_type() == CASS_VALUE_TYPE_BOOLEAN);
  uint8_t value;
  decode_byte(data_, value);
  return value != 0;
}

int32_t Value::as_int32() const {
  assert(!is_null() && value_type() == CASS_VALUE_TYPE_INT);
  int32_t value;
  decode_int32(data_, value);
  return value;
}

CassUuid Value::as_uuid() const {
  assert(!is_null() && (value_type() == CASS_VALUE_TYPE_UUID ||
                        value_type() == CASS_VALUE_TYPE_TIMEUUID));
  CassUuid value;
  decode_uuid(data_, &value);
  return value;

}

StringVec Value::as_stringlist() const {
  assert(!is_null() && (value_type() == CASS_VALUE_TYPE_LIST ||
                        value_type() == CASS_VALUE_TYPE_SET) &&
         primary_value_type() == CASS_VALUE_TYPE_VARCHAR);
  StringVec stringlist;
  CollectionIterator iterator(this);
  while (iterator.next()) {
    stringlist.push_back(iterator.value()->to_string());
  }
  return stringlist;
}

} // namespace cassandra
