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

#include "tuple.hpp"

#include "collection.hpp"
#include "constants.hpp"
#include "encode.hpp"
#include "external.hpp"
#include "macros.hpp"
#include "user_type_value.hpp"

#include <string.h>

using namespace datastax;
using namespace datastax::internal::core;

extern "C" {

CassTuple* cass_tuple_new(size_t item_count) { return CassTuple::to(new Tuple(item_count)); }

CassTuple* cass_tuple_new_from_data_type(const CassDataType* data_type) {
  if (!data_type->is_tuple()) {
    return NULL;
  }
  return CassTuple::to(new Tuple(DataType::ConstPtr(data_type)));
}

void cass_tuple_free(CassTuple* tuple) { delete tuple->from(); }

const CassDataType* cass_tuple_data_type(const CassTuple* tuple) {
  return CassDataType::to(tuple->data_type().get());
}

#define CASS_TUPLE_SET(Name, Params, Value)                                \
  CassError cass_tuple_set_##Name(CassTuple* tuple, size_t index Params) { \
    return tuple->set(index, Value);                                       \
  }

CASS_TUPLE_SET(null, ZERO_PARAMS_(), CassNull())
CASS_TUPLE_SET(int8, ONE_PARAM_(cass_int8_t value), value)
CASS_TUPLE_SET(int16, ONE_PARAM_(cass_int16_t value), value)
CASS_TUPLE_SET(int32, ONE_PARAM_(cass_int32_t value), value)
CASS_TUPLE_SET(uint32, ONE_PARAM_(cass_uint32_t value), value)
CASS_TUPLE_SET(int64, ONE_PARAM_(cass_int64_t value), value)
CASS_TUPLE_SET(float, ONE_PARAM_(cass_float_t value), value)
CASS_TUPLE_SET(double, ONE_PARAM_(cass_double_t value), value)
CASS_TUPLE_SET(bool, ONE_PARAM_(cass_bool_t value), value)
CASS_TUPLE_SET(uuid, ONE_PARAM_(CassUuid value), value)
CASS_TUPLE_SET(inet, ONE_PARAM_(CassInet value), value)
CASS_TUPLE_SET(collection, ONE_PARAM_(const CassCollection* value), value)
CASS_TUPLE_SET(tuple, ONE_PARAM_(const CassTuple* value), value)
CASS_TUPLE_SET(user_type, ONE_PARAM_(const CassUserType* value), value)
CASS_TUPLE_SET(bytes, TWO_PARAMS_(const cass_byte_t* value, size_t value_size),
               CassBytes(value, value_size))
CASS_TUPLE_SET(decimal, THREE_PARAMS_(const cass_byte_t* varint, size_t varint_size, int scale),
               CassDecimal(varint, varint_size, scale))
CASS_TUPLE_SET(duration, THREE_PARAMS_(cass_int32_t months, cass_int32_t days, cass_int64_t nanos),
               CassDuration(months, days, nanos))

#undef CASS_TUPLE_SET

CassError cass_tuple_set_string(CassTuple* tuple, size_t index, const char* value) {
  return tuple->set(index, CassString(value, SAFE_STRLEN(value)));
}

CassError cass_tuple_set_string_n(CassTuple* tuple, size_t index, const char* value,
                                  size_t value_length) {
  return tuple->set(index, CassString(value, value_length));
}

CassError cass_tuple_set_custom(CassTuple* tuple, size_t index, const char* class_name,
                                const cass_byte_t* value, size_t value_size) {
  return tuple->set(index, CassCustom(StringRef(class_name), value, value_size));
}

CassError cass_tuple_set_custom_n(CassTuple* tuple, size_t index, const char* class_name,
                                  size_t class_name_length, const cass_byte_t* value,
                                  size_t value_size) {
  return tuple->set(index, CassCustom(StringRef(class_name, class_name_length), value, value_size));
}

} // extern "C"

CassError Tuple::set(size_t index, CassNull value) {
  CASS_TUPLE_CHECK_INDEX_AND_TYPE(index, value);
  items_[index] = core::encode_with_length(value);
  return CASS_OK;
}

CassError Tuple::set(size_t index, const Tuple* value) {
  CASS_TUPLE_CHECK_INDEX_AND_TYPE(index, value);
  items_[index] = value->encode_with_length();
  return CASS_OK;
}

CassError Tuple::set(size_t index, const Collection* value) {
  CASS_TUPLE_CHECK_INDEX_AND_TYPE(index, value);
  items_[index] = value->encode_with_length();
  return CASS_OK;
}

CassError Tuple::set(size_t index, const UserTypeValue* value) {
  CASS_TUPLE_CHECK_INDEX_AND_TYPE(index, value);
  items_[index] = value->encode_with_length();
  return CASS_OK;
}

Buffer Tuple::encode() const {
  Buffer buf(get_buffers_size());
  encode_buffers(0, &buf);
  return buf;
}

Buffer Tuple::encode_with_length() const {
  size_t buffers_size = get_buffers_size();
  Buffer buf(sizeof(int32_t) + buffers_size);

  size_t pos = buf.encode_int32(0, buffers_size);
  encode_buffers(pos, &buf);

  return buf;
}

size_t Tuple::get_buffers_size() const {
  size_t size = 0;
  for (BufferVec::const_iterator i = items_.begin(), end = items_.end(); i != end; ++i) {
    if (i->size() != 0) {
      size += i->size();
    } else {
      size += sizeof(int32_t); // null
    }
  }
  return size;
}

void Tuple::encode_buffers(size_t pos, Buffer* buf) const {
  for (BufferVec::const_iterator i = items_.begin(), end = items_.end(); i != end; ++i) {
    if (i->size() != 0) {
      pos = buf->copy(pos, i->data(), i->size());
    } else {
      pos = buf->encode_int32(pos, -1); // null
    }
  }
}
