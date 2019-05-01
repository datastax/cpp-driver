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

#include "collection.hpp"

#include "constants.hpp"
#include "external.hpp"
#include "macros.hpp"
#include "tuple.hpp"
#include "user_type_value.hpp"

#include <string.h>

using namespace datastax;
using namespace datastax::internal::core;

extern "C" {

CassCollection* cass_collection_new(CassCollectionType type, size_t item_count) {
  Collection* collection = new Collection(type, item_count);
  collection->inc_ref();
  return CassCollection::to(collection);
}

CassCollection* cass_collection_new_from_data_type(const CassDataType* data_type,
                                                   size_t item_count) {
  if (!data_type->is_collection()) {
    return NULL;
  }
  Collection* collection = new Collection(DataType::ConstPtr(data_type), item_count);
  collection->inc_ref();
  return CassCollection::to(collection);
}

void cass_collection_free(CassCollection* collection) { collection->dec_ref(); }

const CassDataType* cass_collection_data_type(const CassCollection* collection) {
  return CassDataType::to(collection->data_type().get());
}

#define CASS_COLLECTION_APPEND(Name, Params, Value)                            \
  CassError cass_collection_append_##Name(CassCollection* collection Params) { \
    return collection->append(Value);                                          \
  }

CASS_COLLECTION_APPEND(null, ZERO_PARAMS_(), CassNull())
CASS_COLLECTION_APPEND(int8, ONE_PARAM_(cass_int8_t value), value)
CASS_COLLECTION_APPEND(int16, ONE_PARAM_(cass_int16_t value), value)
CASS_COLLECTION_APPEND(int32, ONE_PARAM_(cass_int32_t value), value)
CASS_COLLECTION_APPEND(uint32, ONE_PARAM_(cass_uint32_t value), value)
CASS_COLLECTION_APPEND(int64, ONE_PARAM_(cass_int64_t value), value)
CASS_COLLECTION_APPEND(float, ONE_PARAM_(cass_float_t value), value)
CASS_COLLECTION_APPEND(double, ONE_PARAM_(cass_double_t value), value)
CASS_COLLECTION_APPEND(bool, ONE_PARAM_(cass_bool_t value), value)
CASS_COLLECTION_APPEND(uuid, ONE_PARAM_(CassUuid value), value)
CASS_COLLECTION_APPEND(inet, ONE_PARAM_(CassInet value), value)
CASS_COLLECTION_APPEND(collection, ONE_PARAM_(const CassCollection* value), value)
CASS_COLLECTION_APPEND(tuple, ONE_PARAM_(const CassTuple* value), value)
CASS_COLLECTION_APPEND(user_type, ONE_PARAM_(const CassUserType* value), value)
CASS_COLLECTION_APPEND(bytes, TWO_PARAMS_(const cass_byte_t* value, size_t value_size),
                       CassBytes(value, value_size))
CASS_COLLECTION_APPEND(decimal,
                       THREE_PARAMS_(const cass_byte_t* varint, size_t varint_size, int scale),
                       CassDecimal(varint, varint_size, scale))
CASS_COLLECTION_APPEND(duration,
                       THREE_PARAMS_(cass_int32_t months, cass_int32_t days, cass_int64_t nanos),
                       CassDuration(months, days, nanos))

#undef CASS_COLLECTION_APPEND

CassError cass_collection_append_string(CassCollection* collection, const char* value) {
  return collection->append(CassString(value, SAFE_STRLEN(value)));
}

CassError cass_collection_append_string_n(CassCollection* collection, const char* value,
                                          size_t value_length) {
  return collection->append(CassString(value, value_length));
}

CassError cass_collection_append_custom(CassCollection* collection, const char* class_name,
                                        const cass_byte_t* value, size_t value_size) {
  return collection->append(CassCustom(StringRef(class_name), value, value_size));
}

CassError cass_collection_append_custom_n(CassCollection* collection, const char* class_name,
                                          size_t class_name_length, const cass_byte_t* value,
                                          size_t value_size) {
  return collection->append(
      CassCustom(StringRef(class_name, class_name_length), value, value_size));
}

} // extern "C"

CassError Collection::append(CassNull value) {
  CASS_COLLECTION_CHECK_TYPE(value);
  items_.push_back(Buffer());
  return CASS_OK;
}

CassError Collection::append(const Collection* value) {
  CASS_COLLECTION_CHECK_TYPE(value);
  items_.push_back(value->encode());
  return CASS_OK;
}

CassError Collection::append(const Tuple* value) {
  CASS_COLLECTION_CHECK_TYPE(value);
  items_.push_back(value->encode());
  return CASS_OK;
}

CassError Collection::append(const UserTypeValue* value) {
  CASS_COLLECTION_CHECK_TYPE(value);
  items_.push_back(value->encode());
  return CASS_OK;
}

size_t Collection::get_items_size() const {
  size_t size = 0;
  for (BufferVec::const_iterator i = items_.begin(), end = items_.end(); i != end; ++i) {
    size += sizeof(int32_t);
    size += i->size();
  }
  return size;
}

void Collection::encode_items(char* buf) const {
  for (BufferVec::const_iterator i = items_.begin(), end = items_.end(); i != end; ++i) {
    encode_int32(buf, i->size());
    buf += sizeof(int32_t);
    memcpy(buf, i->data(), i->size());
    buf += i->size();
  }
}

size_t Collection::get_size() const { return sizeof(int32_t) + get_items_size(); }

size_t Collection::get_size_with_length() const { return sizeof(int32_t) + get_size(); }

Buffer Collection::encode() const {
  Buffer buf(get_size());
  size_t pos = buf.encode_int32(0, get_count());
  encode_items(buf.data() + pos);
  return buf;
}

Buffer Collection::encode_with_length() const {
  size_t internal_size = get_size();
  Buffer buf(sizeof(int32_t) + internal_size);
  size_t pos = buf.encode_int32(0, internal_size);
  pos = buf.encode_int32(pos, get_count());
  encode_items(buf.data() + pos);
  return buf;
}
