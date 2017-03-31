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

#include "collection.hpp"

#include "constants.hpp"
#include "external.hpp"
#include "macros.hpp"
#include "tuple.hpp"
#include "user_type_value.hpp"

#include <string.h>

extern "C" {

CassCollection* cass_collection_new(CassCollectionType type,
                                    size_t item_count) {
  cass::Collection* collection = new cass::Collection(type, item_count);
  collection->inc_ref();
  return CassCollection::to(collection);
}

CassCollection* cass_collection_new_from_data_type(const CassDataType* data_type,
                                                   size_t item_count) {
  if (!data_type->is_collection()) {
    return NULL;
  }
  cass::Collection* collection
      = new cass::Collection(cass::DataType::ConstPtr(data_type),
                             item_count);
  collection->inc_ref();
  return CassCollection::to(collection);
}

void cass_collection_free(CassCollection* collection) {
  collection->dec_ref();
}

const CassDataType* cass_collection_data_type(const CassCollection* collection) {
  return CassDataType::to(collection->data_type().get());
}

#define CASS_COLLECTION_APPEND(Name, Params, Value)                           \
 CassError cass_collection_append_##Name(CassCollection* collection Params) { \
   return collection->append(Value);                                          \
 }

CASS_COLLECTION_APPEND(null, ZERO_PARAMS_(), cass::CassNull())
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
CASS_COLLECTION_APPEND(bytes,
                       TWO_PARAMS_(const cass_byte_t* value, size_t value_size),
                       cass::CassBytes(value, value_size))
CASS_COLLECTION_APPEND(decimal,
                       THREE_PARAMS_(const cass_byte_t* varint, size_t varint_size, int scale),
                       cass::CassDecimal(varint, varint_size, scale))
CASS_COLLECTION_APPEND(duration,
                       THREE_PARAMS_(cass_int32_t months, cass_int32_t days, cass_int64_t nanos),
                       cass::CassDuration(months, days, nanos))

#undef CASS_COLLECTION_APPEND

CassError cass_collection_append_string(CassCollection* collection,
                                        const char* value) {
  return collection->append(cass::CassString(value, strlen(value)));
}

CassError cass_collection_append_string_n(CassCollection* collection,
                                          const char* value,
                                          size_t value_length) {
  return collection->append(cass::CassString(value, value_length));
}

CassError cass_collection_append_custom(CassCollection* collection,
                                        const char* class_name,
                                        const cass_byte_t* value,
                                        size_t value_size) {
  return collection->append(cass::CassCustom(cass::StringRef(class_name),
                                             value, value_size));
}

CassError cass_collection_append_custom_n(CassCollection* collection,
                                          const char* class_name,
                                          size_t class_name_length,
                                          const cass_byte_t* value,
                                          size_t value_size) {
  return collection->append(cass::CassCustom(cass::StringRef(class_name, class_name_length),
                                             value, value_size));
}

} // extern "C"

namespace cass {

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

size_t Collection::get_items_size(int version) const {
  if (version >= 3) {
    return get_items_size(sizeof(int32_t));
  } else {
    return get_items_size(sizeof(uint16_t));
  }
}

void Collection::encode_items(int version, char* buf) const {
  if (version >= 3) {
    encode_items_int32(buf);
  } else {
    encode_items_uint16(buf);
  }
}

size_t Collection::get_size_with_length(int version) const {
  size_t internal_size = sizeof(int32_t);
  if (version >= 3) {
    internal_size += sizeof(int32_t) + get_items_size(sizeof(int32_t));
  } else {
    internal_size += sizeof(uint16_t) + get_items_size(sizeof(uint16_t));
  }
  return internal_size;
}

Buffer Collection::encode() const {
  // Inner types are always encoded using the v3+ (int32_t) encoding
  Buffer buf(sizeof(int32_t) + get_items_size(sizeof(int32_t)));
  size_t pos = buf.encode_int32(0, get_count());
  encode_items_int32(buf.data() + pos);
  return buf;
}

Buffer Collection::encode_with_length(int version) const {
  size_t internal_size;

  if (version >= 3) {
    internal_size = sizeof(int32_t) + get_items_size(sizeof(int32_t));
  } else {
    internal_size = sizeof(uint16_t) + get_items_size(sizeof(uint16_t));
  }

  Buffer buf(sizeof(int32_t) + internal_size);

  size_t pos = buf.encode_int32(0, internal_size);

  if (version >= 3) {
    pos = buf.encode_int32(pos, get_count());
    encode_items_int32(buf.data() + pos);
  } else {
    pos = buf.encode_uint16(pos, get_count());
    encode_items_uint16(buf.data() + pos);
  }

  return buf;
}

size_t Collection::get_items_size(size_t num_bytes_for_size) const {
  size_t size = 0;
  for (BufferVec::const_iterator i = items_.begin(),
       end = items_.end(); i != end; ++i) {
    size += num_bytes_for_size;
    size += i->size();
  }
  return size;
}

void Collection::encode_items_int32(char* buf) const {
  for (BufferVec::const_iterator i = items_.begin(),
       end = items_.end(); i != end; ++i) {
    encode_int32(buf, i->size());
    buf += sizeof(int32_t);
    memcpy(buf, i->data(), i->size());
    buf += i->size();
  }
}

void Collection::encode_items_uint16(char* buf) const {
  for (BufferVec::const_iterator i = items_.begin(),
       end = items_.end(); i != end; ++i) {
    encode_uint16(buf, i->size());
    buf += sizeof(uint16_t);
    memcpy(buf, i->data(), i->size());
    buf += i->size();
  }
}

}  // namespace cass
