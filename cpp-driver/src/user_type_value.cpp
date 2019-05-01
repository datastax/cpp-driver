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

#include "user_type_value.hpp"

#include "collection.hpp"
#include "external.hpp"
#include "macros.hpp"
#include "tuple.hpp"
#include "utils.hpp"

#include <string.h>

using namespace datastax;
using namespace datastax::internal;
using namespace datastax::internal::core;

extern "C" {

CassUserType* cass_user_type_new_from_data_type(const CassDataType* data_type) {
  if (!data_type->is_user_type()) {
    return NULL;
  }
  return CassUserType::to(new UserTypeValue(DataType::ConstPtr(data_type)));
}

void cass_user_type_free(CassUserType* user_type) { delete user_type->from(); }

const CassDataType* cass_user_type_data_type(const CassUserType* user_type) {
  return CassDataType::to(user_type->data_type().get());
}

#define CASS_USER_TYPE_SET(Name, Params, Value)                                              \
  CassError cass_user_type_set_##Name(CassUserType* user_type, size_t index Params) {        \
    return user_type->set(index, Value);                                                     \
  }                                                                                          \
  CassError cass_user_type_set_##Name##_by_name(CassUserType* user_type,                     \
                                                const char* name Params) {                   \
    return user_type->set(StringRef(name), Value);                                           \
  }                                                                                          \
  CassError cass_user_type_set_##Name##_by_name_n(CassUserType* user_type, const char* name, \
                                                  size_t name_length Params) {               \
    return user_type->set(StringRef(name, name_length), Value);                              \
  }

CASS_USER_TYPE_SET(null, ZERO_PARAMS_(), CassNull())
CASS_USER_TYPE_SET(int8, ONE_PARAM_(cass_int8_t value), value)
CASS_USER_TYPE_SET(int16, ONE_PARAM_(cass_int16_t value), value)
CASS_USER_TYPE_SET(int32, ONE_PARAM_(cass_int32_t value), value)
CASS_USER_TYPE_SET(uint32, ONE_PARAM_(cass_uint32_t value), value)
CASS_USER_TYPE_SET(int64, ONE_PARAM_(cass_int64_t value), value)
CASS_USER_TYPE_SET(float, ONE_PARAM_(cass_float_t value), value)
CASS_USER_TYPE_SET(double, ONE_PARAM_(cass_double_t value), value)
CASS_USER_TYPE_SET(bool, ONE_PARAM_(cass_bool_t value), value)
CASS_USER_TYPE_SET(inet, ONE_PARAM_(CassInet value), value)
CASS_USER_TYPE_SET(uuid, ONE_PARAM_(CassUuid value), value)
CASS_USER_TYPE_SET(collection, ONE_PARAM_(const CassCollection* value), value)
CASS_USER_TYPE_SET(tuple, ONE_PARAM_(const CassTuple* value), value)
CASS_USER_TYPE_SET(user_type, ONE_PARAM_(const CassUserType* value), value)
CASS_USER_TYPE_SET(bytes, TWO_PARAMS_(const cass_byte_t* value, size_t value_size),
                   CassBytes(value, value_size))
CASS_USER_TYPE_SET(decimal, THREE_PARAMS_(const cass_byte_t* varint, size_t varint_size, int scale),
                   CassDecimal(varint, varint_size, scale))
CASS_USER_TYPE_SET(duration,
                   THREE_PARAMS_(cass_int32_t months, cass_int32_t days, cass_int64_t nanos),
                   CassDuration(months, days, nanos))

#undef CASS_USER_TYPE_SET

CassError cass_user_type_set_string(CassUserType* user_type, size_t index, const char* value) {
  return user_type->set(index, CassString(value, SAFE_STRLEN(value)));
}

CassError cass_user_type_set_string_n(CassUserType* user_type, size_t index, const char* value,
                                      size_t value_length) {
  return user_type->set(index, CassString(value, value_length));
}

CassError cass_user_type_set_string_by_name(CassUserType* user_type, const char* name,
                                            const char* value) {
  return user_type->set(StringRef(name), CassString(value, SAFE_STRLEN(value)));
}

CassError cass_user_type_set_string_by_name_n(CassUserType* user_type, const char* name,
                                              size_t name_length, const char* value,
                                              size_t value_length) {
  return user_type->set(StringRef(name, name_length), CassString(value, value_length));
}

CassError cass_user_type_set_custom(CassUserType* user_type, size_t index, const char* class_name,
                                    const cass_byte_t* value, size_t value_size) {
  return user_type->set(index, CassCustom(StringRef(class_name), value, value_size));
}

CassError cass_user_type_set_custom_n(CassUserType* user_type, size_t index, const char* class_name,
                                      size_t class_name_length, const cass_byte_t* value,
                                      size_t value_size) {
  return user_type->set(index,
                        CassCustom(StringRef(class_name, class_name_length), value, value_size));
}

CassError cass_user_type_set_custom_by_name(CassUserType* user_type, const char* name,
                                            const char* class_name, const cass_byte_t* value,
                                            size_t value_size) {
  return user_type->set(StringRef(name), CassCustom(StringRef(class_name), value, value_size));
}

CassError cass_user_type_set_custom_by_name_n(CassUserType* user_type, const char* name,
                                              size_t name_length, const char* class_name,
                                              size_t class_name_length, const cass_byte_t* value,
                                              size_t value_size) {
  return user_type->set(StringRef(name, name_length),
                        CassCustom(StringRef(class_name, class_name_length), value, value_size));
}

} // extern "C"
