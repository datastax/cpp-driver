/*
  Copyright (c) 2014-2015 DataStax

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
#include "macros.hpp"
#include "external_types.hpp"
#include "utils.hpp"

#include <string.h>

extern "C" {

CassUserType* cass_user_type_new_from_schema(CassSession* session,
                                               const char* keyspace,
                                               const char* type_name) {
  return cass_user_type_new_from_schema_n(session,
                                          keyspace, strlen(keyspace),
                                          type_name, strlen(type_name));
}

CassUserType* cass_user_type_new_from_schema_n(CassSession* session,
                                               const char* keyspace,
                                               size_t keyspace_length,
                                               const char* type_name,
                                               size_t type_name_length) {
  std::string keyspace_id(keyspace, keyspace_length);
  std::string type_name_id(type_name, type_name_length);
  cass::SharedRefPtr<cass::UserType> user_type
      = session->get_user_type(cass::to_cql_id(keyspace_id),
                               cass::to_cql_id(type_name_id));
  if (!user_type) return NULL;
  return CassUserType::to(new cass::UserTypeValue(user_type));
}

CassUserType* cass_user_type_new_from_data_type(CassSession* session,
                                                const CassDataType* data_type) {
  if (!data_type->is_user_type()) {
    return NULL;
  }
  return CassUserType::to(
        new cass::UserTypeValue(
          cass::SharedRefPtr<const cass::DataType>(data_type)));
}

void cass_user_type_free(CassUserType* user_type) {
  delete user_type->from();
}

const CassDataType* cass_user_type_data_type(const CassUserType* user_type) {
  return CassDataType::to(user_type->data_type().get());
}

#define CASS_USER_TYPE_SET(Name, Params, Value)                                \
  CassError cass_user_type_set_##Name(CassUserType* user_type,                 \
                                      size_t index Params) {                   \
    return user_type->set(index, Value);                                       \
  }                                                                            \
  CassError cass_user_type_set_##Name##_by_name(CassUserType* user_type,       \
                                                const char* name Params) {     \
    return user_type->set(cass::StringRef(name), Value);                       \
  }                                                                            \
  CassError cass_user_type_set_##Name##_by_name_n(CassUserType* user_type,     \
                                                  const char* name,            \
                                                  size_t name_length Params) { \
    return user_type->set(cass::StringRef(name, name_length), Value);          \
  }

CASS_USER_TYPE_SET(null, ZERO_PARAMS_(), cass::CassNull())
CASS_USER_TYPE_SET(int32, ONE_PARAM_(cass_int32_t value), value)
CASS_USER_TYPE_SET(int64, ONE_PARAM_(cass_int64_t value), value)
CASS_USER_TYPE_SET(float, ONE_PARAM_(cass_float_t value), value)
CASS_USER_TYPE_SET(double, ONE_PARAM_(cass_double_t value), value)
CASS_USER_TYPE_SET(bool, ONE_PARAM_(cass_bool_t value), value)
CASS_USER_TYPE_SET(inet, ONE_PARAM_(CassInet value), value)
CASS_USER_TYPE_SET(uuid, ONE_PARAM_(CassUuid value), value)
CASS_USER_TYPE_SET(collection, ONE_PARAM_(const CassCollection* value), value)
CASS_USER_TYPE_SET(user_type, ONE_PARAM_(const CassUserType* value), value)
CASS_USER_TYPE_SET(bytes,
                   TWO_PARAMS_(const cass_byte_t* value, size_t value_size),
                   cass::CassBytes(value, value_size))
CASS_USER_TYPE_SET(decimal,
                   THREE_PARAMS_(const cass_byte_t* varint, size_t varint_size, int scale),
                   cass::CassDecimal(varint, varint_size, scale))

#undef CASS_USER_TYPE_SET

CassError cass_user_type_set_string(CassUserType* user_type,
                                    size_t index,
                                    const char* value) {
  return user_type->set(index, cass::CassString(value, strlen(value)));
}

CassError cass_user_type_set_string_n(CassUserType* user_type,
                                      size_t index,
                                      const char* value,
                                      size_t value_length) {
  return user_type->set(index, cass::CassString(value, value_length));
}

CassError cass_user_type_set_string_by_name(CassUserType* user_type,
                                            const char* name,
                                            const char* value) {
  return user_type->set(cass::StringRef(name),
                        cass::CassString(value, strlen(value)));
}

CassError cass_user_type_set_string_by_name_n(CassUserType* user_type,
                                              const char* name,
                                              size_t name_length,
                                              const char* value,
                                              size_t value_length) {
  return user_type->set(cass::StringRef(name, name_length),
                        cass::CassString(value, value_length));
}

} // extern "C"
