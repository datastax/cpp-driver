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

#include "data_type.hpp"

#include "collection.hpp"
#include "external.hpp"
#include "tuple.hpp"
#include "types.hpp"
#include "user_type_value.hpp"
#include "utils.hpp"

#include <string.h>

using namespace datastax;
using namespace datastax::internal::core;

extern "C" {

CassDataType* cass_data_type_new(CassValueType type) {
  DataType* data_type = NULL;
  switch (type) {
    case CASS_VALUE_TYPE_LIST:
    case CASS_VALUE_TYPE_SET:
    case CASS_VALUE_TYPE_TUPLE:
    case CASS_VALUE_TYPE_MAP:
      data_type = new CollectionType(type, false);
      data_type->inc_ref();
      break;

    case CASS_VALUE_TYPE_UDT:
      data_type = new UserType(false);
      data_type->inc_ref();
      break;

    case CASS_VALUE_TYPE_CUSTOM:
      data_type = new CustomType();
      data_type->inc_ref();
      break;

    case CASS_VALUE_TYPE_UNKNOWN:
      // Invalid
      break;

    default:
      if (type < CASS_VALUE_TYPE_LAST_ENTRY) {
        data_type = new DataType(type);
        data_type->inc_ref();
      }
      break;
  }
  return CassDataType::to(data_type);
}

CassDataType* cass_data_type_new_from_existing(const CassDataType* data_type) {
  DataType::Ptr copy = data_type->copy();
  copy->inc_ref();
  return CassDataType::to(copy.get());
}

CassDataType* cass_data_type_new_tuple(size_t item_count) {
  DataType* data_type = new CollectionType(CASS_VALUE_TYPE_TUPLE, item_count, false);
  data_type->inc_ref();
  return CassDataType::to(data_type);
}

CassDataType* cass_data_type_new_udt(size_t field_count) {
  DataType* data_type = new UserType(field_count, false);
  data_type->inc_ref();
  return CassDataType::to(data_type);
}

const CassDataType* cass_data_type_sub_data_type(const CassDataType* data_type, size_t index) {
  const DataType* sub_type = NULL;
  if (data_type->is_collection() || data_type->is_tuple()) {
    const CompositeType* composite_type = static_cast<const CompositeType*>(data_type->from());
    if (index < composite_type->types().size()) {
      sub_type = composite_type->types()[index].get();
    }
  } else if (data_type->is_user_type()) {
    const UserType* user_type = static_cast<const UserType*>(data_type->from());
    if (index < user_type->fields().size()) {
      sub_type = user_type->fields()[index].type.get();
    }
  }
  return CassDataType::to(sub_type);
}

const CassDataType* cass_data_type_sub_data_type_by_name(const CassDataType* data_type,
                                                         const char* name) {
  return cass_data_type_sub_data_type_by_name_n(data_type, name, SAFE_STRLEN(name));
}

const CassDataType* cass_data_type_sub_data_type_by_name_n(const CassDataType* data_type,
                                                           const char* name, size_t name_length) {
  if (!data_type->is_user_type()) {
    return NULL;
  }

  const UserType* user_type = static_cast<const UserType*>(data_type->from());

  IndexVec indices;
  if (user_type->get_indices(StringRef(name, name_length), &indices) == 0) {
    return NULL;
  }

  return CassDataType::to(user_type->fields()[indices.front()].type.get());
}

CassValueType cass_data_type_type(const CassDataType* data_type) { return data_type->value_type(); }

cass_bool_t cass_data_type_is_frozen(const CassDataType* data_type) {
  return data_type->is_frozen() ? cass_true : cass_false;
}

CassError cass_data_type_type_name(const CassDataType* data_type, const char** name,
                                   size_t* name_length) {
  if (!data_type->is_user_type()) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }

  const UserType* user_type = static_cast<const UserType*>(data_type->from());

  *name = user_type->type_name().data();
  *name_length = user_type->type_name().size();

  return CASS_OK;
}

CassError cass_data_type_set_type_name(CassDataType* data_type, const char* type_name) {
  return cass_data_type_set_type_name_n(data_type, type_name, SAFE_STRLEN(type_name));
}

CassError cass_data_type_set_type_name_n(CassDataType* data_type, const char* type_name,
                                         size_t type_name_length) {
  if (!data_type->is_user_type()) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }

  UserType* user_type = static_cast<UserType*>(data_type->from());

  user_type->set_type_name(String(type_name, type_name_length));

  return CASS_OK;
}

CassError cass_data_type_keyspace(const CassDataType* data_type, const char** keyspace,
                                  size_t* keyspace_length) {
  if (!data_type->is_user_type()) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }

  const UserType* user_type = static_cast<const UserType*>(data_type->from());

  *keyspace = user_type->keyspace().data();
  *keyspace_length = user_type->keyspace().size();

  return CASS_OK;
}

CassError cass_data_type_set_keyspace(CassDataType* data_type, const char* keyspace) {
  return cass_data_type_set_keyspace_n(data_type, keyspace, SAFE_STRLEN(keyspace));
}

CassError cass_data_type_set_keyspace_n(CassDataType* data_type, const char* keyspace,
                                        size_t keyspace_length) {
  if (!data_type->is_user_type()) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }

  UserType* user_type = static_cast<UserType*>(data_type->from());

  user_type->set_keyspace(String(keyspace, keyspace_length));

  return CASS_OK;
}

CassError cass_data_type_class_name(const CassDataType* data_type, const char** class_name,
                                    size_t* class_name_length) {
  if (!data_type->is_custom()) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }

  const CustomType* custom_type = static_cast<const CustomType*>(data_type->from());

  *class_name = custom_type->class_name().data();
  *class_name_length = custom_type->class_name().size();

  return CASS_OK;
}

CassError cass_data_type_set_class_name(CassDataType* data_type, const char* class_name) {
  return cass_data_type_set_class_name_n(data_type, class_name, SAFE_STRLEN(class_name));
}

CassError cass_data_type_set_class_name_n(CassDataType* data_type, const char* class_name,
                                          size_t class_name_length) {
  if (!data_type->is_custom()) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }

  CustomType* custom_type = static_cast<CustomType*>(data_type->from());

  custom_type->set_class_name(String(class_name, class_name_length));

  return CASS_OK;
}

size_t cass_data_sub_type_count(const CassDataType* data_type) {
  return cass_data_type_sub_type_count(data_type);
}

size_t cass_data_type_sub_type_count(const CassDataType* data_type) {
  if (data_type->is_collection() || data_type->is_tuple()) {
    const CompositeType* composite_type = static_cast<const CompositeType*>(data_type->from());
    return composite_type->types().size();
  } else if (data_type->is_user_type()) {
    const UserType* user_type = static_cast<const UserType*>(data_type->from());
    return user_type->fields().size();
  }
  return 0;
}

CassError cass_data_type_sub_type_name(const CassDataType* data_type, size_t index,
                                       const char** name, size_t* name_length) {
  if (!data_type->is_user_type()) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }

  const UserType* user_type = static_cast<const UserType*>(data_type->from());

  if (index >= user_type->fields().size()) {
    return CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;
  }

  StringRef field_name = user_type->fields()[index].name;

  *name = field_name.data();
  *name_length = field_name.size();

  return CASS_OK;
}

CassError cass_data_type_add_sub_type(CassDataType* data_type, const CassDataType* sub_data_type) {
  if (!data_type->is_collection() && !data_type->is_tuple()) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }

  CompositeType* composite_type = static_cast<CompositeType*>(data_type->from());

  switch (composite_type->value_type()) {
    case CASS_VALUE_TYPE_LIST:
    case CASS_VALUE_TYPE_SET:
      if (composite_type->types().size() >= 1) {
        return CASS_ERROR_LIB_BAD_PARAMS;
      }
      composite_type->types().push_back(DataType::ConstPtr(sub_data_type));
      break;

    case CASS_VALUE_TYPE_MAP:
      if (composite_type->types().size() >= 2) {
        return CASS_ERROR_LIB_BAD_PARAMS;
      }
      composite_type->types().push_back(DataType::ConstPtr(sub_data_type));
      break;

    case CASS_VALUE_TYPE_TUPLE:
      composite_type->types().push_back(DataType::ConstPtr(sub_data_type));
      break;

    default:
      assert(false);
      break;
  }

  return CASS_OK;
}

CassError cass_data_type_add_sub_type_by_name(CassDataType* data_type, const char* name,
                                              const CassDataType* sub_data_type) {
  return cass_data_type_add_sub_type_by_name_n(data_type, name, SAFE_STRLEN(name), sub_data_type);
}

CassError cass_data_type_add_sub_type_by_name_n(CassDataType* data_type, const char* name,
                                                size_t name_length,
                                                const CassDataType* sub_data_type) {
  if (!data_type->is_user_type()) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }

  UserType* user_type = static_cast<UserType*>(data_type->from());

  user_type->add_field(String(name, name_length), DataType::ConstPtr(sub_data_type));

  return CASS_OK;
}

CassError cass_data_type_add_sub_value_type(CassDataType* data_type, CassValueType sub_value_type) {
  DataType::ConstPtr sub_data_type(new DataType(sub_value_type));
  return cass_data_type_add_sub_type(data_type, CassDataType::to(sub_data_type.get()));
}

CassError cass_data_type_add_sub_value_type_by_name(CassDataType* data_type, const char* name,
                                                    CassValueType sub_value_type) {
  DataType::ConstPtr sub_data_type(new DataType(sub_value_type));
  return cass_data_type_add_sub_type_by_name(data_type, name,
                                             CassDataType::to(sub_data_type.get()));
}

CassError cass_data_type_add_sub_value_type_by_name_n(CassDataType* data_type, const char* name,
                                                      size_t name_length,
                                                      CassValueType sub_value_type) {
  DataType::ConstPtr sub_data_type(new DataType(sub_value_type));
  return cass_data_type_add_sub_type_by_name_n(data_type, name, name_length,
                                               CassDataType::to(sub_data_type.get()));
}

void cass_data_type_free(CassDataType* data_type) { data_type->dec_ref(); }

} // extern "C"

const DataType::ConstPtr DataType::NIL;

DataType::ConstPtr DataType::create_by_class(StringRef name) {
  CassValueType value_type = ValueTypes::by_class(name);
  if (value_type == CASS_VALUE_TYPE_UNKNOWN) {
    return DataType::NIL;
  }
  return ConstPtr(new DataType(value_type));
}

DataType::ConstPtr DataType::create_by_cql(StringRef name) {
  CassValueType value_type = ValueTypes::by_cql(name);
  if (value_type == CASS_VALUE_TYPE_UNKNOWN) {
    return DataType::NIL;
  }
  return ConstPtr(new DataType(value_type));
}

ValueTypes::HashMap ValueTypes::value_types_by_class_;
ValueTypes::HashMap ValueTypes::value_types_by_cql_;

static ValueTypes __value_types__; // Initializer

ValueTypes::ValueTypes() {
  value_types_by_class_.set_empty_key("");
  value_types_by_cql_.set_empty_key("");

#define XX_VALUE_TYPE(name, type, cql, klass)                     \
  if (sizeof(klass) - 1 > 0) value_types_by_class_[klass] = name; \
  if (sizeof(cql) - 1 > 0) value_types_by_cql_[cql] = name;

  CASS_VALUE_TYPE_MAPPING(XX_VALUE_TYPE)
#undef XX_VALUE_TYPE
}

CassValueType ValueTypes::by_class(StringRef name) {
  HashMap::const_iterator i = value_types_by_class_.find(name);
  if (i == value_types_by_class_.end()) {
    return CASS_VALUE_TYPE_UNKNOWN;
  }
  return i->second;
}

CassValueType ValueTypes::by_cql(StringRef name) {
  HashMap::const_iterator i = value_types_by_cql_.find(name);
  if (i == value_types_by_cql_.end()) {
    return CASS_VALUE_TYPE_UNKNOWN;
  }
  return i->second;
}

const DataType::ConstPtr& SimpleDataTypeCache::by_value_type(uint16_t value_type) {
  if (value_type == CASS_VALUE_TYPE_UNKNOWN || value_type == CASS_VALUE_TYPE_CUSTOM ||
      value_type == CASS_VALUE_TYPE_LIST || value_type == CASS_VALUE_TYPE_MAP ||
      value_type == CASS_VALUE_TYPE_SET || value_type == CASS_VALUE_TYPE_UDT ||
      value_type == CASS_VALUE_TYPE_TUPLE || value_type >= CASS_VALUE_TYPE_LAST_ENTRY) {
    return DataType::NIL;
  }
  DataType::ConstPtr& data_type = cache_[value_type];
  if (!data_type) {
    data_type = DataType::ConstPtr(new DataType(static_cast<CassValueType>(value_type)));
  }
  return data_type;
}

bool IsValidDataType<const Collection*>::operator()(const Collection* value,
                                                    const DataType::ConstPtr& data_type) const {
  return value->data_type()->equals(data_type);
}

bool IsValidDataType<const Tuple*>::operator()(const Tuple* value,
                                               const DataType::ConstPtr& data_type) const {
  return value->data_type()->equals(data_type);
}

bool IsValidDataType<const UserTypeValue*>::operator()(const UserTypeValue* value,
                                                       const DataType::ConstPtr& data_type) const {
  return value->data_type()->equals(data_type);
}
