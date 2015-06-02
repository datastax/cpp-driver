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

#include "data_type.hpp"

#include "collection.hpp"
#include "external_types.hpp"
#include "types.hpp"
#include "user_type_value.hpp"

#include <string.h>

extern "C" {

CassDataType* cass_data_type_new(CassSession* session,
                                 CassValueType type) {
  cass::DataType* data_type = NULL;
  switch (type) {
    case CASS_VALUE_TYPE_LIST:
    case CASS_VALUE_TYPE_SET:
    case CASS_VALUE_TYPE_TUPLE:
    case CASS_VALUE_TYPE_MAP:
      data_type = new cass::CollectionType(type);
      data_type->inc_ref();
      break;

    case CASS_VALUE_TYPE_UDT:
      data_type = new cass::UserType();
      data_type->inc_ref();
      break;

    case CASS_VALUE_TYPE_CUSTOM:
      data_type = new cass::CustomType();
      data_type->inc_ref();
      break;

    case CASS_VALUE_TYPE_UNKNOWN:
      // Invalid
      break;

    default:
      if (type < CASS_VALUE_TYPE_LAST_ENTRY) {
        data_type = new cass::DataType(type);
        data_type->inc_ref();
      }
      break;
  }
  return CassDataType::to(data_type);
}

CassDataType* cass_data_type_new_from_existing(CassSession* session,
                                               const CassDataType* data_type) {
  cass::DataType* copy = data_type->copy();
  copy->inc_ref();
  return CassDataType::to(copy);
}

CassDataType* cass_data_type_new_tuple(CassSession* session,
                                       size_t item_count) {
  cass::DataType* data_type
      = new cass::CollectionType(CASS_VALUE_TYPE_TUPLE, item_count);
  data_type->inc_ref();
  return CassDataType::to(data_type);
}

CassDataType* cass_data_type_new_udt(CassSession* session,
                                     const char* keyspace,
                                     const char* type_name,
                                     size_t field_count) {
  return cass_data_type_new_udt_n(session,
                                  keyspace, strlen(keyspace),
                                  type_name, strlen(type_name),
                                  field_count);
}

CassDataType* cass_data_type_new_udt_n(CassSession* session,
                                       const char* keyspace,
                                       size_t keyspace_length,
                                       const char* type_name,
                                       size_t type_name_length,
                                       size_t field_count) {
  std::string keyspace_id(keyspace, keyspace_length);
  std::string type_name_id(type_name, type_name_length);
  cass::UserType* user_type = new cass::UserType(cass::to_cql_id(keyspace_id),
                                                 cass::to_cql_id((type_name_id)),
                                                 field_count);
  user_type->inc_ref();
  return CassDataType::to(user_type);
}

const CassDataType* cass_data_type_sub_data_type(const CassDataType* data_type,
                                                     size_t index) {
  const cass::DataType* sub_type = NULL;
  if (sub_type->is_collection()) {
    const cass::CollectionType* collection_type
        = static_cast<const cass::CollectionType*>(data_type->from());
    if (index < collection_type->types().size()) {
      sub_type = collection_type->types()[index].get();
    }
  } else if (sub_type->is_user_type()) {
    const cass::UserType* user_type
        = static_cast<const cass::UserType*>(data_type->from());
    if (index < user_type->fields().size()) {
      sub_type = user_type->fields()[index].type.get();
    }
  }
  return CassDataType::to(sub_type);
}

const CassDataType* cass_data_type_sub_data_type_by_name(const CassDataType* data_type,
                                                             const char* name) {
  return cass_data_type_sub_data_type_by_name_n(data_type,
                                                name, strlen(name));
}

const CassDataType* cass_data_type_sub_data_type_by_name_n(const CassDataType* data_type,
                                                           const char* name,
                                                           size_t name_length) {
  if (!data_type->is_user_type()) {
    return NULL;
  }

  const cass::UserType* user_type
      = static_cast<const cass::UserType*>(data_type->from());

  cass::HashIndex::IndexVec indices;
  if (user_type->get_indices(cass::StringRef(name, name_length), &indices) == 0) {
    return NULL;
  }

  return CassDataType::to(user_type->fields()[indices.front()].type.get());
}

CassValueType cass_data_type_type(const CassDataType* data_type) {
  return data_type->value_type();
}

CassError cass_data_type_type_name(const CassDataType* data_type,
                              const char** name,
                              size_t* name_length) {
  if (!data_type->is_user_type()) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }

  const cass::UserType* user_type
      = static_cast<const cass::UserType*>(data_type->from());

  *name = user_type->type_name().data();
  *name_length = user_type->type_name().size();

  return CASS_OK;
}

CassError cass_data_type_set_type_name(CassDataType* data_type,
                                       const char* type_name) {
  return cass_data_type_set_type_name_n(data_type,
                                        type_name, strlen(type_name));
}

CassError cass_data_type_set_type_name_n(CassDataType* data_type,
                                         const char* type_name,
                                         size_t type_name_length) {
  if (!data_type->is_user_type()) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }

  cass::UserType* user_type
      = static_cast<cass::UserType*>(data_type->from());

  user_type->set_type_name(std::string(type_name, type_name_length));

  return CASS_OK;
}

CassError cass_data_type_keyspace(const CassDataType* data_type,
                                  const char** keyspace,
                                  size_t* keyspace_length) {
  if (!data_type->is_user_type()) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }

  const cass::UserType* user_type
      = static_cast<const cass::UserType*>(data_type->from());

  *keyspace = user_type->keyspace().data();
  *keyspace_length = user_type->keyspace().size();

  return CASS_OK;
}

CassError cass_data_type_set_keyspace(CassDataType* data_type,
                                        const char* keyspace) {
  return cass_data_type_set_keyspace_n(data_type,
                                       keyspace, strlen(keyspace));
}

CassError cass_data_type_set_keyspace_n(CassDataType* data_type,
                                        const char* keyspace,
                                        size_t keyspace_length) {
  if (!data_type->is_user_type()) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }

  cass::UserType* user_type
      = static_cast<cass::UserType*>(data_type->from());

  user_type->set_keyspace(std::string(keyspace, keyspace_length));

  return CASS_OK;
}

CassError cass_data_type_class_name(CassDataType* data_type,
                                    const char** class_name,
                                    size_t* class_name_length) {
  if (!data_type->is_custom()) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }

  cass::CustomType* custom_type
      = static_cast<cass::CustomType*>(data_type->from());

  *class_name = custom_type->class_name().data();
  *class_name_length = custom_type->class_name().size();

  return CASS_OK;
}

CassError cass_data_type_set_class_name(CassDataType* data_type,
                                        const char* class_name) {
  return cass_data_type_set_class_name_n(data_type,
                                         class_name, strlen(class_name));
}

CassError cass_data_type_set_class_name_n(CassDataType* data_type,
                                          const char* class_name,
                                          size_t class_name_length) {
  if (!data_type->is_custom()) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }

  cass::CustomType* custom_type
      = static_cast<cass::CustomType*>(data_type->from());

  custom_type->set_class_name(std::string(class_name, class_name_length));

  return CASS_OK;
}

CassError cass_data_type_sub_type_name(const CassDataType* data_type,
                                       size_t index,
                                       const char** name,
                                       size_t* name_length) {
  if (!data_type->is_user_type()) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }

  const cass::UserType* user_type
      = static_cast<const cass::UserType*>(data_type->from());

  if (index >= user_type->fields().size()) {
    return CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;
  }

  cass::StringRef field_name = user_type->fields()[index].name;

  *name = field_name.data();
  *name_length = field_name.size();

  return CASS_OK;
}

CassError cass_data_type_add_sub_type(CassDataType* data_type,
                                      const CassDataType* type) {
  if (!data_type->is_collection()) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }

  cass::CollectionType* collection_type
      = static_cast<cass::CollectionType*>(data_type->from());

  switch (collection_type->value_type()) {
    case CASS_VALUE_TYPE_LIST:
    case CASS_VALUE_TYPE_SET:
      if (collection_type->types().size() >= 1) {
        return CASS_ERROR_LIB_BAD_PARAMS;
      }
      collection_type->types().push_back(cass::SharedRefPtr<const cass::DataType>(type));
      break;

    case CASS_VALUE_TYPE_MAP:
      if (collection_type->types().size() >= 2) {
        return CASS_ERROR_LIB_BAD_PARAMS;
      }
      collection_type->types().push_back(cass::SharedRefPtr<const cass::DataType>(type));
      break;

    case CASS_VALUE_TYPE_TUPLE:
      collection_type->types().push_back(cass::SharedRefPtr<const cass::DataType>(type));
      break;

    default:
      assert(false);
      break;
  }

  return CASS_OK;
}

CassError cass_data_type_add_named_sub_type(CassDataType* data_type,
                                              const char* name,
                                              const CassDataType* type) {
  return cass_data_type_add_named_sub_type_n(data_type,
                                                 name, strlen(name),
                                                 type);
}

CassError cass_data_type_add_named_sub_type_n(CassDataType* data_type,
                                                const char* name,
                                                size_t name_length,
                                                const CassDataType* type) {
  if (!data_type->is_user_type()) {
    return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }

  cass::UserType* user_type
      = static_cast<cass::UserType*>(data_type->from());

  user_type->add_field(cass::StringRef(name, name_length),
                       cass::SharedRefPtr<const cass::DataType>(type));

  return CASS_OK;

}

void cass_data_type_free(CassDataType* data_type) {
  data_type->dec_ref();
}

} // extern "C"

namespace cass {

const SharedRefPtr<const DataType> DataType::NIL;

bool cass::IsValidDataType<const Collection*>::operator()(const Collection* value,
                                                          const SharedRefPtr<const DataType>& data_type) const {
  return value->data_type()->equals(data_type);
}

bool cass::IsValidDataType<const UserTypeValue*>::operator()(const UserTypeValue* value,
                                                             const SharedRefPtr<const DataType>& data_type) const {
  return value->data_type()->equals(data_type);
}

} // namespace cass
