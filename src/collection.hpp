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

#ifndef DATASTAX_INTERNAL_COLLECTION_HPP
#define DATASTAX_INTERNAL_COLLECTION_HPP

#include "buffer.hpp"
#include "cassandra.h"
#include "data_type.hpp"
#include "encode.hpp"
#include "external.hpp"
#include "ref_counted.hpp"
#include "types.hpp"

#define CASS_COLLECTION_CHECK_TYPE(Value) \
  do {                                    \
    CassError rc = check(Value);          \
    if (rc != CASS_OK) return rc;         \
  } while (0)

namespace datastax { namespace internal { namespace core {

class UserTypeValue;

class Collection : public RefCounted<Collection> {
public:
  Collection(CassCollectionType type, size_t item_count)
      : data_type_(new CollectionType(static_cast<CassValueType>(type), false)) {
    items_.reserve(item_count);
  }

  Collection(const CollectionType::ConstPtr& data_type, size_t item_count)
      : data_type_(data_type) {
    items_.reserve(item_count);
  }

  CassCollectionType type() const {
    return static_cast<CassCollectionType>(data_type_->value_type());
  }

  const CollectionType::ConstPtr& data_type() const { return data_type_; }
  const BufferVec& items() const { return items_; }

#define APPEND_TYPE(Type)                  \
  CassError append(const Type value) {     \
    CASS_COLLECTION_CHECK_TYPE(value);     \
    items_.push_back(core::encode(value)); \
    return CASS_OK;                        \
  }

  APPEND_TYPE(cass_int8_t)
  APPEND_TYPE(cass_int16_t)
  APPEND_TYPE(cass_int32_t)
  APPEND_TYPE(cass_uint32_t)
  APPEND_TYPE(cass_int64_t)
  APPEND_TYPE(cass_float_t)
  APPEND_TYPE(cass_double_t)
  APPEND_TYPE(cass_bool_t)
  APPEND_TYPE(CassString)
  APPEND_TYPE(CassBytes)
  APPEND_TYPE(CassCustom)
  APPEND_TYPE(CassUuid)
  APPEND_TYPE(CassInet)
  APPEND_TYPE(CassDecimal)
  APPEND_TYPE(CassDuration)

#undef APPEND_TYPE

  CassError append(CassNull value);
  CassError append(const Collection* value);
  CassError append(const Tuple* value);
  CassError append(const UserTypeValue* value);

  size_t get_items_size() const;
  void encode_items(char* buf) const;

  size_t get_size() const;
  size_t get_size_with_length() const;

  Buffer encode() const;
  Buffer encode_with_length() const;

  void clear() { items_.clear(); }

private:
  template <class T>
  CassError check(const T value) {
    IsValidDataType<T> is_valid_type;
    size_t index = items_.size();

    switch (type()) {
      case CASS_COLLECTION_TYPE_MAP:
        if (data_type_->types().size() == 2 &&
            !is_valid_type(value, data_type_->types()[index % 2])) {
          return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
        }
        break;

      case CASS_COLLECTION_TYPE_LIST:
      case CASS_COLLECTION_TYPE_SET:
        if (data_type_->types().size() == 1 && !is_valid_type(value, data_type_->types()[0])) {
          return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
        }
        break;
    }
    return CASS_OK;
  }

  int32_t get_count() const {
    return ((type() == CASS_COLLECTION_TYPE_MAP) ? items_.size() / 2 : items_.size());
  }

private:
  CollectionType::ConstPtr data_type_;
  BufferVec items_;

private:
  DISALLOW_COPY_AND_ASSIGN(Collection);
};

}}} // namespace datastax::internal::core

EXTERNAL_TYPE(datastax::internal::core::Collection, CassCollection)

#endif
