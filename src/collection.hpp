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

#ifndef __CASS_COLLECTION_HPP_INCLUDED__
#define __CASS_COLLECTION_HPP_INCLUDED__

#include "cassandra.h"
#include "data_type.hpp"
#include "encode.hpp"
#include "buffer.hpp"
#include "ref_counted.hpp"
#include "types.hpp"

#define CASS_COLLECTION_CHECK_TYPE(Value) do { \
  CassError rc = check(Value);                 \
  if (rc != CASS_OK) return rc;                \
} while(0)

namespace cass {

class Collection : public RefCounted<Collection> {
public:
  Collection(CassCollectionType type,
             size_t item_count)
    : type_(type)
    , data_type_(new CollectionType(static_cast<CassValueType>(type))) {
    items_.reserve(item_count);
  }

  CassCollectionType type() const { return type_; }
  const SharedRefPtr<CollectionType>& data_type() const { return data_type_; }
  const BufferVec& items() const { return items_; }

#define APPEND_TYPE(Type)                  \
  CassError append(const Type value) {     \
    CASS_COLLECTION_CHECK_TYPE(value);     \
    items_.push_back(cass::encode(value)); \
    return CASS_OK;                        \
  }

  APPEND_TYPE(cass_int32_t)
  APPEND_TYPE(cass_int64_t)
  APPEND_TYPE(cass_float_t)
  APPEND_TYPE(cass_double_t)
  APPEND_TYPE(cass_bool_t)
  APPEND_TYPE(CassString)
  APPEND_TYPE(CassBytes)
  APPEND_TYPE(CassUuid)
  APPEND_TYPE(CassInet)
  APPEND_TYPE(CassDecimal)

#undef APPEND_TYPE

  CassError append(const Collection* value);

  size_t get_items_size(int version) const;
  void encode_items(int version, char* buf) const;

  Buffer encode() const;
  Buffer encode_with_length(int version) const;

  void clear() {
    items_.clear();
    data_type_->types().clear();
  }

private:
  template <class T>
  CassError check(const T value) {
    CreateDataType<T> create_data_type;
    if (type_ == CASS_COLLECTION_TYPE_TUPLE) {
      data_type_->types().push_back(create_data_type(value));
    } else {
      IsValidDataType<T> is_valid_type;
      if (type_ == CASS_COLLECTION_TYPE_MAP) {
        if (data_type_->types().size() == 2) {
          if (!is_valid_type(value, data_type_->types()[items_.size() % 2])) {
            return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
          }
        } else {
          data_type_->types().push_back(create_data_type(value));
        }
      } else {
        if (data_type_->types().size() == 1) {
          if (!is_valid_type(value, data_type_->types()[0])) {
            return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
          }
        } else {
          data_type_->types().push_back(create_data_type(value));
        }
      }
    }
    return CASS_OK;
  }

  int32_t get_count() const {
    return ((type_ == CASS_COLLECTION_TYPE_MAP) ? items_.size() / 2 : items_.size());
  }

  size_t get_items_size(size_t num_bytes_for_size) const;

  void encode_items_int32(char* buf) const;
  void encode_items_uint16(char* buf) const;

private:
  CassCollectionType type_;
  SharedRefPtr<CollectionType> data_type_;
  BufferVec items_;

private:
  DISALLOW_COPY_AND_ASSIGN(Collection);
};

} // namespace cass

#endif

