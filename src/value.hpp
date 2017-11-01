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

#ifndef __CASS_VALUE_HPP_INCLUDED__
#define __CASS_VALUE_HPP_INCLUDED__

#include "cassandra.h"
#include "external.hpp"
#include "result_metadata.hpp"
#include "string_ref.hpp"

namespace cass {

class Value {
public:
  Value()
      : protocol_version_(0)
      , count_(0)
      , size_(-1) { }

  // Used for "null" values
  Value(const DataType::ConstPtr& data_type)
      : protocol_version_(0)
      , data_type_(data_type)
      , count_(0)
      , size_(-1) { }

  // Used for regular values or collections
  Value(int protocol_version,
        const DataType::ConstPtr& data_type,
        char* data, int32_t size);

  // Used for schema metadata collections (converted from JSON)
  Value(int protocol_version,
        const DataType::ConstPtr& data_type,
        int32_t count, char* data, int32_t size)
      : protocol_version_(protocol_version)
      , data_type_(data_type)
      , count_(count)
      , data_(data)
      , size_(size) { }

  int protocol_version() const { return protocol_version_; }

  CassValueType value_type() const {
    if (!data_type_) {
      return CASS_VALUE_TYPE_UNKNOWN;
    }
    return data_type_->value_type();
  }

  const DataType::ConstPtr& data_type() const {
    return data_type_;
  }

  CassValueType primary_value_type() const {
    const DataType::ConstPtr& primary(primary_data_type());
    if (!primary) {
      return CASS_VALUE_TYPE_UNKNOWN;
    }
    return primary->value_type();
  }

  const DataType::ConstPtr& primary_data_type() const {
    if (!data_type_ || !data_type_->is_collection()) {
      return DataType::NIL;
    }
    const CollectionType::ConstPtr& collection_type(data_type_);
    if (collection_type->types().size() < 1) {
      return DataType::NIL;
    }
    return collection_type->types()[0];
  }

  CassValueType secondary_value_type() const {
    const DataType::ConstPtr& secondary(secondary_data_type());
    if (!secondary) {
      return CASS_VALUE_TYPE_UNKNOWN;
    }
    return secondary->value_type();
  }

  const DataType::ConstPtr& secondary_data_type() const {
    if (!data_type_ || !data_type_->is_map()) {
      return DataType::NIL;
    }
    const CollectionType::ConstPtr& collection_type(data_type_);
    if (collection_type->types().size() < 2) {
      return DataType::NIL;
    }
    return collection_type->types()[1];
  }

  bool is_null() const {
    return size_ < 0;
  }

  bool is_collection() const {
    if (!data_type_) return false;
    return data_type_->is_collection();
  }

  bool is_map() const {
    if (!data_type_) return false;
    return data_type_->is_map();
  }

  bool is_tuple() const {
    if (!data_type_) return false;
    return data_type_->is_tuple();
  }

  bool is_user_type() const {
    if (!data_type_) return false;
    return data_type_->is_user_type();
  }

  int32_t count() const {
    return count_;
  }

  char* data() const { return data_; }
  int32_t size() const { return size_; }

  StringRef to_string_ref() const {
    if (size_ < 0) return StringRef();
    return StringRef(data_, size_);
  }

  std::string to_string() const {
    return to_string_ref().to_string();
  }

  bool as_bool() const;
  int32_t as_int32() const;
  CassUuid as_uuid() const;
  StringVec as_stringlist() const;

private:
  int protocol_version_;
  DataType::ConstPtr data_type_;
  int32_t count_;

  char* data_;
  int32_t size_;
};

typedef std::vector<Value> OutputValueVec;

} // namespace cass

EXTERNAL_TYPE(cass::Value, CassValue)

#endif
