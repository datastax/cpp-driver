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

#ifndef __CASS_VALUE_HPP_INCLUDED__
#define __CASS_VALUE_HPP_INCLUDED__

#include "cassandra.h"
#include "result_metadata.hpp"
#include "string_ref.hpp"

namespace cass {

class Value {
public:
  Value()
      : protocol_version_(0)
      , count_(0)
      , size_(-1) { }

  Value(int protcol_version,
        const SharedRefPtr<DataType>& data_type,
        char* data, int32_t size)
      : protocol_version_(protcol_version)
      , data_type_(data_type)
      , count_(0)
      , data_(data)
      , size_(size) { }

  Value(int protcol_version,
        const SharedRefPtr<DataType>& data_type,
        int32_t count, char* data, int32_t size)
      : protocol_version_(protcol_version)
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

  const SharedRefPtr<DataType>& data_type() const {
    return data_type_;
  }

  CassValueType primary_value_type() const {
    SharedRefPtr<DataType> primary(primary_data_type());
    if (!primary) {
      return CASS_VALUE_TYPE_UNKNOWN;
    }
    return primary->value_type();
  }

  SharedRefPtr<DataType> primary_data_type() const {
    if (!data_type_ || !data_type_->is_collection()) {
      return SharedRefPtr<DataType>();
    }
    assert(static_cast<const SharedRefPtr<CollectionType>&>(data_type_)->types().size() > 0);
    return static_cast<const SharedRefPtr<CollectionType>&>(data_type_)->types()[0];
  }

  CassValueType secondary_value_type() const {
    SharedRefPtr<DataType> secondary(secondary_data_type());
    if (!secondary) {
      return CASS_VALUE_TYPE_UNKNOWN;
    }
    return secondary->value_type();
  }

  SharedRefPtr<DataType> secondary_data_type() const {
    if (!data_type_ || !data_type_->is_map()) {
      return SharedRefPtr<DataType>();
    }
    assert(static_cast<const SharedRefPtr<CollectionType>&>(data_type_)->types().size() > 1);
    return static_cast<const SharedRefPtr<CollectionType>&>(data_type_)->types()[1];
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

private:
  int protocol_version_;
  SharedRefPtr<DataType> data_type_;
  int32_t count_;

  char* data_;
  int32_t size_;
};

typedef std::vector<Value> OutputValueVec;

} // namespace cass

#endif
