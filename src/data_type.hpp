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

#ifndef __CASS_DATA_TYPE_HPP_INCLUDED__
#define __CASS_DATA_TYPE_HPP_INCLUDED__

#include "cassandra.h"
#include "hash_index.hpp"
#include "macros.hpp"
#include "ref_counted.hpp"
#include "types.hpp"

#include <map>
#include <string>
#include <vector>

namespace cass {

class Collection;
class UserTypeValue;

inline bool is_int64_type(CassValueType value_type) {
  return value_type == CASS_VALUE_TYPE_BIGINT ||
      value_type == CASS_VALUE_TYPE_COUNTER ||
      value_type == CASS_VALUE_TYPE_TIMESTAMP;
}

inline bool is_string_type(CassValueType value_type) {
  return value_type == CASS_VALUE_TYPE_ASCII ||
      value_type == CASS_VALUE_TYPE_TEXT ||
      value_type == CASS_VALUE_TYPE_VARCHAR;
}

inline bool is_bytes_type(CassValueType value_type) {
  return value_type == CASS_VALUE_TYPE_BLOB ||
      value_type == CASS_VALUE_TYPE_VARINT ||
      value_type == CASS_VALUE_TYPE_CUSTOM;
}

inline bool is_uuid_type(CassValueType value_type) {
  return value_type == CASS_VALUE_TYPE_TIMEUUID ||
      value_type == CASS_VALUE_TYPE_UUID;
}

class DataType : public RefCounted<DataType> {
public:
  static const SharedRefPtr<const DataType> NIL;

  DataType(CassValueType value_type)
    : value_type_(value_type) { }

  virtual ~DataType() { }

  CassValueType value_type() const { return value_type_; }

  bool is_collection() const  {
    return value_type_ == CASS_VALUE_TYPE_LIST ||
        value_type_ == CASS_VALUE_TYPE_MAP ||
        value_type_ == CASS_VALUE_TYPE_SET ||
        value_type_ == CASS_VALUE_TYPE_TUPLE;
  }

  bool is_map() const  {
    return value_type_ == CASS_VALUE_TYPE_MAP;
  }

  bool is_tuple() const {
    return value_type_ == CASS_VALUE_TYPE_TUPLE;
  }

  bool is_user_type() const {
    return value_type_ == CASS_VALUE_TYPE_UDT;
  }

  bool is_custom() const {
    return value_type_ == CASS_VALUE_TYPE_CUSTOM;
  }

  virtual bool equals(const SharedRefPtr<const DataType>& data_type) const {
    return value_type_ == data_type->value_type_;
  }

  virtual DataType* copy() const {
    return new DataType(value_type_);
  }

private:
  int protocol_version_;
  CassValueType value_type_;

private:
 DISALLOW_COPY_AND_ASSIGN(DataType);
};

typedef std::vector<SharedRefPtr<const DataType> > DataTypeVec;

class CollectionType : public DataType {
public:
  CollectionType(CassValueType collection_type)
    : DataType(collection_type) { }

  CollectionType(CassValueType collection_type,
                 size_t types_count)
    : DataType(collection_type) {
    types_.reserve(types_count);
  }

  CollectionType(CassValueType collection_type, const DataTypeVec& types)
    : DataType(collection_type)
    , types_(types) { }

  DataTypeVec& types() { return types_; }
  const DataTypeVec& types() const { return types_; }

  virtual bool equals(const SharedRefPtr<const DataType>& data_type) const {
    assert(value_type() == CASS_VALUE_TYPE_LIST ||
           value_type() == CASS_VALUE_TYPE_SET ||
           value_type() == CASS_VALUE_TYPE_MAP ||
           value_type() == CASS_VALUE_TYPE_TUPLE);

    if (value_type() != data_type->value_type()) {
      return false;
    }

    const SharedRefPtr<const CollectionType>& collection_type(data_type);

    // Only compare sub-types if both have sub-types
    if(!types_.empty() && !collection_type->types_.empty()) {
      if (types_.size() != collection_type->types_.size()) {
        return false;
      }
      for (size_t i = 0; i < types_.size(); ++i) {
        if(!types_[i]->equals(collection_type->types_[i])) {
          return false;
        }
      }
    }

    return true;
  }

  virtual DataType* copy() const {
    return new CollectionType(value_type(), types_);
  }

public:
  static SharedRefPtr<DataType> list(SharedRefPtr<DataType> element_type) {
    DataTypeVec types;
    types.push_back(element_type);
    return SharedRefPtr<DataType>(new CollectionType(CASS_VALUE_TYPE_LIST, types));
  }

  static SharedRefPtr<DataType> set(SharedRefPtr<DataType> element_type) {
    DataTypeVec types;
    types.push_back(element_type);
    return SharedRefPtr<DataType>(new CollectionType(CASS_VALUE_TYPE_SET, types));
  }

  static SharedRefPtr<DataType> map(SharedRefPtr<DataType> key_type, SharedRefPtr<DataType> value_type) {
    DataTypeVec types;
    types.push_back(key_type);
    types.push_back(value_type);
    return SharedRefPtr<DataType>(new CollectionType(CASS_VALUE_TYPE_MAP, types));
  }

  static SharedRefPtr<DataType> tuple(const DataTypeVec& types) {
    return SharedRefPtr<DataType>(new CollectionType(CASS_VALUE_TYPE_TUPLE, types));
  }

private:
  DataTypeVec types_;
};

class CustomType : public DataType {
public:
  CustomType()
    : DataType(CASS_VALUE_TYPE_CUSTOM) { }

  CustomType(const std::string& class_name)
    : DataType(CASS_VALUE_TYPE_CUSTOM)
    , class_name_(class_name) { }

  const std::string& class_name() const { return class_name_; }

  void set_class_name(const std::string& class_name) {
    class_name_ = class_name;
  }

  virtual bool equals(const SharedRefPtr<const DataType>& data_type) const {
    assert(value_type() == CASS_VALUE_TYPE_CUSTOM);
    if (data_type->value_type() != CASS_VALUE_TYPE_CUSTOM) {
      return false;
    }
    const SharedRefPtr<const CustomType>& custom_type(data_type);
    return class_name_ == custom_type->class_name_;
  }

  virtual DataType* copy() const {
    return new CustomType(class_name_);
  }

private:
  std::string class_name_;
};

class UserType : public DataType {
public:
  struct Field : public HashIndex::Entry {
    Field(const std::string& field_name,
          const SharedRefPtr<const DataType>& type)
      : field_name(field_name)
      , type(type) { }

    std::string field_name;
    SharedRefPtr<const DataType> type;
  };

  typedef std::vector<Field> FieldVec;

  UserType()
    : DataType(CASS_VALUE_TYPE_UDT)
    , index_(0) { }

  UserType(const std::string& keyspace,
           const std::string& type_name,
           size_t field_count)
    : DataType(CASS_VALUE_TYPE_UDT)
    , keyspace_(keyspace)
    , type_name_(type_name)
    , index_(field_count) { }

  UserType(const std::string& keyspace,
           const std::string& type_name,
           const FieldVec& fields)
    : DataType(CASS_VALUE_TYPE_UDT)
    , keyspace_(keyspace)
    , type_name_(type_name)
    , fields_(fields)
    , index_(fields.size()) {
    reindex_fields();
  }

  const std::string& keyspace() const { return keyspace_; }

  void set_keyspace(const std::string& keyspace) {
    keyspace_ = keyspace;
  }

  const std::string& type_name() const { return type_name_; }

  void set_type_name(const std::string& type_name) {
    type_name_ = type_name;
  }

  const FieldVec& fields() const { return fields_; }

  size_t get_indices(StringRef name, HashIndex::IndexVec* result) const {
    return index_.get(name, result);
  }

  void add_field(StringRef name, const SharedRefPtr<const DataType>& data_type) {
    if (index_.requires_resize()) {
      index_.reset(fields_.capacity());
      reindex_fields();
    }
    size_t index = fields_.size();
    fields_.push_back(Field(name.to_string(), data_type));
    Field* field = &fields_.back();
    field->index = index;
    field->name = StringRef(field->field_name);
    index_.insert(field);
  }

  virtual bool equals(const SharedRefPtr<const DataType>& data_type) const {
    assert(value_type() == CASS_VALUE_TYPE_UDT);
    if (data_type->value_type() != CASS_VALUE_TYPE_UDT) {
      return false;
    }

    const SharedRefPtr<const UserType>& user_type(data_type);
    if (fields_.size() != user_type->fields_.size()) {
      return false;
    }

    for (size_t i = 0; i < fields_.size(); ++i) {
      if (fields_[i].name != user_type->fields_[i].name ||
          !fields_[i].type->equals(user_type->fields_[i].type)) {
        return false;
      }
    }

    return true;
  }

  virtual DataType* copy() const {
    return new UserType(keyspace_, type_name_, fields_);
  }

private:
  void reindex_fields() {
    for (size_t i = 0; i < fields_.size(); ++i) {
      Field* field = &fields_[i];
      field->index = i;
      field->name = StringRef(field->field_name);
      index_.insert(field);
    }
  }

private:
  std::string keyspace_;
  std::string type_name_;
  FieldVec fields_;
  HashIndex index_;
};

template<class T>
struct IsValidDataType;

template<>
struct IsValidDataType<CassNull> {
  bool operator()(CassNull, const SharedRefPtr<const DataType>& data_type) const {
    return true;
  }
};

template<>
struct IsValidDataType<cass_int32_t> {
  bool operator()(cass_int32_t, const SharedRefPtr<const DataType>& data_type) const {
    return data_type->value_type() == CASS_VALUE_TYPE_INT;
  }
};

template<>
struct IsValidDataType<cass_int64_t> {
  bool operator()(cass_int64_t, const SharedRefPtr<const DataType>& data_type) const {
    return is_int64_type(data_type->value_type());
  }
};

template<>
struct IsValidDataType<cass_float_t> {
  bool operator()(cass_float_t, const SharedRefPtr<const DataType>& data_type) const {
    return data_type->value_type() == CASS_VALUE_TYPE_FLOAT;
  }
};

template<>
struct IsValidDataType<cass_double_t> {
  bool operator()(cass_double_t, const SharedRefPtr<const DataType>& data_type) const {
    return data_type->value_type() == CASS_VALUE_TYPE_DOUBLE;
  }
};

template<>
struct IsValidDataType<cass_bool_t> {
  bool operator()(cass_bool_t, const SharedRefPtr<const DataType>& data_type) const {
    return data_type->value_type() == CASS_VALUE_TYPE_BOOLEAN;
  }
};

template<>
struct IsValidDataType<CassString> {
  bool operator()(CassString, const SharedRefPtr<const DataType>& data_type) const {
    return is_string_type(data_type->value_type());
  }
};

template<>
struct IsValidDataType<CassBytes> {
  bool operator()(CassBytes, const SharedRefPtr<const DataType>& data_type) const {
    return is_bytes_type(data_type->value_type());
  }
};

template<>
struct IsValidDataType<CassUuid> {
  bool operator()(CassUuid, const SharedRefPtr<const DataType>& data_type) const {
    return is_uuid_type(data_type->value_type());
  }
};

template<>
struct IsValidDataType<CassInet> {
  bool operator()(CassInet, const SharedRefPtr<const DataType>& data_type) const {
    return data_type->value_type() == CASS_VALUE_TYPE_INET;
  }
};

template<>
struct IsValidDataType<CassDecimal> {
  bool operator()(CassDecimal, const SharedRefPtr<const DataType>& data_type) const {
    return data_type->value_type() == CASS_VALUE_TYPE_DECIMAL;
  }
};

template<>
struct IsValidDataType<CassCustom> {
  bool operator()(CassCustom, const SharedRefPtr<const DataType>& data_type) const {
    return true;
  }
};

template<>
struct IsValidDataType<const Collection*> {
  bool operator()(const Collection* value, const SharedRefPtr<const DataType>& data_type) const;
};

template<>
struct IsValidDataType<const UserTypeValue*> {
  bool operator()(const UserTypeValue* value, const SharedRefPtr<const DataType>& data_type) const;
};

} // namespace cass

#endif
