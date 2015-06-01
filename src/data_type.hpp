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
      value_type == CASS_VALUE_TYPE_VARINT;
}

inline bool is_uuid_type(CassValueType value_type) {
  return value_type == CASS_VALUE_TYPE_TIMEUUID ||
      value_type == CASS_VALUE_TYPE_UUID;
}

class DataType : public RefCounted<DataType> {
public:
  static const SharedRefPtr<DataType> NIL;

  DataType(CassValueType value_type)
    : value_type_(value_type) { }

  virtual ~DataType() { }

  CassValueType value_type() const { return value_type_; }

  bool is_collection() const  {
    return value_type_ == CASS_VALUE_TYPE_LIST ||
        value_type_ == CASS_VALUE_TYPE_MAP ||
        value_type_ == CASS_VALUE_TYPE_SET;
  }

  bool is_map() const  {
    return value_type_ == CASS_VALUE_TYPE_MAP;
  }

  bool is_user_type() const {
    return value_type_ == CASS_VALUE_TYPE_UDT;
  }

  bool is_tuple() const {
    return value_type_ == CASS_VALUE_TYPE_TUPLE;
  }

  virtual bool equals(const SharedRefPtr<DataType>& data_type, bool exact) const {
    if (exact) {
      return value_type_ == data_type->value_type_;
    } else {
      switch (value_type_) {
        case CASS_VALUE_TYPE_BIGINT:
        case CASS_VALUE_TYPE_COUNTER:
        case CASS_VALUE_TYPE_TIMESTAMP:
          return is_int64_type(data_type->value_type_);

        case CASS_VALUE_TYPE_ASCII:
        case CASS_VALUE_TYPE_TEXT:
        case CASS_VALUE_TYPE_VARCHAR:
          return is_string_type(data_type->value_type_);

        case CASS_VALUE_TYPE_BLOB:
        case CASS_VALUE_TYPE_VARINT:
          return is_bytes_type(data_type->value_type_);

        case CASS_VALUE_TYPE_TIMEUUID:
        case CASS_VALUE_TYPE_UUID:
          return is_uuid_type(data_type->value_type_);

        default:
          return value_type_ == data_type->value_type_;
      }
    }
  }

private:
  CassValueType value_type_;

private:
 DISALLOW_COPY_AND_ASSIGN(DataType);
};

typedef std::vector<SharedRefPtr<DataType> > DataTypeVec;

class CollectionType : public DataType {
public:
  CollectionType(CassValueType collection_type)
    : DataType(collection_type) { }

  CollectionType(CassValueType collection_type, const DataTypeVec& types)
    : DataType(collection_type)
    , types_(types) { }

  DataTypeVec& types() { return types_; }
  const DataTypeVec& types() const { return types_; }

  virtual bool equals(const SharedRefPtr<DataType>& data_type, bool exact) const {
    assert(value_type() == CASS_VALUE_TYPE_LIST ||
           value_type() == CASS_VALUE_TYPE_SET ||
           value_type() == CASS_VALUE_TYPE_MAP ||
           value_type() == CASS_VALUE_TYPE_TUPLE);

    if (value_type() != data_type->value_type()) {
      return false;
    }

    const SharedRefPtr<CollectionType>& collection_type(data_type);

    if (exact) {
      if (types_.size() != collection_type->types_.size()) {
        return false;
      }

      for (size_t i = 0; i < types_.size(); ++i) {
        if(!types_[i]->equals(collection_type->types_[i], true)) {
          return false;
        }
      }
    } else if(!types_.empty() && !collection_type->types_.empty()) { // An empty collection doesn't have subtypes
      if (types_.size() != collection_type->types_.size()) {
        return false;
      }
      for (size_t i = 0; i < types_.size(); ++i) {
        if(!types_[i]->equals(collection_type->types_[i], false)) {
          return false;
        }
      }
    }

    return true;
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
  CustomType(const std::string& class_name)
    : DataType(CASS_VALUE_TYPE_CUSTOM)
    , class_name_(class_name) { }

  const std::string& class_name() const { return class_name_; }
private:
  std::string class_name_;
};

class UserType : public DataType {
public:
  struct Field : public HashIndex::Entry {
    Field(const std::string& field_name,
          SharedRefPtr<DataType> type)
      : field_name(field_name)
      , type(type) { }

    std::string field_name;
    SharedRefPtr<DataType> type;
  };

  typedef std::vector<Field> FieldVec;

  UserType(const std::string& keyspace,
           const std::string& type_name,
           const FieldVec& fields)
    : DataType(CASS_VALUE_TYPE_UDT)
    , keyspace_(keyspace)
    , type_name_(type_name)
    , fields_(fields)
    , index_(fields.size()) {
    for (size_t i = 0; i < fields_.size(); ++i) {
      Field* field = &fields_[i];
      field->index = i;
      field->name = StringRef(field->field_name);
      index_.insert(field);
    }
  }

  const std::string& keyspace() const { return keyspace_; }
  const std::string& type_name() const { return type_name_; }
  const FieldVec& fields() const { return fields_; }

  size_t get_indices(StringRef name, HashIndex::IndexVec* result) const {
    return index_.get(name, result);
  }

  virtual bool equals(const SharedRefPtr<DataType>& data_type, bool exact) const {
    assert(value_type() == CASS_VALUE_TYPE_UDT);
    if (data_type->value_type() != CASS_VALUE_TYPE_UDT) {
      return false;
    }

    const SharedRefPtr<UserType>& user_type(data_type);
    if (fields_.size() != user_type->fields_.size()) {
      return false;
    }

    // UDTs are always compared using "exact" because they come directly from Cassandra
    for (size_t i = 0; i < fields_.size(); ++i) {
      if (fields_[i].name != user_type->fields_[i].name ||
          !fields_[i].type->equals(user_type->fields_[i].type, true)) {
        return false;
      }
    }

    return true;
  }

private:
  std::string keyspace_;
  std::string type_name_;
  FieldVec fields_;
  HashIndex index_;
};

template<class T>
struct IsValidDataType;

template<class T>
struct CreateDataType;

template<>
struct IsValidDataType<CassNull> {
  bool operator()(CassNull, const SharedRefPtr<DataType>& data_type) const {
    return true;
  }
};

template<>
struct IsValidDataType<cass_int32_t> {
  bool operator()(cass_int32_t, const SharedRefPtr<DataType>& data_type) const {
    return data_type->value_type() == CASS_VALUE_TYPE_INT;
  }
};

template<>
struct CreateDataType<cass_int32_t> {
  SharedRefPtr<DataType> operator()(cass_int32_t) const {
    return SharedRefPtr<DataType>(new DataType(CASS_VALUE_TYPE_INT));
  }
};

template<>
struct IsValidDataType<cass_int64_t> {
  bool operator()(cass_int64_t, const SharedRefPtr<DataType>& data_type) const {
    return is_int64_type(data_type->value_type());
  }
};

template<>
struct CreateDataType<cass_int64_t> {
  SharedRefPtr<DataType> operator()(cass_int64_t) const {
    return SharedRefPtr<DataType>(new DataType(CASS_VALUE_TYPE_BIGINT));
  }
};

template<>
struct IsValidDataType<cass_float_t> {
  bool operator()(cass_float_t, const SharedRefPtr<DataType>& data_type) const {
    return data_type->value_type() == CASS_VALUE_TYPE_FLOAT;
  }
};

template<>
struct CreateDataType<cass_float_t> {
  SharedRefPtr<DataType> operator()(cass_float_t) const {
    return SharedRefPtr<DataType>(new DataType(CASS_VALUE_TYPE_FLOAT));
  }
};

template<>
struct IsValidDataType<cass_double_t> {
  bool operator()(cass_double_t, const SharedRefPtr<DataType>& data_type) const {
    return data_type->value_type() == CASS_VALUE_TYPE_DOUBLE;
  }
};

template<>
struct CreateDataType<cass_double_t> {
  SharedRefPtr<DataType> operator()(cass_double_t) const {
    return SharedRefPtr<DataType>(new DataType(CASS_VALUE_TYPE_DOUBLE));
  }
};

template<>
struct IsValidDataType<cass_bool_t> {
  bool operator()(cass_bool_t, const SharedRefPtr<DataType>& data_type) const {
    return data_type->value_type() == CASS_VALUE_TYPE_BOOLEAN;
  }
};

template<>
struct CreateDataType<cass_bool_t> {
  SharedRefPtr<DataType> operator()(cass_bool_t) const {
    return SharedRefPtr<DataType>(new DataType(CASS_VALUE_TYPE_BOOLEAN));
  }
};

template<>
struct IsValidDataType<CassString> {
  bool operator()(CassString, const SharedRefPtr<DataType>& data_type) const {
    return is_string_type(data_type->value_type());
  }
};

template<>
struct CreateDataType<CassString> {
  SharedRefPtr<DataType> operator()(CassString) const {
    return SharedRefPtr<DataType>(new DataType(CASS_VALUE_TYPE_VARCHAR));
  }
};

template<>
struct IsValidDataType<CassBytes> {
  bool operator()(CassBytes, const SharedRefPtr<DataType>& data_type) const {
    return is_bytes_type(data_type->value_type());
  }
};

template<>
struct CreateDataType<CassBytes> {
  SharedRefPtr<DataType> operator()(CassBytes) const {
    return SharedRefPtr<DataType>(new DataType(CASS_VALUE_TYPE_BLOB));
  }
};

template<>
struct IsValidDataType<CassUuid> {
  bool operator()(CassUuid, const SharedRefPtr<DataType>& data_type) const {
    return is_uuid_type(data_type->value_type());
  }
};

template<>
struct CreateDataType<CassUuid> {
  SharedRefPtr<DataType> operator()(CassUuid) const {
    return SharedRefPtr<DataType>(new DataType(CASS_VALUE_TYPE_UUID));
  }
};

template<>
struct IsValidDataType<CassInet> {
  bool operator()(CassInet, const SharedRefPtr<DataType>& data_type) const {
    return data_type->value_type() == CASS_VALUE_TYPE_INET;
  }
};

template<>
struct CreateDataType<CassInet> {
  SharedRefPtr<DataType> operator()(CassInet) const {
    return SharedRefPtr<DataType>(new DataType(CASS_VALUE_TYPE_INET));
  }
};

template<>
struct IsValidDataType<CassDecimal> {
  bool operator()(CassDecimal, const SharedRefPtr<DataType>& data_type) const {
    return data_type->value_type() == CASS_VALUE_TYPE_DECIMAL;
  }
};

template<>
struct CreateDataType<CassDecimal> {
  SharedRefPtr<DataType> operator()(CassDecimal) const {
    return SharedRefPtr<DataType>(new DataType(CASS_VALUE_TYPE_DECIMAL));
  }
};

template<>
struct IsValidDataType<CassCustom> {
  bool operator()(CassCustom, const SharedRefPtr<DataType>& data_type) const {
    return true;
  }
};

template<>
struct IsValidDataType<const Collection*> {
  bool operator()(const Collection* value, const SharedRefPtr<DataType>& data_type) const;
};

template<>
struct CreateDataType<const Collection*> {
  SharedRefPtr<DataType> operator()(const Collection* value) const;
};

} // namespace cass

#endif
