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
#include "hash_table.hpp"
#include "macros.hpp"
#include "ref_counted.hpp"
#include "types.hpp"

#include <map>
#include <string>
#include <vector>

namespace cass {

class Collection;
class Tuple;
class UserTypeValue;

inline bool is_int64_type(CassValueType value_type) {
  return value_type == CASS_VALUE_TYPE_BIGINT ||
      value_type == CASS_VALUE_TYPE_COUNTER ||
      value_type == CASS_VALUE_TYPE_TIMESTAMP ||
      value_type == CASS_VALUE_TYPE_TIME;
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

// Only compare when both arguments are not empty
inline bool equals_both_not_empty(const std::string& s1,
                                  const std::string& s2) {
  return s1.empty() || s2.empty() || s1 == s2;
}

class DataType : public RefCounted<DataType> {
public:
  typedef SharedRefPtr<const DataType> ConstPtr;
  typedef std::vector<ConstPtr> Vec;

  static const DataType::ConstPtr NIL;

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

  bool is_tuple() const {
    return value_type_ == CASS_VALUE_TYPE_TUPLE;
  }

  bool is_user_type() const {
    return value_type_ == CASS_VALUE_TYPE_UDT;
  }

  bool is_custom() const {
    return value_type_ == CASS_VALUE_TYPE_CUSTOM;
  }

  virtual bool equals(const DataType::ConstPtr& data_type) const {
    switch (value_type_) {
      // "text" is an alias for  "varchar"
      case CASS_VALUE_TYPE_TEXT:
      case CASS_VALUE_TYPE_VARCHAR:
        return data_type->value_type_ == CASS_VALUE_TYPE_TEXT ||
            data_type->value_type_ == CASS_VALUE_TYPE_VARCHAR;
      default:
        return value_type_ == data_type->value_type_;
    }
  }

  virtual DataType* copy() const {
    return new DataType(value_type_);
  }

  virtual std::string to_string() const {
    switch (value_type_) {
      case CASS_VALUE_TYPE_ASCII: return "ascii";
      case CASS_VALUE_TYPE_BIGINT: return "bigint";
      case CASS_VALUE_TYPE_BLOB: return "blob";
      case CASS_VALUE_TYPE_BOOLEAN: return "boolean";
      case CASS_VALUE_TYPE_COUNTER: return "counter";
      case CASS_VALUE_TYPE_DECIMAL: return "decimal";
      case CASS_VALUE_TYPE_DOUBLE: return "double";
      case CASS_VALUE_TYPE_FLOAT: return "float";
      case CASS_VALUE_TYPE_INT: return "int";
      case CASS_VALUE_TYPE_TEXT: return "text";
      case CASS_VALUE_TYPE_TIMESTAMP: return "timestamp";
      case CASS_VALUE_TYPE_UUID: return "uuid";
      case CASS_VALUE_TYPE_VARCHAR: return "varchar";
      case CASS_VALUE_TYPE_VARINT: return "varint";
      case CASS_VALUE_TYPE_TIMEUUID: return "timeuuid";
      case CASS_VALUE_TYPE_INET: return "inet";
      case CASS_VALUE_TYPE_DATE: return "date";
      case CASS_VALUE_TYPE_TIME: return "time";
      case CASS_VALUE_TYPE_SMALL_INT: return "smallint";
      case CASS_VALUE_TYPE_TINY_INT: return "tinyint";
      case CASS_VALUE_TYPE_LIST: return "list";
      case CASS_VALUE_TYPE_MAP: return "map";
      case CASS_VALUE_TYPE_SET: return "set";
      case CASS_VALUE_TYPE_TUPLE: return "tuple";
      default: return "";
    }
  }

private:
  int protocol_version_;
  CassValueType value_type_;

private:
 DISALLOW_COPY_AND_ASSIGN(DataType);
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

  virtual bool equals(const DataType::ConstPtr& data_type) const {
    assert(value_type() == CASS_VALUE_TYPE_CUSTOM);
    if (data_type->value_type() != CASS_VALUE_TYPE_CUSTOM) {
      return false;
    }
    const SharedRefPtr<const CustomType>& custom_type(data_type);
    return equals_both_not_empty(class_name_, custom_type->class_name_);
  }

  virtual DataType* copy() const {
    return new CustomType(class_name_);
  }

  virtual std::string to_string() const {
    return class_name_;
  }

private:
  std::string class_name_;
};

class SubTypesDataType : public DataType {
public:
  SubTypesDataType(CassValueType type)
   : DataType(type) { }

  SubTypesDataType(CassValueType type, const DataType::Vec& types)
    : DataType(type)
    , types_(types) { }

  DataType::Vec& types() { return types_; }
  const DataType::Vec& types() const { return types_; }

  virtual std::string to_string() const {
    std::string str(DataType::to_string());
    str.push_back('<');
    bool first = true;
    for (DataType::Vec::const_iterator i = types_.begin(),
         end = types_.end();
         i != end; ++i) {
      if (!first) str.append(", ");
      str.append((*i)->to_string());
    }
    str.push_back('>');
    return str;
  }

protected:
  DataType::Vec types_;
};

class CollectionType : public SubTypesDataType {
public:
  typedef SharedRefPtr<const CollectionType> ConstPtr;

  CollectionType(CassValueType collection_type)
    : SubTypesDataType(collection_type) { }

  CollectionType(CassValueType collection_type,
                 size_t types_count)
    : SubTypesDataType(collection_type) {
    types_.reserve(types_count);
  }

  CollectionType(CassValueType collection_type, const DataType::Vec& types)
    : SubTypesDataType(collection_type, types) { }

  virtual bool equals(const DataType::ConstPtr& data_type) const {
    assert(value_type() == CASS_VALUE_TYPE_LIST ||
           value_type() == CASS_VALUE_TYPE_SET ||
           value_type() == CASS_VALUE_TYPE_MAP);

    if (value_type() != data_type->value_type()) {
      return false;
    }

    const CollectionType::ConstPtr& collection_type(data_type);

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
  static DataType::ConstPtr list(DataType::ConstPtr element_type) {
    DataType::Vec types;
    types.push_back(element_type);
    return DataType::ConstPtr(new CollectionType(CASS_VALUE_TYPE_LIST, types));
  }

  static DataType::ConstPtr set(DataType::ConstPtr element_type) {
    DataType::Vec types;
    types.push_back(element_type);
    return DataType::ConstPtr(new CollectionType(CASS_VALUE_TYPE_SET, types));
  }

  static DataType::ConstPtr map(DataType::ConstPtr key_type,
                                          DataType::ConstPtr value_type) {
    DataType::Vec types;
    types.push_back(key_type);
    types.push_back(value_type);
    return DataType::ConstPtr(new CollectionType(CASS_VALUE_TYPE_MAP, types));
  }
};

class TupleType : public SubTypesDataType {
public:
  typedef SharedRefPtr<const TupleType> ConstPtr;

  TupleType()
    : SubTypesDataType(CASS_VALUE_TYPE_TUPLE) { }

  TupleType(const DataType::Vec& types)
    : SubTypesDataType(CASS_VALUE_TYPE_TUPLE, types) { }

  virtual bool equals(const DataType::ConstPtr& data_type) const {
    assert(value_type() == CASS_VALUE_TYPE_TUPLE);

    if (value_type() != data_type->value_type()) {
      return false;
    }

    const SharedRefPtr<const TupleType>& tuple_type(data_type);

    // Only compare sub-types if both have sub-types
    if(!types_.empty() && !tuple_type->types_.empty()) {
      if (types_.size() != tuple_type->types_.size()) {
        return false;
      }
      for (size_t i = 0; i < types_.size(); ++i) {
        if(!types_[i]->equals(tuple_type->types_[i])) {
          return false;
        }
      }
    }

    return true;
  }

  virtual DataType* copy() const {
    return new TupleType(types_);
  }
};

class UserType : public DataType {
public:
  typedef SharedRefPtr<UserType> Ptr;
  typedef SharedRefPtr<const UserType> ConstPtr;
  typedef std::map<std::string, UserType::Ptr > Map;

  struct Field : public HashTableEntry<Field> {
    Field(const std::string& field_name,
          const DataType::ConstPtr& type)
      : name(field_name)
      , type(type) { }

    std::string name;
    DataType::ConstPtr type;
  };

  typedef CaseInsensitiveHashTable<Field>::EntryVec FieldVec;

  UserType()
    : DataType(CASS_VALUE_TYPE_UDT) { }

  UserType(size_t field_count)
    : DataType(CASS_VALUE_TYPE_UDT)
    , fields_(field_count) { }

  UserType(const std::string& keyspace,
           const std::string& type_name )
    : DataType(CASS_VALUE_TYPE_UDT)
    , keyspace_(keyspace)
    , type_name_(type_name) { }

  UserType(const std::string& keyspace,
           const std::string& type_name,
           const FieldVec& fields)
    : DataType(CASS_VALUE_TYPE_UDT)
    , keyspace_(keyspace)
    , type_name_(type_name)
    , fields_(fields) { }

  const std::string& keyspace() const { return keyspace_; }

  void set_keyspace(const std::string& keyspace) {
    keyspace_ = keyspace;
  }

  const std::string& type_name() const { return type_name_; }

  void set_type_name(const std::string& type_name) {
    type_name_ = type_name;
  }

  const FieldVec& fields() const { return fields_.entries(); }

  size_t get_indices(StringRef name, IndexVec* result) const {
    return fields_.get_indices(name, result);
  }

  void add_field(const std::string name, const DataType::ConstPtr& data_type) {
    fields_.add(Field(name, data_type));
  }

  void set_fields(const FieldVec& fields) {
    fields_.set_entries(fields);
  }

  virtual bool equals(const DataType::ConstPtr& data_type) const {
    assert(value_type() == CASS_VALUE_TYPE_UDT);
    if (data_type->value_type() != CASS_VALUE_TYPE_UDT) {
      return false;
    }

    const UserType::ConstPtr& user_type(data_type);

    if (!equals_both_not_empty(keyspace_, user_type->keyspace_)) {
      return false;
    }

    if (!equals_both_not_empty(type_name_, user_type->type_name_)) {
      return false;
    }

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
    return new UserType(keyspace_, type_name_, fields_.entries());
  }

  virtual std::string to_string() const {
    return type_name_;
  }

private:
  std::string keyspace_;
  std::string type_name_;
  CaseInsensitiveHashTable<Field> fields_;
};

class NativeDataTypes {
public:
  void init_class_names();
  const DataType::ConstPtr& by_class_name(const std::string& name) const;

  void init_cql_names();
  const DataType::ConstPtr& by_cql_name(const std::string& name) const;

private:
  typedef std::map<std::string, DataType::ConstPtr> DataTypeMap;
  DataTypeMap by_class_names_;
  DataTypeMap by_cql_names_;
};

template<class T>
struct IsValidDataType;

template<>
struct IsValidDataType<CassNull> {
  bool operator()(CassNull, const DataType::ConstPtr& data_type) const {
    return true;
  }
};

template<>
struct IsValidDataType<cass_int8_t> {
  bool operator()(cass_int8_t, const DataType::ConstPtr& data_type) const {
    return data_type->value_type() == CASS_VALUE_TYPE_TINY_INT;
  }
};

template<>
struct IsValidDataType<cass_int16_t> {
  bool operator()(cass_int16_t, const DataType::ConstPtr& data_type) const {
    return data_type->value_type() == CASS_VALUE_TYPE_SMALL_INT;
  }
};

template<>
struct IsValidDataType<cass_int32_t> {
  bool operator()(cass_int32_t, const DataType::ConstPtr& data_type) const {
    return data_type->value_type() == CASS_VALUE_TYPE_INT;
  }
};

template<>
struct IsValidDataType<cass_uint32_t> {
  bool operator()(cass_uint32_t, const DataType::ConstPtr& data_type) const {
    return data_type->value_type() == CASS_VALUE_TYPE_DATE;
  }
};

template<>
struct IsValidDataType<cass_int64_t> {
  bool operator()(cass_int64_t, const DataType::ConstPtr& data_type) const {
    return is_int64_type(data_type->value_type());
  }
};

template<>
struct IsValidDataType<cass_float_t> {
  bool operator()(cass_float_t, const DataType::ConstPtr& data_type) const {
    return data_type->value_type() == CASS_VALUE_TYPE_FLOAT;
  }
};

template<>
struct IsValidDataType<cass_double_t> {
  bool operator()(cass_double_t, const DataType::ConstPtr& data_type) const {
    return data_type->value_type() == CASS_VALUE_TYPE_DOUBLE;
  }
};

template<>
struct IsValidDataType<cass_bool_t> {
  bool operator()(cass_bool_t, const DataType::ConstPtr& data_type) const {
    return data_type->value_type() == CASS_VALUE_TYPE_BOOLEAN;
  }
};

template<>
struct IsValidDataType<CassString> {
  bool operator()(CassString, const DataType::ConstPtr& data_type) const {
    return is_string_type(data_type->value_type());
  }
};

template<>
struct IsValidDataType<CassBytes> {
  bool operator()(CassBytes, const DataType::ConstPtr& data_type) const {
    return is_bytes_type(data_type->value_type());
  }
};

template<>
struct IsValidDataType<CassUuid> {
  bool operator()(CassUuid, const DataType::ConstPtr& data_type) const {
    return is_uuid_type(data_type->value_type());
  }
};

template<>
struct IsValidDataType<CassInet> {
  bool operator()(CassInet, const DataType::ConstPtr& data_type) const {
    return data_type->value_type() == CASS_VALUE_TYPE_INET;
  }
};

template<>
struct IsValidDataType<CassDecimal> {
  bool operator()(CassDecimal, const DataType::ConstPtr& data_type) const {
    return data_type->value_type() == CASS_VALUE_TYPE_DECIMAL;
  }
};

template<>
struct IsValidDataType<const Collection*> {
  bool operator()(const Collection* value, const DataType::ConstPtr& data_type) const;
};

template<>
struct IsValidDataType<const Tuple*> {
  bool operator()(const Tuple* value, const DataType::ConstPtr& data_type) const;
};

template<>
struct IsValidDataType<const UserTypeValue*> {
  bool operator()(const UserTypeValue* value, const DataType::ConstPtr& data_type) const;
};

} // namespace cass

#endif
