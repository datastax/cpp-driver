/*
  Copyright (c) 2014-2016 DataStax

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

#include "data_type_parser.hpp"

#include "utils.hpp"
#include "logger.hpp"
#include "scoped_ptr.hpp"
#include "string_ref.hpp"

#include <sstream>

#define REVERSED_TYPE "org.apache.cassandra.db.marshal.ReversedType"
#define FROZEN_TYPE "org.apache.cassandra.db.marshal.FrozenType"
#define COMPOSITE_TYPE "org.apache.cassandra.db.marshal.CompositeType"
#define COLLECTION_TYPE "org.apache.cassandra.db.marshal.ColumnToCollectionType"

#define LIST_TYPE "org.apache.cassandra.db.marshal.ListType"
#define SET_TYPE "org.apache.cassandra.db.marshal.SetType"
#define MAP_TYPE "org.apache.cassandra.db.marshal.MapType"
#define UDT_TYPE "org.apache.cassandra.db.marshal.UserType"
#define TUPLE_TYPE "org.apache.cassandra.db.marshal.TupleType"

namespace cass {

int hex_value(int c) {
  if (c >= '0' && c <= '9') {
    return c - '0';
  } else if (c >= 'A' && c <= 'F') {
    return c - 'A' + 10;
  } else if (c >= 'a' && c <= 'f') {
    return c - 'a' + 10;
  }
  return -1;
}

bool from_hex(const std::string& hex, std::string* result) {
  if ((hex.length() & 1) == 1) { // Invalid if not divisible by 2
    return false;
  }
  size_t size = hex.length() / 2;
  result->resize(size);
  for (size_t i = 0; i < size; ++i) {
    int half0 = hex_value(hex[i * 2]);
    int half1 = hex_value(hex[i * 2 + 1]);
    if (half0 < 0 || half1 < 0) {
      return false;
    }
    (*result)[i] = static_cast<char>((static_cast<uint8_t>(half0) << 4) | static_cast<uint8_t>(half1));
  }
  return true;
}

DataType::ConstPtr DataTypeCqlNameParser::parse(const std::string& type,
                                                const NativeDataTypes& native_types,
                                                KeyspaceMetadata* keyspace,
                                                bool is_frozen) {
  Parser parser(type, 0);
  std::string type_name;
  Parser::TypeParamsVec params;

  parser.parse_type_name(&type_name);
  std::transform(type_name.begin(), type_name.end(), type_name.begin(), tolower);

  DataType::ConstPtr native_type(native_types.by_cql_name(type_name));
  if (native_type) {
    return native_type;
  }

  if (type_name == "list") {
    parser.parse_type_parameters(&params);
    if (params.size() != 1) {
      LOG_ERROR("Expecting single parameter for list %s", type.c_str());
      return DataType::NIL;
    }
    DataType::ConstPtr element_type = parse(params[0], native_types, keyspace);
    return CollectionType::list(element_type, is_frozen);
  }

  if (type_name == "set") {
    parser.parse_type_parameters(&params);
    if (params.size() != 1) {
      LOG_ERROR("Expecting single parameter for set %s", type.c_str());
      return DataType::NIL;
    }
    DataType::ConstPtr element_type = parse(params[0], native_types, keyspace);
    return CollectionType::set(element_type, is_frozen);
  }

  if (type_name == "map") {
    parser.parse_type_parameters(&params);
    if (params.size() != 2) {
      LOG_ERROR("Expecting two parameters for set %s", type.c_str());
      return DataType::NIL;
    }
    DataType::ConstPtr key_type = parse(params[0], native_types, keyspace);
    DataType::ConstPtr value_type = parse(params[1], native_types, keyspace);
    return CollectionType::map(key_type, value_type, is_frozen);
  }

  if (type_name == "tuple") {
    parser.parse_type_parameters(&params);
    if (params.empty()) {
      LOG_ERROR("Expecting at least a one parameter for tuple %s", type.c_str());
      return DataType::NIL;
    }
    DataType::Vec types;
    for (Parser::TypeParamsVec::iterator i = params.begin(),
         end = params.end();
         i != end;  ++i) {
      types.push_back(parse(*i, native_types, keyspace));
    }
    return DataType::ConstPtr(new TupleType(types, is_frozen));
  }

  if (type_name == "frozen") {
    parser.parse_type_parameters(&params);
    if (params.size() != 1) {
      LOG_ERROR("Expecting single parameter for frozen keyword %s", type.c_str());
      return DataType::NIL;
    }
    return parse(params[0], native_types, keyspace, true);
  }

  if (type_name == "empty") {
    return DataType::ConstPtr(new CustomType(type_name));
  }

  if (type_name.empty()) {
    return DataType::NIL;
  }

  UserType::ConstPtr user_type(keyspace->get_or_create_user_type(type_name, is_frozen));

  if (user_type->is_frozen() != is_frozen) {
    return UserType::Ptr(new UserType(user_type->keyspace(),
                                      user_type->type_name(),
                                      user_type->fields(),
                                      is_frozen));
  }

  return user_type;
}

void DataTypeCqlNameParser::Parser::parse_type_name(std::string* name) {
  skip_blank();
  read_next_identifier(name);
}

void DataTypeCqlNameParser::Parser::parse_type_parameters(TypeParamsVec* params) {
  params->clear();

  if (is_eos()) return;

  skip_blank_and_comma();

  if (str_[index_] != '<') {
    LOG_ERROR("Expecting char %u of %s to be '<' but '%c' found",
              (unsigned int)index_, str_.c_str(), str_[index_]);
    return;
  }

  ++index_; // Skip '<'

  std::string name;
  std::string args;
  while (skip_blank_and_comma()) {
    if (str_[index_] == '>') {
      ++index_;
      return;
    }
    parse_type_name(&name);
    if (!read_raw_type_parameters(&args))
      return;
    params->push_back(name + args);
  }
}

void DataTypeCqlNameParser::Parser::read_next_identifier(std::string* name) {
  size_t start_index = index_;
  if (str_[start_index] == '"') {
    ++index_;
    while (!is_eos()) {
      bool is_quote = str_[index_] == '"';
      ++index_;
      if (is_quote) {
        if  (!is_eos() && str_[index_] == '"') {
          ++index_;
        } else {
          break;
        }
      }
    }
  } else {
    while (!is_eos() && (is_identifier_char(str_[index_]) || str_[index_] == '"')) {
      ++index_;
    }
  }
  name->assign(str_.begin() + start_index, str_.begin() + index_);
}

bool DataTypeCqlNameParser::Parser::read_raw_type_parameters(std::string* params) {
  skip_blank();

  params->clear();

  if (is_eos() || str_[index_] == '>' || str_[index_] == ',') return true;

  if (str_[index_] != '<') {
    LOG_ERROR("Expecting char %u of %s to be '<' but '%c' found",
              (unsigned int)index_, str_.c_str(), str_[index_]);
    return false;
  }

  size_t start_index = index_;
  int open = 1;
  bool in_quotes = false;
  while (open > 0) {
    ++index_;

    if (is_eos()) {
      LOG_ERROR("Angle brackets not closed in type %s", str_.c_str());
      return false;
    }

    if (!in_quotes) {
      if (str_[index_] == '"') {
        in_quotes = true;
      } else if (str_[index_] == '<') {
        open++;
      } else if (str_[index_] == '>') {
        open--;
      }
    } else if (str_[index_] == '"') {
      in_quotes = false;
    }
  }

  ++index_; // Move past the  trailing '>'
  params->assign(str_.begin() + start_index, str_.begin() + index_);
  return true;
}

bool DataTypeClassNameParser::is_reversed(const std::string& type) {
  return starts_with(type, REVERSED_TYPE);
}

bool DataTypeClassNameParser::is_frozen(const std::string& type) {
  return starts_with(type, FROZEN_TYPE);
}

bool DataTypeClassNameParser::is_composite(const std::string& type) {
  return starts_with(type, COMPOSITE_TYPE);
}

bool DataTypeClassNameParser::is_collection(const std::string& type) {
  return starts_with(type, COLLECTION_TYPE);
}

bool DataTypeClassNameParser::is_user_type(const std::string& type) {
  return starts_with(type, UDT_TYPE);
}

bool DataTypeClassNameParser::is_tuple_type(const std::string& type) {
  return starts_with(type, TUPLE_TYPE);
}

DataType::ConstPtr DataTypeClassNameParser::parse_one(const std::string& type, const NativeDataTypes& native_types) {
  bool is_frozen = DataTypeClassNameParser::is_frozen(type);

  std::string class_name;

  if (is_reversed(type) || is_frozen) {
    if (!get_nested_class_name(type, &class_name)) {
      return DataType::ConstPtr();
    }
  } else {
    class_name = type;
  }

  Parser parser(class_name, 0);
  std::string next;

  parser.get_next_name(&next);

  if (starts_with(next, LIST_TYPE)) {
    TypeParamsVec params;
    if (!parser.get_type_params(&params) || params.empty()) {
      return DataType::ConstPtr();
    }
    DataType::ConstPtr element_type(parse_one(params[0], native_types));
    if (!element_type) {
      return DataType::ConstPtr();
    }
    return CollectionType::list(element_type, is_frozen);
  } else if(starts_with(next, SET_TYPE)) {
    TypeParamsVec params;
    if (!parser.get_type_params(&params) || params.empty()) {
      return DataType::ConstPtr();
    }
    DataType::ConstPtr element_type(parse_one(params[0], native_types));
    if (!element_type) {
      return DataType::ConstPtr();
    }
    return CollectionType::set(element_type, is_frozen);
  } else if(starts_with(next, MAP_TYPE)) {
    TypeParamsVec params;
    if (!parser.get_type_params(&params) || params.size() < 2) {
      return DataType::ConstPtr();
    }
    DataType::ConstPtr key_type(parse_one(params[0], native_types));
    DataType::ConstPtr value_type(parse_one(params[1], native_types));
    if (!key_type || !value_type) {
      return DataType::ConstPtr();
    }
    return CollectionType::map(key_type, value_type, is_frozen);
  }

  if (is_frozen) {
    LOG_WARN("Got a frozen type for something other than a collection, "
             "this driver might be too old for your version of Cassandra");
  }

  if (is_user_type(next)) {
    parser.skip(); // Skip '('

    std::string keyspace;
    if (!parser.read_one(&keyspace)) {
      return DataType::ConstPtr();
    }
    parser.skip_blank_and_comma();

    std::string hex;
    if (!parser.read_one(&hex)) {
      return DataType::ConstPtr();
    }

    std::string type_name;
    if (!from_hex(hex, &type_name)) {
      LOG_ERROR("Invalid hex string \"%s\" for parameter", hex.c_str());
      return DataType::ConstPtr();
    }

    if (keyspace.empty() || type_name.empty()) {
      LOG_ERROR("UDT has no keyspace or type name");
      return DataType::ConstPtr();
    }

    parser.skip_blank_and_comma();
    NameAndTypeParamsVec raw_fields;
    if (!parser.get_name_and_type_params(&raw_fields)) {
      return DataType::ConstPtr();
    }

    UserType::FieldVec fields;
    for (NameAndTypeParamsVec::const_iterator i = raw_fields.begin(),
         end = raw_fields.end(); i != end; ++i) {
      DataType::ConstPtr data_type = parse_one(i->second, native_types);
      if (!data_type) {
        return DataType::ConstPtr();
      }
      fields.push_back(UserType::Field(i->first, data_type));
    }

    return DataType::ConstPtr(new UserType(keyspace, type_name, fields, true));
  }

  if (is_tuple_type(type)) {
    TypeParamsVec raw_types;
    if (!parser.get_type_params(&raw_types)) {
      return DataType::ConstPtr();
    }

    DataType::Vec types;
    for (TypeParamsVec::const_iterator i = raw_types.begin(),
         end = raw_types.end(); i != end; ++i) {
      DataType::ConstPtr data_type = parse_one(*i, native_types);
      if (!data_type) {
        return DataType::ConstPtr();
      }
      types.push_back(data_type);
    }

    return DataType::ConstPtr(new TupleType(types, true));
  }

  DataType::ConstPtr native_type(native_types.by_class_name(next));
  if (native_type) {
    return native_type;
  }

  return DataType::ConstPtr(new CustomType(next));
}

ParseResult::Ptr DataTypeClassNameParser::parse_with_composite(const std::string& type, const NativeDataTypes& native_types) {
  Parser parser(type, 0);

  std::string next;
  parser.get_next_name(&next);

  if (!is_composite(next)) {
    DataType::ConstPtr data_type = parse_one(type, native_types);
    if (!data_type) {
      return ParseResult::Ptr();
    }
    return ParseResult::Ptr(new ParseResult(data_type, is_reversed(next)));
  }

  TypeParamsVec sub_class_names;

  if (!parser.get_type_params(&sub_class_names)) {
    return ParseResult::Ptr();
  }

  if (sub_class_names.empty()) {
    LOG_ERROR("Expected at least one subclass type for a composite type");
    return ParseResult::Ptr();
  }

  ParseResult::CollectionMap collections;
  const std::string& last = sub_class_names.back();
  size_t count = sub_class_names.size();
  if (is_collection(last)) {
    count--;

    Parser collection_parser(last, 0);
    collection_parser.get_next_name();
    NameAndTypeParamsVec params;
    if (!collection_parser.get_collection_params(&params)) {
      return ParseResult::Ptr();
    }

    for (NameAndTypeParamsVec::const_iterator i = params.begin(),
         end = params.end(); i != end; ++i) {
      DataType::ConstPtr data_type = parse_one(i->second, native_types);
      if (!data_type) {
        return ParseResult::Ptr();
      }
      collections[i->first] = data_type;
    }
  }

  DataType::Vec types;
  ParseResult::ReversedVec reversed;
  for (size_t i = 0; i < count; ++i) {
    DataType::ConstPtr data_type = parse_one(sub_class_names[i], native_types);
    if (!data_type) {
      return ParseResult::Ptr();
    }
    types.push_back(data_type);
    reversed.push_back(is_reversed(sub_class_names[i]));
  }

  return ParseResult::Ptr(new ParseResult(true, types, reversed, collections));
}

bool DataTypeClassNameParser::get_nested_class_name(const std::string& type, std::string* class_name) {
  Parser parser(type, 0);
  parser.get_next_name();
  TypeParamsVec params;
  parser.get_type_params(&params);
  if (params.size() != 1) {
    return false;
  }
  *class_name = params[0];
  return true;
}

bool DataTypeClassNameParser::Parser::read_one(std::string* name_and_args) {
  std::string name;
  get_next_name(&name);
  std::string args;
  if (!read_raw_arguments(&args)) {
    return false;
  }
  *name_and_args = name + args;
  return true;
}

void DataTypeClassNameParser::Parser::get_next_name(std::string* name) {
  skip_blank();
  read_next_identifier(name);
}

bool DataTypeClassNameParser::Parser::get_type_params(TypeParamsVec* params) {
  if (is_eos()) {
    params->clear();
    return true;
  }

  if (str_[index_] != '(') {
    parse_error(str_, index_,  "Expected '(' before type parameters");
    return false;
  }

  ++index_; // Skip '('

  while (skip_blank_and_comma()) {
    if (str_[index_] == ')') {
      ++index_;
      return true;
    }

    std::string param;
    if (!read_one(&param)) {
      return false;
    }
    params->push_back(param);
  }

  parse_error(str_, index_,  "Unexpected end of string");
  return false;
}

bool DataTypeClassNameParser::Parser::get_name_and_type_params(NameAndTypeParamsVec* params) {
  while (skip_blank_and_comma()) {
    if (str_[index_] == ')') {
      ++index_;
      return true;
    }

    std::string hex;
    read_next_identifier(&hex);

    std::string name;
    if (!from_hex(hex, &name)) {
      LOG_ERROR("Invalid hex string \"%s\" for parameter", hex.c_str());
      return DataType::ConstPtr();
    }

    skip_blank();

    if (str_[index_] != ':') {
      parse_error(str_, index_,  "Expected ':'");
      return false;
    }

    ++index_;
    skip_blank();

    std::string type;

    if (!read_one(&type)) {
      return false;
    }

    params->push_back(std::make_pair(name, type));
  }

  parse_error(str_, index_,  "Unexpected end of string");
  return false;
}

bool DataTypeClassNameParser::Parser::get_collection_params(NameAndTypeParamsVec* params) {
  if (is_eos()) {
    params->clear();
    return true;
  }

  if (str_[index_] != '(') {
    parse_error(str_, index_,  "Expected '(' before collection parameters");
    return false;
  }

  ++index_; // Skip '('

  return get_name_and_type_params(params);
}

bool DataTypeClassNameParser::Parser::read_raw_arguments(std::string* args) {
  skip_blank();

  if (is_eos() || str_[index_] == ')' || str_[index_] == ',') {
    *args = "";
    return true;
  }

  if (str_[index_] != '(') {
    parse_error(str_, index_, "Expected '('");
    return false;
  }

  int i = index_;
  int open = 1;
  while (open > 0) {
    ++index_;

    if (is_eos()) {
      parse_error(str_, index_, "Expected ')'");
      return false;
    }

    if (str_[index_] == '(') {
      open++;
    } else if (str_[index_] == ')') {
      open--;
    }
  }

  ++index_; // Skip ')'
  *args = str_.substr(i, index_);
  return true;
}

void DataTypeClassNameParser::Parser::read_next_identifier(std::string* name) {
  size_t i = index_;
  while (!is_eos() && is_identifier_char(str_[index_]))
    ++index_;
  if (name != NULL) {
    if (index_ > i) {
      *name = str_.substr(i, index_ - i);
    } else{
      name->clear();
    }
  }
}

void DataTypeClassNameParser::Parser::parse_error(const std::string& str,
                                     size_t index,
                                     const char* error) {
  LOG_ERROR("Error parsing '%s' at %u index: %s",
            str.c_str(),
            static_cast<unsigned int>(index),
            error);
}

} // namespace cass
