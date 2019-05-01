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

#ifndef DATASTAX_INTERNAL_TYPE_PARSER_HPP
#define DATASTAX_INTERNAL_TYPE_PARSER_HPP

#include "cassandra.h"
#include "data_type.hpp"
#include "map.hpp"
#include "metadata.hpp"
#include "ref_counted.hpp"
#include "string.hpp"
#include "value.hpp"
#include "vector.hpp"

#define EMPTY_TYPE "org.apache.cassandra.db.marshal.EmptyType"

namespace datastax { namespace internal { namespace core {

class ParserBase {
public:
  ParserBase(const String& str, size_t index)
      : str_(str)
      , index_(index) {}

  void skip() { ++index_; }

  void skip_blank() {
    while (!is_eos() && is_blank(str_[index_])) {
      ++index_;
    }
  }

  bool skip_blank_and_comma() {
    bool comma_found = false;
    while (!is_eos()) {
      int c = str_[index_];
      if (c == ',') {
        if (comma_found) {
          return true;
        } else {
          comma_found = true;
        }
      } else if (!is_blank(c)) {
        return true;
      }
      ++index_;
    }
    return false;
  }

  bool is_eos() const { return index_ >= str_.length(); }

  static bool is_identifier_char(int c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '-' ||
           c == '+' || c == '.' || c == '_' || c == '&';
  }

  static bool is_blank(int c) { return c == ' ' || c == '\t' || c == '\n'; }

protected:
  const String str_;
  size_t index_;
};

class DataTypeCqlNameParser {
public:
  static DataType::ConstPtr parse(const String& type, SimpleDataTypeCache& cache,
                                  KeyspaceMetadata* keyspace, bool is_frozen = false);

private:
  class Parser : public ParserBase {
  public:
    typedef Vector<String> TypeParamsVec;

    Parser(const String& str, size_t index)
        : ParserBase(str, index) {}

    void parse_type_name(String* name);
    void parse_type_parameters(TypeParamsVec* params);

  private:
    void read_next_identifier(String* name);
    bool read_raw_type_parameters(String* param);
  };
};

class ParseResult : public RefCounted<ParseResult> {
public:
  typedef SharedRefPtr<ParseResult> Ptr;
  typedef Vector<bool> ReversedVec;
  typedef Map<String, DataType::ConstPtr> CollectionMap;

  ParseResult(DataType::ConstPtr type, bool reversed)
      : is_composite_(false) {
    types_.push_back(type);
    reversed_.push_back(reversed);
  }

  ParseResult(bool is_composite, const DataType::Vec& types, ReversedVec reversed,
              CollectionMap collections)
      : is_composite_(is_composite)
      , types_(types)
      , reversed_(reversed)
      , collections_(collections) {}

  bool is_composite() const { return is_composite_; }
  const DataType::Vec& types() const { return types_; }
  const ReversedVec& reversed() const { return reversed_; }
  const CollectionMap& collections() const { return collections_; }

private:
  bool is_composite_;
  DataType::Vec types_;
  ReversedVec reversed_;
  CollectionMap collections_;
};

class DataTypeClassNameParser {
public:
  static bool is_reversed(const String& type);
  static bool is_frozen(const String& type);
  static bool is_composite(const String& type);
  static bool is_collection(const String& type);

  static bool is_user_type(const String& type);
  static bool is_tuple_type(const String& type);

  static DataType::ConstPtr parse_one(const String& type, SimpleDataTypeCache& cache);
  static ParseResult::Ptr parse_with_composite(const String& type, SimpleDataTypeCache& cache);

private:
  static bool get_nested_class_name(const String& type, String* class_name);

  typedef Vector<String> TypeParamsVec;
  typedef Vector<std::pair<String, String> > NameAndTypeParamsVec;

  class Parser : public ParserBase {
  public:
    Parser(const String& str, size_t index)
        : ParserBase(str, index) {}

    bool read_one(String* name_and_args);
    void get_next_name(String* name = NULL);
    bool get_type_params(TypeParamsVec* params);
    bool get_name_and_type_params(NameAndTypeParamsVec* params);
    bool get_collection_params(NameAndTypeParamsVec* params);

  private:
    bool read_raw_arguments(String* args);
    void read_next_identifier(String* name);

    static void parse_error(const String& str, size_t index, const char* error);
  };

  DataTypeClassNameParser();
};

}}} // namespace datastax::internal::core

#endif
