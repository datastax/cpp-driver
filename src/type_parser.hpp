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

#ifndef __CASS_TYPE_PARSER_HPP_INCLUDED__
#define __CASS_TYPE_PARSER_HPP_INCLUDED__

#include "cassandra.h"
#include "data_type.hpp"
#include "ref_counted.hpp"
#include "value.hpp"

#include <map>
#include <string>
#include <vector>

namespace cass {

class ParseResult : public RefCounted<ParseResult> {
public:
  typedef std::vector<bool> ReversedVec;
  typedef std::map<std::string, SharedRefPtr<DataType> > CollectionMap;

  ParseResult(SharedRefPtr<DataType> type, bool reversed)
    : is_composite_(false) {
    types_.push_back(type);
    reversed_.push_back(reversed);
  }

  ParseResult(bool is_composite,
              const DataTypeVec& types,
              ReversedVec reversed,
              CollectionMap collections)
    : is_composite_(is_composite)
    , types_(types)
    , reversed_(reversed)
    , collections_(collections) { }

  bool is_composite() const { return is_composite_; }
  const DataTypeVec& types() const { return types_; }
  const ReversedVec& reversed() const  { return reversed_; }
  const CollectionMap& collections() const  { return collections_; }

private:
  bool is_composite_;
  DataTypeVec types_;
  ReversedVec reversed_;
  CollectionMap collections_;
};

class TypeParser {
public:
  static bool is_reversed(const std::string& type);
  static bool is_frozen(const std::string& type);
  static bool is_composite(const std::string& type);
  static bool is_collection(const std::string& type);

  static bool is_user_type(const std::string& type);
  static bool is_tuple_type(const std::string& type);

  static SharedRefPtr<DataType> parse_one(const std::string& type);
  static SharedRefPtr<ParseResult> parse_with_composite(const std::string& type);

private:
  static bool get_nested_class_name(const std::string& type, std::string* class_name);

  typedef std::vector<std::string> TypeParamsVec;
  typedef std::vector<std::pair<std::string, std::string> > NameAndTypeParamsVec;

  class Parser {
  public:
    Parser(const std::string& str, size_t index)
      : str_(str)
      , index_(index) {}

    void skip() { ++index_; }
    void skip_blank();
    bool skip_blank_and_comma();

    bool read_one(std::string* name_and_args);
    void get_next_name(std::string* name = NULL);
    bool get_type_params(TypeParamsVec* params);
    bool get_name_and_type_params(NameAndTypeParamsVec* params);
    bool get_collection_params(NameAndTypeParamsVec* params);

  private:
    bool read_raw_arguments(std::string* args);
    void read_next_identifier(std::string* name);

    static void parse_error(const std::string& str,
                            size_t index,
                            const char* error);

    bool is_eos() const {
      return index_ >= str_.length();
    }

    static bool is_identifier_char(int c) {
      return (c >= '0' && c <= '9')
          || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
          || c == '-' || c == '+' || c == '.' || c == '_' || c == '&';
    }

    static bool is_blank(int c) {
      return c == ' ' || c == '\t' || c == '\n';
    }

  private:
    const std::string str_;
    size_t index_;
  };

  TypeParser();
};

} // namespace cass

#endif
