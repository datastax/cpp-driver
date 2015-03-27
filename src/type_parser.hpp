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
#include "value.hpp"

#include <list>
#include <map>
#include <string>

namespace cass {

class TypeDescriptor {
public:
  TypeDescriptor(CassValueType type = CASS_VALUE_TYPE_UNKNOWN, bool is_reversed = false);
  TypeDescriptor(CassValueType type, bool is_reversed, std::list<TypeDescriptor>& type_arguments);

  size_t component_count() { return type_ == CASS_VALUE_TYPE_CUSTOM ? type_args_.size() : 1; }
  std::string to_string() const;

  CassValueType type_;
  bool is_reversed_;
  std::list<TypeDescriptor> type_args_;
};

class TypeParser {
public:
  static bool is_reversed(const std::string& class_name);

  static TypeDescriptor parse(const std::string& class_name);

  class TypeMapper {
  public:
    TypeMapper();
    CassValueType operator[](const std::string& cass_type) const;
  private:
    typedef std::map<std::string, CassValueType> NameTypeMap;
    NameTypeMap name_type_map_;
  };

private:
  TypeParser(const std::string& class_name, size_t start_index = 0);

  CassValueType parse_one_type(size_t hint = 0);
  TypeDescriptor parse_types(bool is_reversed = false);

  CassValueType parse_name();

  const static TypeMapper type_map_;
  const std::string& type_buffer_;
  std::string::size_type index_;
};

} // namespace cass

#endif
