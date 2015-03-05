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

#include "type_parser.hpp"

#include "common.hpp"
#include "scoped_ptr.hpp"

#include <sstream>

#define REVERSED_TYPE "org.apache.cassandra.db.marshal.ReversedType("

namespace cass {

TypeDescriptor::TypeDescriptor(CassValueType type, bool is_reversed)
  : type_(type)
  , is_reversed_(is_reversed) {}

TypeDescriptor::TypeDescriptor(CassValueType type, bool is_reversed, std::list<TypeDescriptor>& type_arguments)
  : type_(type)
  , is_reversed_(is_reversed)
  , type_args_(type_arguments) {}

std::string TypeDescriptor::to_string() const {
  std::stringstream ss;
  if (is_reversed_) ss << "reversed(";
  ss << type_;
  if (!type_args_.empty()) {
    ss << '(';
    for (std::list<TypeDescriptor>::const_iterator i = type_args_.begin(); i != type_args_.end(); ++i) {
      ss << i->to_string() << ',';
    }
    ss << ')';
  }
  if (is_reversed_) ss << ')';
  return ss.str();
}

TypeParser::TypeMapper::TypeMapper() {
  name_type_map_["org.apache.cassandra.db.marshal.AsciiType"] = CASS_VALUE_TYPE_ASCII;
  name_type_map_["org.apache.cassandra.db.marshal.LongType"] = CASS_VALUE_TYPE_BIGINT;
  name_type_map_["org.apache.cassandra.db.marshal.BytesType"] = CASS_VALUE_TYPE_BLOB;
  name_type_map_["org.apache.cassandra.db.marshal.BooleanType"] = CASS_VALUE_TYPE_BOOLEAN;
  name_type_map_["org.apache.cassandra.db.marshal.CounterColumnType"] = CASS_VALUE_TYPE_COUNTER;
  name_type_map_["org.apache.cassandra.db.marshal.DecimalType"] = CASS_VALUE_TYPE_DECIMAL;
  name_type_map_["org.apache.cassandra.db.marshal.DoubleType"] = CASS_VALUE_TYPE_DOUBLE;
  name_type_map_["org.apache.cassandra.db.marshal.FloatType"] = CASS_VALUE_TYPE_FLOAT;
  name_type_map_["org.apache.cassandra.db.marshal.InetAddressType"] = CASS_VALUE_TYPE_INET;
  name_type_map_["org.apache.cassandra.db.marshal.Int32Type"] = CASS_VALUE_TYPE_INT;
  name_type_map_["org.apache.cassandra.db.marshal.UTF8Type"] = CASS_VALUE_TYPE_TEXT;
  name_type_map_["org.apache.cassandra.db.marshal.TimestampType"] = CASS_VALUE_TYPE_TIMESTAMP;
  name_type_map_["org.apache.cassandra.db.marshal.DateType"] = CASS_VALUE_TYPE_TIMESTAMP;
  name_type_map_["org.apache.cassandra.db.marshal.UUIDType"] = CASS_VALUE_TYPE_UUID;
  name_type_map_["org.apache.cassandra.db.marshal.IntegerType"] = CASS_VALUE_TYPE_INT;
  name_type_map_["org.apache.cassandra.db.marshal.TimeUUIDType"] = CASS_VALUE_TYPE_TIMEUUID;
  name_type_map_["org.apache.cassandra.db.marshal.ListType"] = CASS_VALUE_TYPE_LIST;
  name_type_map_["org.apache.cassandra.db.marshal.MapType"] = CASS_VALUE_TYPE_MAP;
  name_type_map_["org.apache.cassandra.db.marshal.SetType"] = CASS_VALUE_TYPE_SET;
  name_type_map_["org.apache.cassandra.db.marshal.CompositeType"] = CASS_VALUE_TYPE_CUSTOM;
}

CassValueType TypeParser::TypeMapper::operator [](const std::string& type_name) const {
  NameTypeMap::const_iterator itr = name_type_map_.find(type_name);
  if (itr != name_type_map_.end()) {
    return itr->second;
  } else {
    return CASS_VALUE_TYPE_UNKNOWN;
  }
}

TypeParser::TypeParser(const std::string& class_name, size_t start_index)
  : type_buffer_(class_name)
  , index_(start_index) {}

bool TypeParser::is_reversed(const std::string& class_name) {
  return starts_with(class_name, REVERSED_TYPE);
}

TypeDescriptor TypeParser::parse(const std::string& class_name) {

  bool reversed = is_reversed(class_name);

  size_t start = reversed ? strlen(REVERSED_TYPE) : 0;
  ScopedPtr<TypeParser> parser(new TypeParser(class_name, start));

  return parser->parse_types(reversed);
}

CassValueType TypeParser::parse_one_type(size_t hint) {
  size_t bound = type_buffer_.size();
  if (hint == 0) {
    hint = type_buffer_.find_first_of(",()", index_);
  }
  if (hint != std::string::npos) {
    bound = hint;
  }
  size_t len = bound - index_;
  CassValueType t = type_map_[type_buffer_.substr(index_, len)];
  index_ += len;
  return t;
}

TypeDescriptor TypeParser::parse_types(bool is_reversed) {

  CassValueType value_type = parse_one_type();
  std::list<TypeDescriptor> type_args;

  bool list_open = false;
  size_t i;

  while (index_ < type_buffer_.size() &&
        (i = type_buffer_.find_first_of(",() ", index_)) != std::string::npos) {
    switch (type_buffer_[i]) {
      case ' ':
        ++index_;
        break;
      case ',':
        if (list_open) {
          CassValueType inner_type = parse_one_type(i);
          type_args.push_back(TypeDescriptor(inner_type));
        }
        ++index_;
        break;
      case '(':
        list_open = true;
        ++index_;
        type_args.push_back(parse_types(false));
        break;
      case ')':
      {
        CassValueType inner_type = parse_one_type(i);
        type_args.push_back(TypeDescriptor(inner_type));
      }
        list_open = false;
        ++index_;
        break;
      default: // unexpected state
        index_ = type_buffer_.size();
        break;
    }
    if (!list_open) {
      break;
    }
  }
  return TypeDescriptor(value_type, is_reversed, type_args);
}

const TypeParser::TypeMapper TypeParser::type_map_;

} // namespace cass
