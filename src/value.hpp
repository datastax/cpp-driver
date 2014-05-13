/*
  Copyright 2014 DataStax

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

#include <stdint.h>

#include <vector>
#include <map>
#include <set>

#include "common.hpp"

namespace cass {

class Map;
class List;
class Set;

struct Decimal {
  int32_t scale;
  std::string* varint;
};

struct Inet {
  uint8_t address[16];
  uint8_t address_len;
  int32_t port;
};

union Basic {
    float flt;
    double dbl;
    bool bln;
    int32_t i32;
    int64_t i64; // Counter, Timestmap
    uint8_t uuid[16]; // Uuid, Timeuuid
    Inet inet;
    Decimal decimal;
    std::string* bytes; // Blob, Custom, Ascii, Varchar, Varint
};

union Collection {
    Map* map;
    Set* set;
    List* list;
};

class Value {
  public:
    enum Type {
      NONE = 0,
      FLOAT,
      DOUBLE,
      BOOL,
      INT32,
      INT64,
      UUID,
      INET,
      BYTES,
      DECIMAL,
      LIST,
      MAP,
      SET,
      UDT,
    };

    static const size_t FLOAT_SIZE = 4;
    static const size_t DOUBLE_SIZE = 8;
    static const size_t BOOL_SIZE = 1;
    static const size_t INT32_SIZE = 4;
    static const size_t INT64_SIZE = 8;
    static const size_t UUID_SIZE = 16;

    Value()
      : type_(NONE) { }

    Value(const Value& v)
      : type_(v.type_) {
      if(type_ == BYTES) {
        basic().bytes = new std::string(*v.basic().bytes);
      } else if(type_ == DECIMAL) {
        basic().decimal.scale = v.basic().decimal.scale;
        basic().decimal.varint = new std::string(*v.basic().decimal.varint);
      } else {
        data_ = v.data_;
      }
    }

    ~Value() {
      if(type_ == BYTES) {
        delete basic().bytes ;
      } else if(type_ == DECIMAL) {
        delete basic().decimal.varint;
      }
    }

#define BASIC_TYPE(Name, Type, DeclType)                \
    static Value create_##Name(const DeclType& input) { \
      Value v(Type);                                    \
      v.basic().Name = input;                           \
      return v;                                         \
    }                                                   \
    DeclType as_##Name() const { return basic().Name; }

    BASIC_TYPE(flt, FLOAT, float)
    BASIC_TYPE(dbl, DOUBLE, double)
    BASIC_TYPE(bln, BOOL, bool)
    BASIC_TYPE(i32, INT32, int32_t)
    BASIC_TYPE(i64, INT64, int64_t)
#undef BASIC_TYPE

    static Value create_bytes(const char* data, size_t size) {
      Value v(BYTES);
      v.basic().bytes = new std::string(data, size);
      return v;
    }
    const std::string* as_bytes() const { return basic().bytes; }

    static Value create_uuid(const uint8_t* uuid) {
      Value v(UUID);
      memcpy(v.basic().uuid, uuid, sizeof(UUID_SIZE));
      return v;
    }
    const uint8_t* as_uuid() const { return basic().uuid; }

    static Value create_decimal(int32_t scale, const char* data, size_t size) {
      Value v(BYTES);
      v.basic().decimal.scale = scale;
      v.basic().decimal.varint = new std::string(data, size);
      return v;
    }
    const Decimal& as_decimal() const { return basic().decimal; }

    static Value create_inet(const uint8_t* address, uint8_t address_len, int32_t port) {
      Value v(INET);
      memcpy(v.basic().inet.address, address, address_len);
      v.basic().inet.address_len = address_len;
      v.basic().inet.port = port;
      return v;
    }
    const Inet& as_inet() const { return basic().inet; }

    size_t get_size() const {
      switch(type_) {
        case FLOAT: return FLOAT_SIZE;
        case DOUBLE: return DOUBLE_SIZE;
        case BOOL: return BOOL_SIZE;
        case INT32: return INT32_SIZE;
        case INT64: return INT64_SIZE;
        case UUID: return UUID_SIZE;
        case INET: return 1 + as_inet().address_len + INT32_SIZE;
        case BYTES: return as_bytes()->size();
        case DECIMAL: return INT32_SIZE + as_decimal().varint->size();
        case LIST: assert(false);
        case MAP: assert(false);
        case SET: assert(false);
        case UDT: assert(false);
        default: assert(false);
      }
    }

    char* encode(char* output) const;

    static Value decode(char* input, Value* value) {
      return Value(NONE);
    }

private:
    Value(Type type)
      : type_(type) { }

    inline Basic& basic() { return data_.basic; }
    inline const Basic& basic() const { return data_.basic; }

    Type type_;
    union {
        Basic basic;
        Collection collection;
    } data_;
};

class List {
  public:
    Value::Type value_type;
    std::vector<Basic> data_;
};

// TODO(mpenick): Custom comparators
class Map {
  public:
    Value::Type key_type;
    Value::Type value_type;
    std::map<Basic, Basic> data_;
};

class Set {
  public:
    Value::Type value_type;
    std::set<Basic> data_;
};

}

#endif
