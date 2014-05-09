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

#ifndef __CASS_STATEMENT_HPP_INCLUDED__
#define __CASS_STATEMENT_HPP_INCLUDED__

#include <list>
#include <utility>

#include "message_body.hpp"

#define CASS_VALUE_CHECK_INDEX(i)                                        \
  if (index >= size()) {                                                \
    return CASS_ERROR_LIB_BAD_PARAMS;                                    \
  }

namespace cass {

struct Statement : public MessageBody {
  typedef std::pair<char*, size_t>        Value;
  typedef std::vector<Value>              ValueCollection;
  typedef ValueCollection::iterator       ValueIterator;
  typedef ValueCollection::const_iterator ConstValueIterator;

  ValueCollection values;

  virtual
  ~Statement() {}

  virtual uint8_t
  kind() const = 0;

  virtual void
  statement(
      const std::string& statement) = 0;

  virtual void
  statement(
      const char* statement,
      size_t      size) = 0;

  virtual const char*
  statement() const = 0;

  virtual size_t
  statement_size() const = 0;

  virtual void
  consistency(
      int16_t consistency) = 0;

  virtual int16_t
  consistency() const = 0;

  virtual int16_t
  serial_consistency() const = 0;

  virtual void
  serial_consistency(
      int16_t consistency) = 0;

  inline void
  resize(
      size_t size) {
    values.resize(size);
  }

  inline size_t
  size() const {
    return values.size();
  }

  inline int
  bind(
      size_t  index,
      int16_t value) {
    CASS_VALUE_CHECK_INDEX(index);
    Value pair = std::make_pair(new char[sizeof(int16_t)], sizeof(int16_t));
    encode_short(pair.first, value);
    values[index] = pair;
    return CASS_ERROR_NO_ERROR;
  }

  inline int
  bind(
      size_t  index,
      int32_t value) {
    CASS_VALUE_CHECK_INDEX(index);
    Value pair = std::make_pair(new char[sizeof(int32_t)], sizeof(int32_t));
    encode_int(pair.first, value);
    values[index] = pair;
    return CASS_ERROR_NO_ERROR;
  }

  inline int
  bind(
      size_t  index,
      int64_t value) {
    CASS_VALUE_CHECK_INDEX(index);
    Value pair = std::make_pair(new char[sizeof(int64_t)], sizeof(int64_t));
    encode_int64(pair.first, value);
    values[index] = pair;
    return CASS_ERROR_NO_ERROR;
  }

  inline int
  bind(
      size_t index,
      char*  input,
      size_t input_length) {
    CASS_VALUE_CHECK_INDEX(index);
    Value pair = std::make_pair(new char[input_length], input_length);
    memcpy(pair.first, input, input_length);
    values[index] = pair;
    return CASS_ERROR_NO_ERROR;
  }

  inline int
  bind(
      size_t index,
      float  value) {
    CASS_VALUE_CHECK_INDEX(index);
    Value pair = std::make_pair(new char[sizeof(float)], sizeof(float));
    encode_int(pair.first, static_cast<float>(value));
    values[index] = pair;
    return CASS_ERROR_NO_ERROR;
  }

  inline int
  bind(
      size_t index,
      double value) {
    CASS_VALUE_CHECK_INDEX(index);
    Value pair = std::make_pair(new char[sizeof(double)], sizeof(double));
    encode_int64(pair.first, static_cast<int64_t>(value));
    values[index] = pair;
    return CASS_ERROR_NO_ERROR;
  }

  inline int
  bind(
      size_t index,
      bool   value) {
    CASS_VALUE_CHECK_INDEX(index);
    Value pair = std::make_pair(new char[sizeof(uint8_t)], sizeof(uint8_t));
    encode_byte(pair.first, value ? 0x01 : 0x00);
    values[index] = pair;
    return CASS_ERROR_NO_ERROR;
  }

  inline ValueIterator
  begin() {
    return values.begin();
  }

  inline ValueIterator
  end() {
    return values.end();
  }

  inline ConstValueIterator
  begin() const {
    return values.begin();
  }

  inline ConstValueIterator
  end() const {
    return values.end();
  }
};

} // namespace cass

#endif
