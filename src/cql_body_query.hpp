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

#ifndef __QUERY_HPP_INCLUDED__
#define __QUERY_HPP_INCLUDED__

#include <list>
#include <string>
#include <utility>
#include <vector>
#include "cql_body.hpp"

#define CQL_QUERY_FLAG_VALUES             0x01
#define CQL_QUERY_FLAG_SKIP_METADATA      0x02
#define CQL_QUERY_FLAG_PAGE_SIZE          0x04
#define CQL_QUERY_FLAG_PAGING_STATE       0x08
#define CQL_QUERY_FLAG_SERIAL_CONSISTENCY 0x10

namespace cql {

class BodyQuery
    : public cql::Body {
 private:
  typedef std::pair<const char*, size_t> Value;
  typedef std::list<Value>               ValueCollection;

  std::string       query_;
  int16_t           consistency_;
  size_t            page_size_;
  bool              page_size_set_;
  std::vector<char> paging_state_;
  bool              serial_consistent_;
  int16_t           serial_consistency_;
  ValueCollection   values_;

 public:
  BodyQuery() :
      consistency_(CQL_CONSISTENCY_ANY),
      page_size_(0),
      page_size_set_(false),
      serial_consistent_(false),
      serial_consistency_(CQL_CONSISTENCY_SERIAL)
  {}

  uint8_t
  opcode() {
    return CQL_OPCODE_QUERY;
  }

  void
  query_string(
      const char* input) {
    query_.assign(input);
  }

  void
  query_string(
      const char* input,
      size_t      size) {
    query_.assign(input, size);
  }

  void
  query_string(
      const std::string& input) {
    query_ = input;
  }

  void
  page_size(
      size_t size) {
    page_size_set_ = true;
    page_size_     = size;
  }

  void
  paging_state(
      const char* state,
      size_t      size) {
    paging_state_.resize(size);
    paging_state_.assign(state, state + size);
  }

  void
  add_value(
      const char* value,
      size_t      size) {
    values_.push_back(std::make_pair(value, size));
  }

  void
  consistency(
      int16_t consistency) {
    consistency_ = consistency;
  }

  void
  serial_consistency(
      int16_t consistency) {
    serial_consistent_ = true;
    serial_consistency_ = consistency;
  }

  bool
  consume(
      char*  buffer,
      size_t size) {
    (void) buffer;
    (void) size;
    return true;
  }

  bool
  prepare(
      size_t reserved,
      char** output,
      size_t& size) {
    uint8_t  flags  = 0x00;
    // reserved + the long string
    size            = reserved + sizeof(int32_t) + query_.size();
    // consistency
    size           += sizeof(int16_t);
    // flags
    size           += 1;

    if (serial_consistent_) {
      flags |= CQL_QUERY_FLAG_SERIAL_CONSISTENCY;
      size  += sizeof(int16_t);
    }

    if (!paging_state_.empty()) {
      flags |= CQL_QUERY_FLAG_PAGING_STATE;
      size += (sizeof(int16_t) + paging_state_.size());
    }

    if (!values_.empty()) {
      size += sizeof(int16_t);
      for (ValueCollection::const_iterator it = values_.begin();
           it != values_.end();
           ++it) {
        size += (sizeof(int32_t) + it->second);
      }
      flags |= CQL_QUERY_FLAG_VALUES;
    }

    if (page_size_set_) {
      size += sizeof(int32_t);
      flags |= CQL_QUERY_FLAG_PAGE_SIZE;
    }

    if (serial_consistent_) {
      size += sizeof(int16_t);
      flags |= CQL_QUERY_FLAG_SERIAL_CONSISTENCY;
    }

    *output = new char[size];

    char* buffer = encode_long_string(
        *output + reserved,
        query_.c_str(),
        query_.size());

    buffer = encode_short(buffer, consistency_);
    buffer = encode_byte(buffer, flags);

    if (!values_.empty()) {
      buffer = encode_short(buffer, values_.size());
      for (ValueCollection::const_iterator it = values_.begin();
           it != values_.end();
           ++it) {
        buffer = encode_long_string(buffer, it->first, it->second);
      }
    }

    if (page_size_set_) {
      buffer = encode_int(buffer, page_size_);
    }

    if (!paging_state_.empty()) {
      buffer = encode_string(buffer, &paging_state_[0], paging_state_.size());
    }

    if (serial_consistent_) {
      buffer = encode_short(buffer, serial_consistency_);
    }

    return true;
  }

 private:
  BodyQuery(const BodyQuery&) {}
  void operator=(const BodyQuery&) {}
};
}
#endif
