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

#ifndef __CASS_QUERY_REQUEST_HPP_INCLUDED__
#define __CASS_QUERY_REQUEST_HPP_INCLUDED__

#include <list>
#include <string>
#include <utility>
#include <vector>
#include "statement.hpp"
#include "message_body.hpp"

#define CASS_QUERY_FLAG_VALUES 0x01
#define CASS_QUERY_FLAG_SKIP_METADATA 0x02
#define CASS_QUERY_FLAG_PAGE_SIZE 0x04
#define CASS_QUERY_FLAG_PAGING_STATE 0x08
#define CASS_QUERY_FLAG_SERIAL_CONSISTENCY 0x10

namespace cass {

struct QueryRequest : public Statement {
  std::string query;
  int16_t consistency_value;
  int page_size;
  std::vector<char> paging_state;
  int16_t serial_consistency_value;

public:
  QueryRequest(const char* statement, size_t statement_length,
               size_t value_count, CassConsistency consistency)
      : Statement(CQL_OPCODE_QUERY, value_count)
      , query(statement, statement_length)
      , consistency_value(consistency)
      , page_size(-1)
      , serial_consistency_value(CASS_CONSISTENCY_ANY) {}

  QueryRequest(size_t value_count, CassConsistency consistency)
      : Statement(CQL_OPCODE_QUERY, value_count)
      , consistency_value(consistency)
      , page_size(-1)
      , serial_consistency_value(CASS_CONSISTENCY_ANY) {}

  QueryRequest()
      : Statement(CQL_OPCODE_QUERY)
      , consistency_value(CASS_CONSISTENCY_ANY)
      , page_size(-1)
      , serial_consistency_value(CASS_CONSISTENCY_ANY) {}

  uint8_t kind() const {
    // used for batch statements
    return 0;
  }

  void consistency(int16_t consistency) { consistency_value = consistency; }

  int16_t consistency() const { return consistency_value; }

  void serial_consistency(int16_t serial_consistency) {
    serial_consistency_value = serial_consistency;
  }

  int16_t serial_consistency() const { return serial_consistency_value; }

  const char* statement() const { return &query[0]; }

  size_t statement_size() const { return query.size(); }

  virtual void statement(const std::string& statement) {
    query.assign(statement.begin(), statement.end());
  }

  void statement(const char* input, size_t size) { query.assign(input, size); }

  bool prepare(size_t reserved, char** output, size_t& size) {
    uint8_t flags = 0x00;
    // reserved + the long string
    size = reserved + sizeof(int32_t) + query.size();
    // consistency_value
    size += sizeof(int16_t);
    // flags
    size += 1;

    if (serial_consistency_value != 0) {
      flags |= CASS_QUERY_FLAG_SERIAL_CONSISTENCY;
      size += sizeof(int16_t);
    }

    if (!paging_state.empty()) {
      flags |= CASS_QUERY_FLAG_PAGING_STATE;
      size += (sizeof(int16_t) + paging_state.size());
    }

    if (!values.empty()) {
      size += sizeof(int16_t);
      for (const auto& value : values) {
        size += sizeof(int32_t);
        int32_t value_size = value.size();
        if (value_size > 0) {
          size += value_size;
        }
      }
      flags |= CASS_QUERY_FLAG_VALUES;
    }

    if (page_size >= 0) {
      size += sizeof(int32_t);
      flags |= CASS_QUERY_FLAG_PAGE_SIZE;
    }

    if (serial_consistency_value != 0) {
      size += sizeof(int16_t);
      flags |= CASS_QUERY_FLAG_SERIAL_CONSISTENCY;
    }

    *output = new char[size];

    char* buffer =
        encode_long_string(*output + reserved, query.c_str(), query.size());

    buffer = encode_short(buffer, consistency_value);
    buffer = encode_byte(buffer, flags);

    if (!values.empty()) {
      buffer = encode_short(buffer, values.size());
      for (const auto& value : values) {
        buffer = encode_bytes(buffer, value.data(), value.size());
      }
    }

    if (page_size >= 0) {
      buffer = encode_int(buffer, page_size);
    }

    if (!paging_state.empty()) {
      buffer = encode_string(buffer, &paging_state[0], paging_state.size());
    }

    if (serial_consistency_value != 0) {
      buffer = encode_short(buffer, serial_consistency_value);
    }

    return true;
  }
};

} // namespace cass

#endif
