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

#ifndef __CQL_BATCH_STATEMENT_HPP_INCLUDED__
#define __CQL_BATCH_STATEMENT_HPP_INCLUDED__

#include <list>
#include <string>
#include <utility>
#include <vector>
#include "cql_message_body.hpp"
#include "cql_serialization.hpp"
#include "cql_statement.hpp"

#define CQL_QUERY_FLAG_VALUES             0x01
#define CQL_QUERY_FLAG_SKIP_METADATA      0x02
#define CQL_QUERY_FLAG_PAGE_SIZE          0x04
#define CQL_QUERY_FLAG_PAGING_STATE       0x08
#define CQL_QUERY_FLAG_SERIAL_CONSISTENCY 0x10

struct CqlBatchStatement
    : public CqlMessageBody {
  typedef std::list<CqlStatement*> StatementCollection;

  uint8_t             type;
  StatementCollection statements;
  int16_t             consistency;

  explicit
  CqlBatchStatement(
      size_t consistency) :
      consistency(consistency)
  {}

  ~CqlBatchStatement() {
    for (CqlStatement* statement : statements) {
      delete statement;
    }
  }

  uint8_t
  opcode() const {
    return CQL_OPCODE_BATCH;
  }

  void
  add_statement(
      CqlStatement* statement) {
    statements.push_back(statement);
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
    // reserved + type + length
    size = reserved + sizeof(uint8_t) + sizeof(uint16_t);

    for (const CqlStatement* statement : statements) {
      size += sizeof(uint8_t);
      if (statement->kind() == 0) {
        size += sizeof(int32_t);
      } else {
        size += sizeof(int16_t);
      }
      size += statement->statement_size();

      for (const auto& value : *statement) {
        size += sizeof(int32_t);
        size += value.second;
      }
    }
    size    += sizeof(int16_t);
    *output  = new char[size];

    char* buffer = encode_byte(*output + reserved, type);
    buffer = encode_short(buffer, statements.size());

    for (const CqlStatement* statement : statements) {
      buffer = encode_byte(buffer, statement->kind());

      if (statement->kind() == 0) {
        buffer = encode_long_string(
            buffer,
            statement->statement(),
            statement->statement_size());
      } else {
        buffer = encode_string(
            buffer,
            statement->statement(),
            statement->statement_size());
      }

      buffer = encode_short(buffer, statement->size());
      for (const auto& value : *statement) {
        buffer = encode_long_string(buffer, value.first, value.second);
      }
    }
    encode_short(buffer, consistency);
    return true;
  }

 private:
  CqlBatchStatement(const CqlBatchStatement&) {}
  void operator=(const CqlBatchStatement&) {}
};

#endif
