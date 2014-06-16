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

#ifndef __CASS_BATCH_REQUEST_HPP_INCLUDED__
#define __CASS_BATCH_REQUEST_HPP_INCLUDED__

#include <list>
#include <string>
/*
  Copyright (c) 2014 DataStax

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

#include <utility>
#include <vector>

#include "common.hpp"
#include "message_body.hpp"
#include "serialization.hpp"
#include "statement.hpp"
#include "execute_request.hpp"

#define CASS_QUERY_FLAG_VALUES 0x01
#define CASS_QUERY_FLAG_SKIP_METADATA 0x02
#define CASS_QUERY_FLAG_PAGE_SIZE 0x04
#define CASS_QUERY_FLAG_PAGING_STATE 0x08
#define CASS_QUERY_FLAG_SERIAL_CONSISTENCY 0x10

namespace cass {

struct BatchRequest : public MessageBody {
  typedef std::list<Statement*> StatementCollection;
  typedef std::map<std::string, ExecuteRequest*> PreparedCollection;

  uint8_t type;
  StatementCollection statements;
  PreparedCollection prepared_statements;
  int16_t consistency;

  BatchRequest(size_t consistency, uint8_t type_)
      : MessageBody(CQL_OPCODE_BATCH)
      , type(type_)
      , consistency(consistency) {}

  ~BatchRequest() {
    for (Statement* statement : statements) {
      statement->release();
    }
  }

  void add_statement(Statement* statement) {
    if (statement->kind() == 1) {
      ExecuteRequest* execute_request = static_cast<ExecuteRequest*>(statement);
      prepared_statements[execute_request->prepared_id] = execute_request;
    }
    statement->retain();
    statements.push_back(statement);
  }

  bool prepared_statement(const std::string& id, std::string* statement) {
    auto it = prepared_statements.find(id);
    if (it != prepared_statements.end()) {
      *statement = it->second->prepared_statement;
      return true;
    }
    return false;
  }

  bool prepare(size_t reserved, char** output, size_t& size) {
    // reserved + type + length
    size = reserved + sizeof(uint8_t) + sizeof(uint16_t);

    for (const Statement* statement : statements) {
      size += sizeof(uint8_t);
      if (statement->kind() == 0) {
        size += sizeof(int32_t);
      } else {
        size += sizeof(uint16_t);
      }
      size += statement->statement_size();

      size += sizeof(uint16_t);
      for (const auto& value : *statement) {
        size += sizeof(int32_t);
        int32_t value_size = value.size();
        if (value_size > 0) {
          size += value_size;
        }
      }
    }
    size += sizeof(uint16_t);
    *output = new char[size];

    char* buffer = encode_byte(*output + reserved, type);
    buffer = encode_short(buffer, statements.size());

    for (const Statement* statement : statements) {
      buffer = encode_byte(buffer, statement->kind());

      if (statement->kind() == 0) {
        buffer = encode_long_string(buffer, statement->statement(),
                                    statement->statement_size());
      } else {
        buffer = encode_string(buffer, statement->statement(),
                               statement->statement_size());
      }

      buffer = encode_short(buffer, statement->values.size());
      for (const auto& value : *statement) {
        buffer = encode_bytes(buffer, value.data(), value.size());
      }
    }
    encode_short(buffer, consistency);
    return true;
  }
};

} // namespace cass

#endif
