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

#include "statement.hpp"
#include "constants.hpp"

#include <string>
#include <vector>

#define CASS_QUERY_FLAG_VALUES 0x01
#define CASS_QUERY_FLAG_SKIP_METADATA 0x02
#define CASS_QUERY_FLAG_PAGE_SIZE 0x04
#define CASS_QUERY_FLAG_PAGING_STATE 0x08
#define CASS_QUERY_FLAG_SERIAL_CONSISTENCY 0x10

namespace cass {

class QueryRequest : public Statement {
public:
  QueryRequest()
    : Statement(CQL_OPCODE_QUERY) {}

  QueryRequest(size_t value_count)
    : Statement(CQL_OPCODE_QUERY, value_count) {}

  QueryRequest(const char* statement, size_t statement_length,
               size_t value_count)
      : Statement(CQL_OPCODE_QUERY, value_count)
      , query_(statement, statement_length) {}

  uint8_t kind() const {
    // used for batch statements
    return 0;
  }

  const char* statement() const { return &query_[0]; }

  size_t statement_size() const { return query_.size(); }

  virtual void statement(const std::string& statement) {
    query_.assign(statement.begin(), statement.end());
  }

  void statement(const char* input, size_t size) { query_.assign(input, size); }


  int32_t encode(int version, BufferVec* bufs);

  bool encode(size_t reserved, char** output, size_t& size);

private:
  std::string query_;
};

} // namespace cass

#endif
