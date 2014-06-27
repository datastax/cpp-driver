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

#ifndef __CASS_EXECUTE_REQUEST_HPP_INCLUDED__
#define __CASS_EXECUTE_REQUEST_HPP_INCLUDED__

#include <string>
#include <vector>

#include "statement.hpp"
#include "constants.hpp"
#include "prepared.hpp"

namespace cass {

class ExecuteRequest : public Statement {
public:
  ExecuteRequest(const Prepared* prepared, size_t value_count)
      : Statement(CQL_OPCODE_EXECUTE, value_count)
      , prepared_id_(prepared->id())
      , prepared_statement_(prepared->statement()) {}

  uint8_t kind() const {
    // used for batch statements
    return 1;
  }

  const char* statement() const { return prepared_id_.data(); }

  size_t statement_size() const { return prepared_id_.size(); }

  virtual void statement(const std::string& statement) {
    prepared_id_.assign(statement.begin(), statement.end());
  }

  void statement(const char* input, size_t size) {
    prepared_id_.assign(input, size);
  }

  const std::string prepared_id() const { return prepared_id_; }
  const std::string prepared_statement() const { return prepared_statement_; }

  bool encode(size_t reserved, char** output, size_t& size);

private:
  std::string prepared_id_;
  std::string prepared_statement_;
};

} // namespace cass

#endif
