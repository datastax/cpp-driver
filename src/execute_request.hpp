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
      : Statement(CQL_OPCODE_EXECUTE, CASS_BATCH_KIND_PREPARED, value_count)
      , prepared_statement_(prepared->statement()) {
    set_prepared_id(prepared->id());
  }

  const std::string prepared_statement() const { return prepared_statement_; }

private:
  std::string prepared_statement_;
};

class ExecuteRequestMessage : public RequestMessage {
public:
  ExecuteRequestMessage(const Request* request)
    : RequestMessage(request) {}

  int32_t encode(int version, Writer::Bufs* bufs);
};


} // namespace cass

#endif
