/*
  Copyright (c) DataStax, Inc.

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

#include "cassandra.h"
#include "constants.hpp"
#include "external.hpp"
#include "request.hpp"
#include "ref_counted.hpp"
#include "statement.hpp"

#include <map>
#include <string>
#include <vector>

namespace cass {

class ExecuteRequest;

class BatchRequest : public RoutableRequest {
public:
  typedef std::vector<Statement::Ptr> StatementVec;

  BatchRequest(uint8_t type_)
      : RoutableRequest(CQL_OPCODE_BATCH)
      , type_(type_) { }

  uint8_t type() const { return type_; }

  const StatementVec& statements() const { return statements_; }

  void add_statement(Statement* statement);

  bool find_prepared_query(const std::string& id, std::string* query) const;

  virtual bool get_routing_key(std::string* routing_key) const;

private:
  int encode(int version, RequestCallback* callback, BufferVec* bufs) const;

private:
  uint8_t type_;
  StatementVec statements_;
};

} // namespace cass

EXTERNAL_TYPE(cass::BatchRequest, CassBatch)

#endif
