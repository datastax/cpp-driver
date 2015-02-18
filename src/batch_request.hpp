/*
  Copyright (c) 2014-2015 DataStax

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
#include "request.hpp"
#include "ref_counted.hpp"

#include <list>
#include <map>
#include <string>

namespace cass {

class Statement;
class ExecuteRequest;

class BatchRequest : public RoutableRequest {
public:
  typedef std::list<SharedRefPtr<Statement> > StatementList;

  BatchRequest(uint8_t type_)
      : RoutableRequest(CQL_OPCODE_BATCH)
      , type_(type_)
      , consistency_(CASS_CONSISTENCY_ONE) {}

  uint8_t type() const { return type_; }

  const StatementList& statements() const { return statements_; }

  int16_t consistency() const { return consistency_; }

  void set_consistency(int16_t consistency) { consistency_ = consistency; }

  void add_statement(Statement* statement);

  bool prepared_statement(const std::string& id, std::string* statement) const;

  virtual bool get_routing_key(std::string* routing_key) const;

private:
  int encode(int version, BufferVec* bufs) const;
  int encode_v2(BufferVec* bufs) const;

private:
  typedef std::map<std::string, ExecuteRequest*> PreparedMap;

  uint8_t type_;
  StatementList statements_;
  int16_t consistency_;
  PreparedMap prepared_statements_;
};

} // namespace cass

#endif
