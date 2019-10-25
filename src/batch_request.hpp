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

#ifndef DATASTAX_INTERNAL_BATCH_REQUEST_HPP
#define DATASTAX_INTERNAL_BATCH_REQUEST_HPP

#include "cassandra.h"
#include "constants.hpp"
#include "external.hpp"
#include "map.hpp"
#include "ref_counted.hpp"
#include "request.hpp"
#include "statement.hpp"
#include "string.hpp"
#include "vector.hpp"

namespace datastax { namespace internal { namespace core {

class ExecuteRequest;

class BatchRequest : public RoutableRequest {
public:
  typedef SharedRefPtr<BatchRequest> Ptr;
  typedef Vector<Statement::Ptr> StatementVec;

  BatchRequest(uint8_t type)
      : RoutableRequest(CQL_OPCODE_BATCH)
      , type_(type) {}

  uint8_t type() const { return type_; }

  const StatementVec& statements() const { return statements_; }

  void add_statement(Statement* statement);

  bool find_prepared_query(const String& id, String* query) const;

  virtual bool get_routing_key(String* routing_key) const;

private:
  int encode(ProtocolVersion version, RequestCallback* callback, BufferVec* bufs) const;

private:
  uint8_t type_;
  StatementVec statements_;
};

}}} // namespace datastax::internal::core

EXTERNAL_TYPE(datastax::internal::core::BatchRequest, CassBatch)

#endif
