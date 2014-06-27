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

#include "batch_request.hpp"
#include "cassandra.hpp"
#include "serialization.hpp"
#include "statement.hpp"
#include "execute_request.hpp"

extern "C" {

CassBatch* cass_batch_new(CassBatchType type) {
  cass::BatchRequest* batch = new cass::BatchRequest(type);
  batch->retain();
  return CassBatch::to(batch);
}

void cass_batch_free(CassBatch* batch) {
  batch->release();
}

void cass_batch_set_consistency(CassBatch* batch,
                                CassConsistency consistency) {
  batch->set_consistency(consistency);
}

CassError cass_batch_add_statement(CassBatch* batch, CassStatement* statement) {
  batch->add_statement(statement);
  return CASS_OK;
}

} // extern "C"

namespace cass {

BatchRequest::~BatchRequest() {
  for (StatementList::iterator it = statements_.begin(),
                               end = statements_.end();
       it != end; ++it) {
    (*it)->release();
  }
}

void BatchRequest::add_statement(Statement* statement) {
  if (statement->kind() == 1) {
    ExecuteRequest* execute_request = static_cast<ExecuteRequest*>(statement);
    prepared_statements_[execute_request->prepared_id()] = execute_request;
  }
  statement->retain();
  statements_.push_back(statement);
}

bool BatchRequest::prepared_statement(const std::string& id,
                                      std::string* statement) {
  PreparedMap::iterator it = prepared_statements_.find(id);
  if (it != prepared_statements_.end()) {
    *statement = it->second->prepared_statement();
    return true;
  }
  return false;
}

bool BatchRequest::encode(size_t reserved, char** output, size_t& size) {
  // reserved + type + length
  size = reserved + sizeof(uint8_t) + sizeof(uint16_t);

  for (StatementList::const_iterator it = statements_.begin(),
                                     end = statements_.end();
       it != end; ++it) {
    const Statement* statement = *it;

    size += sizeof(uint8_t);
    if (statement->kind() == 0) {
      size += sizeof(int32_t);
    } else {
      size += sizeof(uint16_t);
    }
    size += statement->statement_size();
    size += statement->encoded_values_size();
  }

  size += sizeof(uint16_t);
  *output = new char[size];

  char* buffer = encode_byte(*output + reserved, type_);
  buffer = encode_short(buffer, statements_.size());

  for (StatementList::const_iterator it = statements_.begin(),
                                     end = statements_.end();
       it != end; ++it) {
    const Statement* statement = *it;

    buffer = encode_byte(buffer, statement->kind());

    if (statement->kind() == 0) {
      buffer = encode_long_string(buffer, statement->statement(),
                                  statement->statement_size());
    } else {
      buffer = encode_string(buffer, statement->statement(),
                             statement->statement_size());
    }

    buffer = statement->encode_values(buffer);
  }

  encode_short(buffer, consistency_);

  return true;
}

} // namespace cass
