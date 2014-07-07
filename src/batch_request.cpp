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

ssize_t BatchRequest::encode(int version, BufferVec* bufs) const {
  if(version != 2) {
    return ENCODE_ERROR_UNSUPPORTED_PROTOCOL;
  }
  return encode_v2(bufs);
}

ssize_t BatchRequest::encode_v2(BufferVec* bufs) const {
  const int version = 2;

  size_t length = 0;

  {
    // <type> [byte] + <n> [short]
    size_t buf_size = sizeof(uint8_t) + sizeof(uint16_t);

    Buffer buf(buf_size);
    size_t pos = buf.encode_byte(0, type_);
    buf.encode_uint16(pos, statements().size());

    bufs->push_back(buf);
    length += buf_size;
  }

  for (BatchRequest::StatementList::const_iterator
       it = statements_.begin(),
       end = statements_.end();
       it != end; ++it) {
    const Statement* statement = *it;

    // <kind> [byte]
    ssize_t buf_size = sizeof(uint8_t);

    // <string_or_id> [long string] | [short bytes]
    buf_size += (statement->kind() == CASS_BATCH_KIND_QUERY) ? sizeof(int32_t) : sizeof(uint16_t);
    buf_size += statement->query().size();

    // <n><value_1>...<value_n>
    buf_size += sizeof(uint16_t); // <n> [short]

    bufs->push_back(Buffer(buf_size));
    length += buf_size;

    Buffer& buf = bufs->back();
    size_t pos = buf.encode_byte(0, statement->kind());

    if(statement->kind() == CASS_BATCH_KIND_QUERY) {
      pos = buf.encode_long_string(pos,
                                 statement->query().data(),
                                 statement->query().size());
    } else {
      pos = buf.encode_string(pos,
                              statement->query().data(),
                              statement->query().size());
    }

    buf.encode_uint16(pos, statement->values_count());
    if(statement->values_count() > 0) {
      length += statement->encode_values(version, bufs);
    }
  }

  {
    // <consistency> [short]
    size_t buf_size = sizeof(uint16_t);

    Buffer buf(buf_size);
    buf.encode_uint16(0, consistency_);
    bufs->push_back(buf);
    length += buf_size;
  }

  return length;
}

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
                                      std::string* statement) const {
  PreparedMap::const_iterator it = prepared_statements_.find(id);
  if (it != prepared_statements_.end()) {
    *statement = it->second->prepared_statement();
    return true;
  }
  return false;
}

} // namespace cass
