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


int32_t BatchRequestMessage::encode(int version, Writer::Bufs* bufs) {
  assert(version == 2);

  int32_t length = 0;

  const BatchRequest* batch = static_cast<const BatchRequest*>(request());

  {
    // <type> [byte] + <n> [short]
    int32_t buf_size = 1 + 2;

    body_head_buf_ = Buffer(buf_size);

    length += buf_size;

    bufs->push_back(uv_buf_init(body_head_buf_.data(), body_head_buf_.size()));

    char* pos = encode_byte(body_head_buf_.data(), batch->type());
    pos = encode_short(pos, batch->statements().size());
  }

  for (BatchRequest::StatementList::const_iterator
       it = batch->statements().begin(),
       end = batch->statements().end();
       it != end; ++it) {
    const Statement* statement = *it;

    // <kind> [byte]
    int32_t buf_size = 1;

    // <string_or_id> [long string] | [short bytes]
    buf_size += (statement->kind() == CASS_BATCH_KIND_QUERY) ? 4 : 2;
    buf_size += statement->query().size();

    // <n><value_1>...<value_n>
    buf_size += 2; // <n> [short]

    statement_bufs_.push_back(Buffer(buf_size));

    Buffer* buf = &statement_bufs_.back();

    length += buf_size;

    bufs->push_back(uv_buf_init(buf->data(), buf->size()));

    char* pos = encode_byte(buf->data(), statement->kind());

    if(statement->kind() == CASS_BATCH_KIND_QUERY) {
      pos = encode_long_string(pos, statement->query().data(),
                               statement->query().size());
    } else {
      pos = encode_string(pos, statement->query().data(),
                          statement->query().size());
    }

    if(statement->has_values()) {
      encode_short(pos, statement->values_count());
      length += statement->encode_values(version, &body_collection_bufs_, bufs);
    }
  }

  {
    // <consistency> [short]
    int32_t buf_size = 2;

    body_tail_buf_ = Buffer(buf_size);

    length += buf_size;

    bufs->push_back(uv_buf_init(body_tail_buf_.data(), body_tail_buf_.size()));

    encode_short(body_tail_buf_.data(), batch->consistency());
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
