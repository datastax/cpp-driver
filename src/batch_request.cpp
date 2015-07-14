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

#include "batch_request.hpp"

#include "execute_request.hpp"
#include "external_types.hpp"
#include "serialization.hpp"
#include "statement.hpp"


extern "C" {

CassBatch* cass_batch_new(CassBatchType type) {
  cass::BatchRequest* batch = new cass::BatchRequest(type);
  batch->inc_ref();
  return CassBatch::to(batch);
}

void cass_batch_free(CassBatch* batch) {
  batch->dec_ref();
}

CassError cass_batch_set_consistency(CassBatch* batch,
                                CassConsistency consistency) {
  batch->set_consistency(consistency);
  return CASS_OK;
}

CassError cass_batch_set_retry_policy(CassBatch* batch,
                                      CassRetryPolicy* retry_policy) {
  batch->set_retry_policy(retry_policy);
  return CASS_OK;
}

CassError cass_batch_add_statement(CassBatch* batch, CassStatement* statement) {
  batch->add_statement(statement);
  return CASS_OK;
}

} // extern "C"

namespace cass {

int BatchRequest::encode(int version, Handler* handler, BufferVec* bufs) const {
  int length = 0;
  uint8_t flags = 0;

  if (version == 1) {
    return ENCODE_ERROR_UNSUPPORTED_PROTOCOL;
  }

  {
    // <type> [byte] + <n> [short]
    size_t buf_size = sizeof(uint8_t) + sizeof(uint16_t);

    Buffer buf(buf_size);

    size_t pos = buf.encode_byte(0, type_);
    buf.encode_uint16(pos, statements().size());

    bufs->push_back(buf);
    length += buf_size;
  }

  for (BatchRequest::StatementList::const_iterator i = statements_.begin(),
       end = statements_.end(); i != end; ++i) {
    const SharedRefPtr<Statement>& statement(*i);
    if (statement->has_names_for_values()) {
      return ENCODE_ERROR_BATCH_WITH_NAMED_VALUES;
    }
    int32_t result = (*i)->encode_batch(version, bufs, handler->encoding_cache());
    if (result < 0) {
      return result;
    }
    length += result;
  }

  {
    // <consistency> [short]
    size_t buf_size = sizeof(uint16_t);
    if (version >= 3) {
      buf_size += sizeof(uint8_t);
    }

    Buffer buf(buf_size);

    size_t pos = buf.encode_uint16(0, consistency_);
    if (version >= 3) {
      buf.encode_byte(pos, flags);
    }

    bufs->push_back(buf);
    length += buf_size;
  }

  return length;
}

void BatchRequest::add_statement(Statement* statement) {
  if (statement->kind() == CASS_BATCH_KIND_PREPARED) {
    ExecuteRequest* execute_request = static_cast<ExecuteRequest*>(statement);
    prepared_statements_[execute_request->prepared()->id()] = execute_request;
  }
  statements_.push_back(SharedRefPtr<Statement>(statement));
}

bool BatchRequest::prepared_statement(const std::string& id,
                                      std::string* statement) const {
  PreparedMap::const_iterator it = prepared_statements_.find(id);
  if (it != prepared_statements_.end()) {
    *statement = it->second->prepared()->statement();
    return true;
  }
  return false;
}

bool BatchRequest::get_routing_key(std::string* routing_key, EncodingCache* cache) const {
  for (BatchRequest::StatementList::const_iterator i = statements_.begin();
       i != statements_.end(); ++i) {
    if ((*i)->get_routing_key(routing_key, cache)) {
      return true;
    }
  }
  return false;
}

} // namespace cass
