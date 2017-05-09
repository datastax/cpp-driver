/*
  Copyright (c) 2014-2016 DataStax

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

#include "constants.hpp"
#include "execute_request.hpp"
#include "external.hpp"
#include "request_callback.hpp"
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

CassError cass_batch_set_serial_consistency(CassBatch* batch,
                                            CassConsistency serial_consistency) {
  batch->set_serial_consistency(serial_consistency);
  return CASS_OK;
}

CassError cass_batch_set_timestamp(CassBatch* batch,
                                   cass_int64_t timestamp) {
  batch->set_timestamp(timestamp);
  return CASS_OK;
}

CassError cass_batch_set_request_timeout(CassBatch *batch,
                                         cass_uint64_t timeout_ms) {
  batch->set_request_timeout_ms(timeout_ms);
  return CASS_OK;
}

CassError cass_batch_set_is_idempotent(CassBatch* batch,
                                       cass_bool_t is_idempotent) {
  batch->set_is_idempotent(is_idempotent == cass_true);
  return CASS_OK;
}

CassError cass_batch_set_retry_policy(CassBatch* batch,
                                      CassRetryPolicy* retry_policy) {
  batch->set_retry_policy(retry_policy);
  return CASS_OK;
}

CassError cass_batch_set_custom_payload(CassBatch* batch,
                                        const CassCustomPayload* payload) {
  batch->set_custom_payload(payload);
  return CASS_OK;
}

CassError cass_batch_add_statement(CassBatch* batch, CassStatement* statement) {
  batch->add_statement(statement);
  return CASS_OK;
}

} // extern "C"

namespace cass {

// Format: <type><n><query_1>...<query_n><consistency><flags>[<serial_consistency>][<timestamp>]
// where:
// <type> is a [byte]
// <n> is a [short]
// <query> has the format <kind><string_or_id><n>[<name_1>]<value_1>...[<name_n>]<value_n>
// <consistency> is a [short]
// Only protocol v3 and higher for the following:
// <flags> is a [byte] (or [int] for protocol v5)
// <serial_consistency> is a [short]
// <timestamp> is a [long]
int BatchRequest::encode(int version, RequestCallback* callback, BufferVec* bufs) const {
  int length = 0;
  uint32_t flags = 0;

  if (version == 1) {
    return REQUEST_ERROR_UNSUPPORTED_PROTOCOL;
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
    const Statement::Ptr& statement(*i);
    if (statement->has_names_for_values()) {
      callback->on_error(CASS_ERROR_LIB_BAD_PARAMS,
                        "Batches cannot contain queries with named values");
      return REQUEST_ERROR_BATCH_WITH_NAMED_VALUES;
    }
    int32_t result = statement->encode_batch(version, callback, bufs);
    if (result < 0) {
      return result;
    }
    length += result;
  }

  {
    // <consistency> [short]
    size_t buf_size = sizeof(uint16_t);
    if (version >= 3) {
      // <flags>[<serial_consistency><timestamp>]
      if (version >= 5) {
        buf_size += sizeof(int32_t); // [int]
      } else {
        buf_size += sizeof(uint8_t); // [byte]
      }

      if (serial_consistency() != 0) {
        buf_size += sizeof(uint16_t); // [short]
        flags |= CASS_QUERY_FLAG_SERIAL_CONSISTENCY;
      }

      if (callback->timestamp() != CASS_INT64_MIN) {
        buf_size += sizeof(int64_t); // [long]
        flags |= CASS_QUERY_FLAG_DEFAULT_TIMESTAMP;
      }
    }

    Buffer buf(buf_size);

    size_t pos = buf.encode_uint16(0, callback->consistency());
    if (version >= 3) {
      if (version >= 5) {
        pos = buf.encode_int32(pos, flags);
      } else {
        pos = buf.encode_byte(pos, flags);
      }

      if (serial_consistency() != 0) {
        pos = buf.encode_uint16(pos, serial_consistency());
      }

      if (callback->timestamp() != CASS_INT64_MIN) {
        pos = buf.encode_int64(pos, callback->timestamp());
      }
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
  statements_.push_back(Statement::Ptr(statement));
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
