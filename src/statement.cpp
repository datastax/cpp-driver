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

#include "statement.hpp"

#include "collection.hpp"
#include "execute_request.hpp"
#include "external_types.hpp"
#include "macros.hpp"
#include "prepared.hpp"
#include "query_request.hpp"
#include "scoped_ptr.hpp"
#include "string_ref.hpp"

#include <uv.h>

extern "C" {

CassStatement* cass_statement_new(CassSession* session,
                                  const char* query,
                                  size_t parameter_count) {
  return cass_statement_new_n(session, query, strlen(query), parameter_count);
}

CassStatement* cass_statement_new_n(CassSession* session,
                                    const char* query,
                                    size_t query_length,
                                    size_t parameter_count) {
  cass::QueryRequest* query_request = new cass::QueryRequest(parameter_count);
  query_request->inc_ref();
  query_request->set_query(query, query_length);
  return CassStatement::to(query_request);
}


CassError cass_statement_add_key_index(CassStatement* statement, size_t index) {
  if (statement->kind() != CASS_BATCH_KIND_QUERY) return CASS_ERROR_LIB_BAD_PARAMS;
  if (index >= statement->buffers_count()) return CASS_ERROR_LIB_BAD_PARAMS;
  statement->add_key_index(index);
  return CASS_OK;
}

CassError cass_statement_set_keyspace(CassStatement* statement, const char* keyspace) {
  return cass_statement_set_keyspace_n(statement, keyspace, strlen(keyspace));
}

CassError cass_statement_set_keyspace_n(CassStatement* statement,
                                        const char* keyspace,
                                        size_t keyspace_length) {
  if (statement->kind() != CASS_BATCH_KIND_QUERY) return CASS_ERROR_LIB_BAD_PARAMS;
  statement->set_keyspace(std::string(keyspace, keyspace_length));
  return CASS_OK;
}

void cass_statement_free(CassStatement* statement) {
  statement->dec_ref();
}

CassError cass_statement_set_consistency(CassStatement* statement,
                                         CassConsistency consistency) {
  statement->set_consistency(consistency);
  return CASS_OK;
}

CassError cass_statement_set_serial_consistency(CassStatement* statement,
                                                CassConsistency serial_consistency) {
  statement->set_serial_consistency(serial_consistency);
  return CASS_OK;
}

CassError cass_statement_set_paging_size(CassStatement* statement,
                                         int page_size) {
  statement->set_page_size(page_size);
  return CASS_OK;
}

CassError cass_statement_set_paging_state(CassStatement* statement,
                                          const CassResult* result) {
  statement->set_paging_state(result->paging_state());
  return CASS_OK;
}

#define CASS_STATEMENT_BIND(Name, Params, Value)                                \
  CassError cass_statement_bind_##Name(CassStatement* statement,                \
                                      size_t index Params) {                    \
    return statement->set(index, Value);                                        \
  }                                                                             \
  CassError cass_statement_bind_##Name##_by_name(CassStatement* statement,      \
                                                const char* name Params) {      \
    return statement->set(cass::StringRef(name), Value);                        \
  }                                                                             \
  CassError cass_statement_bind_##Name##_by_name_n(CassStatement* statement,    \
                                                   const char* name,            \
                                                   size_t name_length Params) { \
    return statement->set(cass::StringRef(name, name_length), Value);           \
  }

CASS_STATEMENT_BIND(null, , cass::CassNull())
CASS_STATEMENT_BIND(int32, ONE_PARAM_(cass_int32_t value), value)
CASS_STATEMENT_BIND(int64, ONE_PARAM_(cass_int64_t) value, value)
CASS_STATEMENT_BIND(float, ONE_PARAM_(cass_float_t value), value)
CASS_STATEMENT_BIND(double, ONE_PARAM_(cass_double_t value), value)
CASS_STATEMENT_BIND(bool, ONE_PARAM_(cass_bool_t value), value)
CASS_STATEMENT_BIND(uuid, ONE_PARAM_(CassUuid value), value)
CASS_STATEMENT_BIND(inet, ONE_PARAM_(CassInet value), value)
CASS_STATEMENT_BIND(collection, ONE_PARAM_(const CassCollection* value), value->from())
CASS_STATEMENT_BIND(bytes,
                    TWO_PARAMS_(const cass_byte_t* value, size_t value_size),
                    cass::CassBytes(value, value_size))
CASS_STATEMENT_BIND(custom,
                    TWO_PARAMS_(size_t size, cass_byte_t** output),
                    cass::CassCustom(output, size))
CASS_STATEMENT_BIND(decimal,
                    THREE_PARAMS_(const cass_byte_t* varint, size_t varint_size, int scale),
                    cass::CassDecimal(varint, varint_size, scale))

#undef CASS_STATEMENT_BIND

CassError cass_statement_bind_string(CassStatement* statement, size_t index,
                                     const char* value) {
  return cass_statement_bind_string_n(statement, index,
                                      value, strlen(value));
}

CassError cass_statement_bind_string_n(CassStatement* statement, size_t index,
                                       const char* value, size_t value_length) {
  return statement->set(index, cass::CassString(value, value_length));
}

CassError cass_statement_bind_string_by_name(CassStatement* statement,
                                             const char* name,
                                             const char* value) {
  return statement->set(cass::StringRef(name),
                        cass::CassString(value, strlen(value)));
}

CassError cass_statement_bind_string_by_name_n(CassStatement* statement,
                                               const char* name,
                                               size_t name_length,
                                               const char* value,
                                               size_t value_length) {
  return statement->set(cass::StringRef(name, name_length),
                        cass::CassString(value, strlen(value)));
}

} // extern "C"

namespace cass {

bool Statement::get_routing_key(std::string* routing_key)  const {
  if (key_indices_.empty()) return false;

  if (key_indices_.size() == 1) {
      assert(key_indices_.front() < buffers_count());
      Buffer buf(buffers()[key_indices_.front()]);
      routing_key->assign(buf.data() + sizeof(int32_t), buf.size());
  } else {
    size_t length = 0;

    for (std::vector<size_t>::const_iterator i = key_indices_.begin();
         i != key_indices_.end(); ++i) {
      assert(*i < buffers_count());
      length += sizeof(uint16_t) + buffers()[*i].size() + 1;
    }

    routing_key->clear();
    routing_key->reserve(length);

    for (std::vector<size_t>::const_iterator i = key_indices_.begin();
         i != key_indices_.end(); ++i) {

      Buffer buf(buffers()[*i]);

      char size_buf[sizeof(uint16_t)];
      encode_uint16(size_buf, buf.size());
      routing_key->append(size_buf, sizeof(uint16_t));
      routing_key->append(buf.data() + sizeof(int32_t), buf.size());
      routing_key->push_back(0);
    }
  }

  return true;
}

} // namespace  cass
