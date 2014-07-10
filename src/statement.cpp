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

#include "types.hpp"
#include "statement.hpp"
#include "query_request.hpp"

#include "third_party/boost/boost/cstdint.hpp"

extern "C" {

CassStatement* cass_statement_new(CassString statement, size_t parameter_count) {
  cass::Statement* query = new cass::QueryRequest(parameter_count);
  query->retain();
  query->set_query(statement.data, statement.length);
  return CassStatement::to(query);
}

void cass_statement_free(CassStatement* statement) {
  statement->release();
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
                                         cass_int32_t page_size) {
  statement->set_page_size(page_size);
  return CASS_OK;
}

CassError cass_statement_set_paging_state(CassStatement* statement,
                                          const CassResult* result) {
  statement->set_paging_state(result->paging_state());
  return CASS_OK;
}

CassError cass_statement_bind_null(CassStatement* statement,
                                   cass_size_t index) {
  return statement->bind_null(index);
}

CassError cass_statement_bind_int32(CassStatement* statement, size_t index,
                                    int32_t value) {
  return statement->bind_int32(index, value);
}

CassError cass_statement_bind_int64(CassStatement* statement, size_t index,
                                    int64_t value) {
  return statement->bind_int64(index, value);
}

CassError cass_statement_bind_float(CassStatement* statement, size_t index,
                                    float value) {
  return statement->bind_float(index, value);
}

CassError cass_statement_bind_double(CassStatement* statement, size_t index,
                                     double value) {
  return statement->bind_double(index, value);
}

CassError cass_statement_bind_bool(CassStatement* statement, size_t index,
                                   cass_bool_t value) {
  return statement->bind_byte(index, value == cass_true);
}

CassError cass_statement_bind_string(CassStatement* statement, size_t index,
                                     CassString value) {
  return statement->bind(index, value.data, value.length);
}

CassError cass_statement_bind_bytes(CassStatement* statement, size_t index,
                                    CassBytes value) {
  return statement->bind(index, value.data, value.size);
}

CassError cass_statement_bind_uuid(CassStatement* statement, size_t index,
                                   const CassUuid value) {
  return statement->bind(index, value);
}

CassError cass_statement_bind_inet(CassStatement* statement, size_t index,
                                   CassInet value) {
  return statement->bind(index, value.address, value.address_length);
}

CassError cass_statement_bind_decimal(CassStatement* statement,
                                      cass_size_t index, CassDecimal value) {
  return statement->bind(index, value.scale, value.varint.data,
                         value.varint.size);
}

CassError cass_statement_bind_collection(CassStatement* statement, size_t index,
                                         const CassCollection* collection) {
  return statement->bind(index, collection->from());
}

CassError cass_statement_bind_custom(CassStatement* statement,
                                     cass_size_t index, cass_size_t size,
                                     cass_byte_t** output) {
  return statement->bind(index, size, output);
}

} // extern "C"

namespace cass {

int32_t Statement::encode_values(int version, BufferVec* bufs) const {
  int32_t values_size = 0;
  for (ValueVec::const_iterator it = values_.begin(), end = values_.end();
       it != end; ++it) {
    if (it->is_empty()) {
      Buffer buf(sizeof(int32_t));
      buf.encode_int32(0, -1); // [bytes] "null"
      bufs->push_back(buf);
      values_size += sizeof(int32_t);
    } else if (it->is_collection()) {
      values_size += it->collection()->encode(version, bufs);
    } else {
      bufs->push_back(*it);
      values_size += it->size();
    }
  }
  return values_size;
}

} // namespace  cass
