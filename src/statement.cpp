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

#include "cassandra.hpp"
#include "statement.hpp"
#include "query_request.hpp"

#include "third_party/boost/boost/cstdint.hpp"

extern "C" {

CassStatement* cass_statement_new(CassString statement, size_t parameter_count,
                                  CassConsistency consistency) {
  cass::Statement* query = new cass::QueryRequest(parameter_count, consistency);
  query->retain();
  query->statement(statement.data, statement.length);
  return CassStatement::to(query);
}

void cass_statement_free(CassStatement* statement) {
  statement->release();
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
  return statement->bind_bool(index, value == cass_true);
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
                                         const CassCollection* collection,
                                         cass_bool_t is_map) {
  return statement->bind(index, collection->from(), is_map == cass_true);
}

CassError cass_statement_bind_custom(CassStatement* statement,
                                     cass_size_t index, cass_size_t size,
                                     cass_byte_t** output) {
  return statement->bind(index, size, output);
}

} // extern "C"

namespace cass {

size_t Statement::encoded_size() const {
  size_t total_size = sizeof(uint16_t);
  for (ValueVec::const_iterator it = values_.begin(), end = values_.end();
       it != end; ++it) {
    total_size += sizeof(int32_t);
    int32_t value_size = it->size();
    if (value_size > 0) {
      total_size += value_size;
    }
  }
  return total_size;
}

char* Statement::encode(char* buffer) const {
  buffer = encode_short(buffer, values_.size());
  for (ValueVec::const_iterator it = values_.begin(), end = values_.end();
       it != end; ++it) {
    buffer = encode_bytes(buffer, it->data(), it->size());
  }
  return buffer;
}

} // namespace  cass
