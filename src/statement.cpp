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


extern "C" {

cass_statement_t*
cass_statement_new(const char* statement, size_t statement_length,
                   size_t parameter_count,
                   cass_consistency_t consistency) {
  cass::Statement* query_statement = new cass::Query(parameter_count, consistency);
  query_statement->statement(statement, statement_length);
  return cass_statement_t::to(query_statement);
}

void
cass_statement_free(cass_statement_t *statement) {
  delete statement->from();
}

cass_code_t
cass_statement_bind_int32(
    cass_statement_t* statement,
    size_t        index,
    int32_t       value) {
  return statement->bind_int32(index, value);
}

cass_code_t
cass_statement_bind_int64(
    cass_statement_t* statement,
    size_t        index,
    int64_t       value) {
  return statement->bind_int64(index, value);
}

cass_code_t
cass_statement_bind_float(
    cass_statement_t* statement,
    size_t        index,
    float         value) {
  return statement->bind_float(index, value);
}

cass_code_t
cass_statement_bind_double(
    cass_statement_t*  statement,
    size_t         index,
    double         value) {
  return statement->bind_double(index, value);
}

cass_code_t
cass_statement_bind_bool(
    cass_statement_t*  statement,
    size_t         index,
    cass_bool_t    value) {
  return statement->bind_bool(index, value);
}

cass_code_t
cass_statement_bind_string(
    cass_statement_t*  statement,
    size_t         index,
    const char*    value,
    size_t         value_length) {
  return statement->bind(index, value, value_length);
}

cass_code_t
cass_statement_bind_bytes(
    cass_statement_t*  statement,
    size_t         index,
    const uint8_t*    value,
    size_t         value_length) {
  return statement->bind(index, value, value_length);
}

cass_code_t
cass_statement_bind_uuid(
    cass_statement_t*  statement,
    size_t         index,
    cass_uuid_t        value) {
  return statement->bind(index, value);
}

cass_code_t
cass_statement_bind_inet(
    cass_statement_t* statement,
    size_t         index,
    cass_inet_t value) {
  return statement->bind(index, value.address, value.address_length);
}

cass_code_t
cass_statement_bind_decimal(cass_statement_t* statement,
                            cass_size_t index,
                            cass_uint32_t scale,
                            const cass_byte_t* varint, cass_size_t varint_length) {
  return statement->bind(index, scale, varint, varint_length);
}

cass_code_t
cass_statement_bind_collection(
    cass_statement_t*  statement,
    size_t          index,
    const cass_collection_t* collection,
    cass_bool_t is_map) {
  return statement->bind(index, collection->from(), static_cast<bool>(is_map));
}

cass_code_t
cass_statement_bind_custom(cass_statement_t* statement,
                           cass_size_t index,
                           cass_size_t size,
                           cass_byte_t** output) {
  return statement->bind(index, size, output);
}

} // extern "C"
