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
cass_statement_bind_short(
    cass_statement_t* statement,
    size_t        index,
    int16_t       value) {
  return statement->bind_int32(index, value);
}

cass_code_t
cass_statement_bind_int(
    cass_statement_t* statement,
    size_t        index,
    int32_t       value) {
  return statement->bind_int32(index, value);
}

cass_code_t
cass_statement_bind_bigint(
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

int
cass_statement_bind_time(
    cass_statement_t*  statement,
    size_t         index,
    int64_t        value) {
  return statement->bind_int64(index, value);
}

cass_code_t
cass_statement_bind_uuid(
    cass_statement_t*  statement,
    size_t         index,
    cass_uuid_t        value) {
  return statement->bind(index, value);
}

cass_code_t
cass_statement_bind_counter(
    cass_statement_t*  statement,
    size_t         index,
    int64_t        value) {
  return statement->bind_int64(index, value);
}

cass_code_t
cass_statement_bind_string(
    cass_statement_t*  statement,
    size_t         index,
    const char*    value,
    size_t         length) {
  return statement->bind(index, value, length);
}

cass_code_t
cass_statement_bind_blob(
    cass_statement_t*  statement,
    size_t         index,
    uint8_t*       value,
    size_t         length) {
  return statement->bind(index, reinterpret_cast<char*>(value), length);
}

cass_code_t
cass_statement_bind_decimal(
    cass_statement_t* statement,
    size_t        index,
    uint32_t      scale,
    uint8_t*      value,
    size_t        length) {
  return CASS_OK;
}

cass_code_t
cass_statement_bind_inet(
    cass_statement_t* statement,
    size_t         index,
    const cass_uint8_t* address,
    cass_uint8_t    address_len) {
  return statement->bind(index, address, address_len);
}

cass_code_t
cass_statement_bind_varint(
    cass_statement_t*  statement,
    size_t         index,
    uint8_t*       value,
    size_t         length) {
  return statement->bind(index, reinterpret_cast<char*>(value), length);
}

cass_code_t
cass_statement_bind_collection(
    cass_statement_t*  statement,
    size_t          index,
    cass_collection_t*     collection,
    cass_bool_t is_map) {
  return statement->bind(index, collection->from(), static_cast<bool>(is_map));
}

} // extern "C"
