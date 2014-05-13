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

#include <string>

#include "cassandra.h"
#include "session.hpp"
#include "body_result.hpp"

// This abstraction allows us to separate internal types from the
// external opaque pointers that we expose.
template<typename In, typename Ex>
struct External : public In {
  inline In* from() { return static_cast<In*>(this); }
  inline static Ex* to(In* in) { return static_cast<Ex*>(in); }
  inline static const Ex* to(const In* in) { return static_cast<const Ex*>(in); }
};

extern "C" {

// Internal/External type mappings
struct CassSession : External<cass::Session, CassSession> { };
struct CassStatement : External<cass::Statement, CassStatement> { };
struct CassFuture : External<cass::Future, CassFuture> { };
struct CassBatchStatement : External<cass::BatchStatement, CassBatchStatement> { };
struct CassPrepared : External<cass::Prepared, CassPrepared> { };
struct CassResult : External<cass::Result, CassResult> { };

CassSession* cass_session_new() {
  return CassSession::to(new cass::Session());
}

CassSession* cass_session_clone(CassSession* session) {
  return CassSession::to(new cass::Session(session));
}

void
cass_session_free(
   CassSession* session) {
  delete session->from();
}

CassCode
cass_session_setopt(
    CassSession* session,
    CassOption   option,
    const void* data,
    size_t      data_len) {
  return session->config_.option(option, data, data_len);
}

CassCode
cass_session_getopt(
    CassSession* session,
    CassOption   option,
    void**      data,
    size_t*     data_len)
{
  return CASS_OK;
}

CassCode
cass_session_connect(
   CassSession* session,
   CassFuture** future) {
  cass::Future* session_future = session->connect("");
  *future = CassFuture::to(session_future);
  return CASS_OK;
}

CassCode
cass_session_connect_keyspace(
   CassSession*  session,
    const char* keyspace,
    CassFuture** future) {
  cass::Future* session_future = session->connect(keyspace);
  *future = CassFuture::to(session_future);
  return CASS_OK;
}

CassCode
cass_session_shutdown(
    CassSession* session,
    CassFuture** future) {
  cass::Future* shutdown_future = session->shutdown();
  *future = CassFuture::to(shutdown_future);
  return CASS_OK;
}

void
cass_future_free(
   CassFuture* future) {
  // TODO(mpenick): We can't do this because the memory could still be in use by an internal thread
  // This needs to be referenced counted
  delete future->from();
}

int
cass_future_ready(
   CassFuture* future) {
  return static_cast<int>(future->ready());
}

void
cass_future_wait(
   CassFuture* future) {
  future->wait();
}

int
cass_future_wait_timed(
   CassFuture* future,
    size_t     wait) {
  return static_cast<int>(future->wait_for(wait));
}

CassSession*
cass_future_get_session(
   CassFuture* future) {
  cass::SessionFutureImpl* session_future = static_cast<cass::SessionFutureImpl*>(future->from());
  return CassSession::to(session_future->result);
}

CassResult*
cass_future_get_result(
   CassFuture* future) {
  cass::MessageFutureImpl* message_future = static_cast<cass::MessageFutureImpl*>(future->from());
  cass::Result* result = static_cast<cass::Result*>(message_future->result->body.release());
  return CassResult::to(result);
}

CassPrepared*
cass_future_get_prepare(
   CassFuture* future) {
  cass::MessageFutureImpl* message_future = static_cast<cass::MessageFutureImpl*>(future->from());
  cass::Result* result = static_cast<cass::Result*>(message_future->result->body.get());
  return CassPrepared::to(new cass::Prepared(std::string(result->prepared,
                                                         result->prepared_size)));
}

void
cass_future_error_string(
    CassFuture* future,
    char*      output,
    size_t     output_len,
    size_t*    total) {
  *total = future->error->message.copy(output, output_len);
}

CassSource
cass_future_error_source(
    CassFuture* future) {
  return future->error->source;
}

CassCode
cass_future_error_code(
    CassFuture* future) {
  return future->error->code;
}

const char*
cass_error_desc(
    int code) {
  return "";
}

CassCode
cass_session_query(
    CassSession*    session,
    const char*    statement,
    size_t         statement_length,
    size_t         parameter_count,
    CassConsistency consistency,
    CassStatement** output) {
  (void) session;
  cass::Statement* query_statement = new cass::QueryStatement(parameter_count, consistency);
  *output = CassStatement::to(query_statement);
  (*output)->statement(statement, statement_length);
  return CASS_OK;
}

CassCode
cass_session_prepare(
    CassSession* session,
    const char* statement,
    size_t      statement_length,
    CassFuture** output) {
  cass::Future* future = session->prepare(statement, statement_length);
  *output = CassFuture::to(future);
  return CASS_OK;
}

CassCode
cass_session_bind(
    CassSession*    session,
    CassPrepared*   prepared,
    size_t         parameter_count,
    CassConsistency consistency,
    CassStatement** output) {
  cass::Statement* bound_statement = new cass::BoundStatement(*prepared, parameter_count, consistency);
  *output = CassStatement::to(bound_statement);
  return CASS_OK;
}

CassCode
cass_session_batch(
    CassSession*         session,
    CassConsistency      consistency,
    CassBatchStatement** output) {
  cass::BatchStatement* batch_statment = new cass::BatchStatement(consistency);
  *output = CassBatchStatement::to(batch_statment);
  return CASS_OK;
}

CassCode
cass_batch_add_statement(
    CassBatchStatement* batch,
    CassStatement*      statement) {
  batch->add_statement(statement);
  return CASS_OK;
}

/**
 * Bind a short to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return
 */
CassCode
cass_statement_bind_short(
    CassStatement* statement,
    size_t        index,
    int16_t       value) {
  return statement->bind(index, value);
}

/**
 * Bind an int to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return
 */
CassCode
cass_statement_bind_int(
    CassStatement* statement,
    size_t        index,
    int32_t       value) {
  return statement->bind(index, value);
}

/**
 * Bind a bigint to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return
 */
CassCode
cass_statement_bind_bigint(
    CassStatement* statement,
    size_t        index,
    int64_t       value) {
  return statement->bind(index, value);
}

/**
 * Bind a float to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return
 */
CassCode
cass_statement_bind_float(
    CassStatement* statement,
    size_t        index,
    float         value) {
  return statement->bind(index, value);
}

/**
 * Bind a double to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return NULL if successful, otherwise pointer to CassError structure
 */
CassCode
cass_statement_bind_double(
    CassStatement*  statement,
    size_t         index,
    double         value) {
  return statement->bind(index, value);
}

/**
 * Bind a bool to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return NULL if successful, otherwise pointer to CassError structure
 */
CassCode
cass_statement_bind_bool(
    CassStatement*  statement,
    size_t         index,
    cass_bool_t    value) {
  return statement->bind(index, value);
}

/**
 * Bind a time stamp to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return NULL if successful, otherwise pointer to CassError structure
 */
int
cass_statement_bind_time(
    CassStatement*  statement,
    size_t         index,
    int64_t        value) {
  return statement->bind(index, value);
}

/**
 * Bind a UUID to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return NULL if successful, otherwise pointer to CassError structure
 */
CassCode
cass_statement_bind_uuid(
    CassStatement*  statement,
    size_t         index,
    CassUuid        value) {
  return statement->bind(index, value);
}

/**
 * Bind a counter to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return NULL if successful, otherwise pointer to CassError structure
 */
CassCode
cass_statement_bind_counter(
    CassStatement*  statement,
    size_t         index,
    int64_t        value) {
  return statement->bind(index, value);
}

/**
 * Bind a string to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 * @param length
 *
 * @return NULL if successful, otherwise pointer to CassError structure
 */
CassCode
cass_statement_bind_string(
    CassStatement*  statement,
    size_t         index,
    char*          value,
    size_t         length) {
  return statement->bind(index, value, length);
}

/**
 * Bind a blob to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 * @param length
 *
 * @return NULL if successful, otherwise pointer to CassError structure
 */
CassCode
cass_statement_bind_blob(
    CassStatement*  statement,
    size_t         index,
    uint8_t*       value,
    size_t         length) {
  return statement->bind(index, reinterpret_cast<char*>(value), length);
}

/**
 * Bind a decimal to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param scale
 * @param value
 * @param length
 *
 * @return NULL if successful, otherwise pointer to CassError structure
 */
CassCode
cass_statement_bind_decimal(
    CassStatement* statement,
    size_t        index,
    uint32_t      scale,
    uint8_t*      value,
    size_t        length) {
  return CASS_OK;
}

CassCode
cass_statement_bind_inet(
    CassStatement* statement,
    size_t         index,
    cass_uint8_t*   address,
    cass_uint8_t    address_len,
    cass_int32_t   port) {
  return statement->bind(index, address, address_len, port);
}

/**
 * Bind a varint to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 * @param length
 *
 * @return NULL if successful, otherwise pointer to CassError structure
 */
CassCode
cass_statement_bind_varint(
    CassStatement*  statement,
    size_t         index,
    uint8_t*       value,
    size_t         length) {
  return statement->bind(index, reinterpret_cast<char*>(value), length);
}

CassCode
cass_session_exec(
    CassSession*   session,
    CassStatement* statement,
    CassFuture**   future) {
  *future = CassFuture::to(session->execute(statement->from()));
  return CASS_OK;
}

size_t
cass_result_rowcount(
    CassResult* result) {
  if(result->kind == CASS_RESULT_KIND_ROWS) {
    return result->row_count;
  }
  return 0;
}

size_t
cass_result_colcount(
    CassResult* result) {
  if(result->kind == CASS_RESULT_KIND_ROWS) {
    return result->column_count;
  }
  return 0;
}

CassCode
cass_result_coltype(
    CassResult*     result,
    size_t         index,
    CassColumnType* coltype) {
  if(result->kind == CASS_RESULT_KIND_ROWS) {
    *coltype = static_cast<CassColumnType>(result->column_metadata[index].type);
    return CASS_OK;
  }
  return CASS_ERROR_LIB_BAD_PARAMS; // TODO(mpenick): Figure out error codes
}

CassCode
cass_result_iterator_new(
    CassResult* result,
    CassIterator** iterator) {
  return CASS_ERROR_LIB_BAD_PARAMS; // TODO(mpenick): Figure out error codes
}

CassCode
cass_result_iterator_get(
    CassIterator* iterator,
    CassRow** row) {
  return CASS_ERROR_LIB_BAD_PARAMS; // TODO(mpenick): Figure out error codes
}

CassCode
cass_iterator_next(
    CassIterator* iterator) {
  return CASS_ERROR_LIB_BAD_PARAMS; // TODO(mpenick): Figure out error codes
}

CassCode
cass_iterator_reset(
    CassIterator* iterator) {
  return CASS_ERROR_LIB_BAD_PARAMS; // TODO(mpenick): Figure out error codes
}

CASS_EXPORT void
cass_iterator_free(
    CassIterator* iterator) {
}

CassCode
cass_row_getcol(
    CassRow*  row,
    size_t index,
    CassValue** value) {
  return CASS_ERROR_LIB_BAD_PARAMS; // TODO(mpenick): Figure out error codes
}

} // extern "C"
