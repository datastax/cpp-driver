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

#include "cql.h"
#include "cql_cluster.hpp"
#include "cql_session.hpp"

struct CqlSessionFuture {
  std::unique_ptr<CqlSessionRequest> request;

  CqlSessionFuture(
      CqlSessionRequest* request) :
      request(request) {
  }
};

CqlCluster*
cql_cluster_new() {
  return new CqlCluster();
}

void
cql_cluster_free(
    struct CqlCluster* cluster) {
  delete cluster;
}

CqlError*
cql_cluster_setopt(
    struct CqlCluster* cluster,
    int                option,
    const void*        data,
    size_t             datalen) {
  cluster->option(option, data, datalen);
  return CQL_ERROR_NO_ERROR;
}

CqlError*
cql_session_new(
    CqlCluster*  cluster,
    CqlSession** session) {
  *session = cluster->new_session();
  return CQL_ERROR_NO_ERROR;
}

void
cql_session_free(
    struct CqlSession* session) {
  delete session;
}

CqlError*
cql_session_connect(
    struct CqlSession*        session,
    struct CqlSessionFuture** future) {
  *future = new CqlSessionFuture(session->connect(""));
  return CQL_ERROR_NO_ERROR;
}

CqlError*
cql_session_connect_keyspace(
    struct CqlSession* session,
    char*              keyspace,
    CqlSessionFuture** future) {
  *future = new CqlSessionFuture(session->connect(keyspace));
  return CQL_ERROR_NO_ERROR;
}

CqlError*
cql_session_shutdown(
    CqlSession*        session,
    CqlSessionFuture** future) {
  *future = new CqlSessionFuture(session->shutdown());
  return CQL_ERROR_NO_ERROR;
}

void
cql_session_future_free(
    struct CqlSessionFuture* future) {
  delete future;
}

int
cql_session_future_ready(
    struct CqlSessionFuture* future) {
  return static_cast<int>(future->request->ready());
}

void
cql_session_future_wait(
    struct CqlSessionFuture* future) {
  future->request->wait();
}

int
cql_session_future_wait_timed(
    struct CqlSessionFuture* future,
    size_t                   wait) {
  return static_cast<int>(
      future->request->wait_for(
          std::chrono::microseconds(wait)));
}

CqlError*
cql_session_future_get_error(
    struct CqlSessionFuture* future) {
  return future->request->error;
}

void
cql_error_string(
    CqlError* error,
    char*     output,
    size_t    n,
    size_t*   total) {
  *total = error->message.copy(output, n);
}

int
cql_error_source(
    CqlError* error) {
  return error->source;
}

int
cql_error_code(
    CqlError* error) {
  return error->code;
}

CqlError*
cql_session_query(
    CqlSession*    session,
    char*          statement,
    size_t         statement_length,
    size_t         paramater_count,
    size_t         consistency,
    CqlStatement** output) {
  (void) session;
  *output = new CqlQueryStatement(paramater_count, consistency);
  (*output)->statement(statement, statement_length);
  return CQL_ERROR_NO_ERROR;
}

CqlError*
cql_session_prepare(
    CqlSession*        session,
    const char*        statement,
    size_t             statement_length,
    CqlPrepareFuture** output) {
  *output = session->prepare(statement, statement_length);
  return CQL_ERROR_NO_ERROR;
}

CqlError*
cql_session_bind(
    CqlSession*    session,
    CqlPrepared*   prepared,
    size_t         paramater_count,
    size_t         consistency,
    CqlStatement** output) {
  *output = new CqlBoundStatement(*prepared, consistency);
  return CQL_ERROR_NO_ERROR;
}

CqlError*
cql_session_batch(
    CqlSession*         session,
    size_t              consistency,
    CqlBatchStatement** output) {
  *output = new CqlBatchStatement(consistency);
  return CQL_ERROR_NO_ERROR;
}

CqlError*
cql_batch_add_statement(
    CqlBatchStatement* batch,
    CqlStatement*      statement) {
  batch->add_statement(statement);
  return CQL_ERROR_NO_ERROR;
}
