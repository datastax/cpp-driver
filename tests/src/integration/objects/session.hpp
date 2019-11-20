/*
  Copyright (c) DataStax, Inc.

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

#ifndef __TEST_SESSION_HPP__
#define __TEST_SESSION_HPP__
#include "cassandra.h"

#include "objects/object_base.hpp"

#include "objects/future.hpp"
#include "objects/prepared.hpp"
#include "objects/result.hpp"
#include "objects/schema.hpp"
#include "objects/statement.hpp"

#include "exception.hpp"

namespace test { namespace driver {

/**
 * Wrapped session object
 */
class Session : public Object<CassSession, cass_session_free> {
  friend class Cluster;

public:
  class Exception : public test::CassException {
  public:
    Exception(const std::string& message, const CassError error_code,
              const std::string& error_message)
        : test::CassException(message, error_code, error_message) {}
  };

  /**
   * Create the default session object
   */
  Session()
      : Object<CassSession, cass_session_free>(cass_session_new()) {}

  /**
   * Create the session object from the native driver object
   *
   * @param session Native driver object
   */
  Session(CassSession* session)
      : Object<CassSession, cass_session_free>(session) {}

  /**
   * Create the session object from a shared reference
   *
   * @param session Shared reference
   */
  Session(Ptr session)
      : Object<CassSession, cass_session_free>(session) {}

  /**
   * Close the active session
   *
   * @param assert_ok True if error code for future should be asserted
   *                  CASS_OK; false otherwise (default: true)
   */
  void close(bool assert_ok = true) {
    Future close_future = cass_session_close(get());
    close_future.wait(assert_ok);
  }

  /**
   * Asynchronously close the session.
   *
   * @return A future to track the closing process of the session.
   */
  Future close_async() { return cass_session_close(get()); }

  /**
   * Get the error code that occurred during the connection
   *
   * @return Error code of the future
   */
  CassError connect_error_code() { return connect_future_.error_code(); }

  /**
   * Get the human readable description of the error code during the connection
   *
   * @return Error description
   */
  const std::string connect_error_description() { return connect_future_.error_description(); }

  /**
   * Get the error message that occurred during the connection
   *
   * @return Error message
   */
  const std::string connect_error_message() { return connect_future_.error_message(); }

  /**
   *  Get the current driver metrics
   *
   * @return Driver metrics
   */
  CassMetrics metrics() const {
    CassMetrics metrics;
    cass_session_get_metrics(get(), &metrics);
    return metrics;
  }

  /**
   *  Get the current driver speculative execution metrics
   *
   * @return Driver speculative execution metrics
   */
  CassSpeculativeExecutionMetrics speculative_execution_metrics() const {
    CassSpeculativeExecutionMetrics speculative_execution_metrics;
    cass_session_get_speculative_execution_metrics(get(), &speculative_execution_metrics);
    return speculative_execution_metrics;
  }

  /**
   * Execute a batch statement synchronously
   *
   * @param batch Batch statement to execute
   * @param assert_ok True if error code for future should be asserted
   *                  CASS_OK; false otherwise (default: true)
   * @return Result object
   */
  Result execute(Batch batch, bool assert_ok = true) {
    Future future(cass_session_execute_batch(get(), batch.get()));
    future.wait(assert_ok);
    return Result(future);
  }

  /**
   * Execute a statement synchronously
   *
   * @param statement Query or bound statement to execute
   * @param assert_ok True if error code for future should be asserted
   *                  CASS_OK; false otherwise (default: true)
   * @return Result object
   */
  Result execute(Statement statement, bool assert_ok = true) {
    Future future(cass_session_execute(get(), statement.get()));
    future.wait(assert_ok);
    return Result(future);
  }

  /**
   * Execute a query synchronously
   *
   * @param query Query to execute as a simple statement
   * @param consistency Consistency level to execute the query at
   *                    (default: CASS_CONSISTENCY_LOCAL_ONE)
   * @param is_idempotent True if statement is idempotent; false otherwise
   *                      (default: false)
   * @param assert_ok True if error code for future should be asserted
   *                  CASS_OK; false otherwise (default: true)
   * @return Result object
   */
  Result execute(const std::string& query, CassConsistency consistency = CASS_CONSISTENCY_LOCAL_ONE,
                 bool is_idempotent = false, bool assert_ok = true) {
    Statement statement(query);
    statement.set_consistency(consistency);
    statement.set_idempotent(is_idempotent);
    return execute(statement, assert_ok);
  }

  /**
   * Execute a batch statement asynchronously
   *
   * @param batch Batch statement to execute
   * @return Future object
   */
  Future execute_async(Batch batch) {
    return Future(cass_session_execute_batch(get(), batch.get()));
  }

  /**
   * Execute a statement asynchronously
   *
   * @param statement Query or bound statement to execute
   * @return Future object
   */
  Future execute_async(Statement statement) {
    return Future(cass_session_execute(get(), statement.get()));
  }

  /**
   * Execute a query asynchronously
   *
   * @param query Query to execute as a simple statement
   * @param consistency Consistency level to execute the query at
   *                    (default: CASS_CONSISTENCY_LOCAL_ONE)
   * @param is_idempotent True if statement is idempotent; false otherwise
   *                      (default: false)
   * @return Future object
   */
  Future execute_async(const std::string& query,
                       CassConsistency consistency = CASS_CONSISTENCY_LOCAL_ONE,
                       bool is_idempotent = false) {
    Statement statement(query);
    statement.set_consistency(consistency);
    statement.set_idempotent(is_idempotent);
    return execute_async(statement);
  }

  /**
   * Create a prepared statement
   *
   * @param query The query to prepare
   * @param assert_ok True if error code for future should be asserted
   *                  CASS_OK; false otherwise (default: true)
   * @return Prepared statement generated by the server for the query
   */
  Prepared prepare(const std::string& query, bool assert_ok = true) {
    Future prepare_future = cass_session_prepare(get(), query.c_str());
    prepare_future.wait(assert_ok);
    return Prepared(prepare_future);
  }

  /**
   * Create a prepared statement from an existing statement inheriting the existing statement's
   * settings.
   *
   * @param statement The existing statement
   * @param assert_ok True if error code for future should be asserted
   *                  CASS_OK; false otherwise (default: true)
   * @return Prepared statement generated by the server for the query
   */
  Prepared prepare_from_existing(Statement statement, bool assert_ok = true) {
    Future prepare_future = cass_session_prepare_from_existing(get(), statement.get());
    prepare_future.wait(assert_ok);
    return Prepared(prepare_future);
  }

  /**
   * Get a schema snapshot
   *
   * @return A schema snapshot
   */
  Schema schema() {
    const CassSchemaMeta* schema_meta = cass_session_get_schema_meta(get());
    if (schema_meta == NULL) {
      throw test::Exception("Unable to get schema metadata");
    }
    return Schema(schema_meta);
  }

protected:
  /**
   * Create a new session and establish a connection to the server;
   * synchronously
   *
   * @param cluster Cluster configuration to use when establishing
   *                connection
   * @param keyspace Keyspace to use (default: None)
   * @return Session object
   * @throws Session::Exception If session could not be established
   */
  static Session connect(CassCluster* cluster, const std::string& keyspace, bool assert_ok = true) {
    Session session;
    if (keyspace.empty()) {
      session.connect_future_ = cass_session_connect(session.get(), cluster);
    } else {
      session.connect_future_ =
          cass_session_connect_keyspace(session.get(), cluster, keyspace.c_str());
    }
    session.connect_future_.wait(false);
    if (assert_ok && session.connect_error_code() != CASS_OK) {
      throw Exception("Unable to Establish Session Connection: " +
                          session.connect_error_description(),
                      session.connect_error_code(), session.connect_error_message());
    }
    return session;
  }

private:
  Future connect_future_;
};

}} // namespace test::driver

#endif // __TEST_SESSION_HPP__
