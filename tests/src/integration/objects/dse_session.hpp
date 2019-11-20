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

#ifndef __TEST_DSE_SESSION_HPP__
#define __TEST_DSE_SESSION_HPP__
#include "objects/dse_cluster.hpp"

#include "objects/dse_statement.hpp"

namespace test { namespace driver { namespace dse {

/**
 * Wrapped DSE session object
 */
class Session : public driver::Session {
  friend class Cluster;

public:
  /**
   * Create the default DSE session object
   */
  Session()
      : driver::Session() {}

  /**
   * Create the DSE session object from the native driver object
   *
   * @param session Native driver object
   */
  Session(CassSession* session)
      : driver::Session(session) {}

  /**
   * Create the DSE session object from a shared reference
   *
   * @param session Shared reference
   */
  Session(Ptr session)
      : driver::Session(session) {}

  /**
   * Create the DSE session object from a wrapped session
   *
   * @param session Wrapped session object
   */
  Session(driver::Session session)
      : driver::Session(session) {}

  /**
   * Execute a DSE batch statement synchronously
   *
   * @param batch Batch statement to execute
   * @param assert_ok True if error code for future should be asserted
   *                  CASS_OK; false otherwise (default: true)
   * @return Result object
   */
  Result execute(Batch batch, bool assert_ok = true) {
    Future future(execute_async(batch));
    future.wait(assert_ok);
    return Result(future);
  }

  /**
   * Execute a DSE statement synchronously
   *
   * @param statement Query or bound statement to execute
   * @param assert_ok True if error code for future should be asserted
   *                  CASS_OK; false otherwise (default: true)
   * @return Result object
   */
  Result execute(Statement statement, bool assert_ok = true) {
    Future future(execute_async(statement));
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
    return driver::Session::execute(query, consistency, is_idempotent, assert_ok);
  }

  /**
   * Execute a batch statement synchronously as another user
   *
   * @param batch Batch statement to execute
   * @param name Name to execute the batch statement as
   * @param assert_ok True if error code for future should be asserted
   *                  CASS_OK; false otherwise (default: true)
   * @return Result object
   */
  Result execute_as(driver::Batch batch, const std::string& name, bool assert_ok = true) {
    return execute_as(Batch(batch), name, assert_ok);
  }

  /**
   * Execute a DSE batch statement synchronously as another user
   *
   * @param batch Batch statement to execute
   * @param name Name to execute the batch statement as
   * @param assert_ok True if error code for future should be asserted
   *                  CASS_OK; false otherwise (default: true)
   */
  Result execute_as(Batch batch, const std::string& name, bool assert_ok = true) {
    batch.set_execute_as(name);
    Future future(cass_session_execute_batch(get(), batch.get()));
    future.wait(assert_ok);
    return Result(future);
  }

  /**
   * Execute a statement synchronously as another user
   *
   * @param statement Query or bound statement to execute
   * @param name Name to execute the statement as
   * @param assert_ok True if error code for future should be asserted
   *                  CASS_OK; false otherwise (default: true)
   * @return Result object
   */
  Result execute_as(driver::Statement statement, const std::string& name, bool assert_ok = true) {
    return execute_as(Statement(statement), name, assert_ok);
  }

  /**
   * Execute a DSE statement synchronously as another user
   *
   * @param statement Query or bound statement to execute
   * @param name Name to execute the statement as
   * @param assert_ok True if error code for future should be asserted
   *                  CASS_OK; false otherwise (default: true)
   * @return Result object
   */
  Result execute_as(Statement statement, const std::string& name, bool assert_ok = true) {
    statement.set_execute_as(name);
    Future future(cass_session_execute(get(), statement.get()));
    future.wait(assert_ok);
    return Result(future);
  }

  /**
   * Execute a query synchronously as another user
   *
   * @param query Query to execute as a simple statement
   * @param name Name to execute the query as
   * @param consistency Consistency level to execute the query at
   *                    (default: CASS_CONSISTENCY_LOCAL_ONE)
   * @param is_idempotent True if statement is idempotent; false otherwise
   *                      (default: false)
   * @param assert_ok True if error code for future should be asserted
   *                  CASS_OK; false otherwise (default: true)
   * @return Result object
   */
  Result execute_as(const std::string& query, const std::string& name,
                    CassConsistency consistency = CASS_CONSISTENCY_LOCAL_ONE,
                    bool is_idempotent = false, bool assert_ok = true) {
    Statement statement(query);
    statement.set_consistency(consistency);
    statement.set_idempotent(is_idempotent);
    return execute_as(statement, name, assert_ok);
  }

  /**
   * Execute a DSE batch statement asynchronously
   *
   * @param batch Batch statement to execute
   * @return Future object
   */
  Future execute_async(Batch batch) {
    return Future(cass_session_execute_batch(get(), batch.get()));
  }

  /**
   * Execute a DSE statement asynchronously
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
    return driver::Session::execute_async(query, consistency, is_idempotent);
  }

  /**
   * Execute a batch statement asynchronously
   *
   * @param batch Batch statement to execute
   * @param name Name to execute the batch statement as
   * @return Future object
   */
  Future execute_async_as(driver::Batch batch, const std::string& name) {
    return execute_async_as(Batch(batch), name);
  }

  /**
   * Execute a DSE batch statement asynchronously
   *
   * @param batch Batch statement to execute
   * @param name Name to execute the batch statement as
   * @return Future object
   */
  Future execute_async_as(Batch batch, const std::string& name) {
    batch.set_execute_as(name);
    return execute_async(batch);
  }

  /**
   * Execute a statement asynchronously
   *
   * @param statement Query or bound statement to execute
   * @param name Name to execute the statement as
   * @return Future object
   */
  Future execute_async_as(driver::Statement statement, const std::string& name) {
    return execute_async_as(Statement(statement), name);
  }

  /**
   * Execute a DSE statement asynchronously
   *
   * @param statement Query or bound statement to execute
   * @param name Name to execute the statement as
   * @return Future object
   */
  Future execute_async_as(Statement statement, const std::string& name) {
    statement.set_execute_as(name);
    return Future(cass_session_execute(get(), statement.get()));
  }

  /**
   * Execute a query asynchronously
   *
   * @param query Query to execute as a simple statement
   * @param name Name to execute the statement as
   * @param consistency Consistency level to execute the query at
   *                    (default: CASS_CONSISTENCY_LOCAL_ONE)
   * @param is_idempotent True if statement is idempotent; false otherwise
   *                      (default: false)
   * @return Future object
   */
  Future execute_async_as(const std::string& query, const std::string& name,
                          CassConsistency consistency = CASS_CONSISTENCY_LOCAL_ONE,
                          bool is_idempotent = false) {
    Statement statement(query);
    statement.set_consistency(consistency);
    statement.set_idempotent(is_idempotent);
    return execute_async_as(statement, name);
  }
};

}}} // namespace test::driver::dse

#endif // __TEST_DSE_SESSION_HPP__
