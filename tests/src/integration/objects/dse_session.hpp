/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __TEST_DSE_SESSION_HPP__
#define __TEST_DSE_SESSION_HPP__
#include "objects/dse_cluster.hpp"

#include "objects/dse_statement.hpp"
#include "objects/dse_graph_statement.hpp"
#include "objects/dse_graph_result_set.hpp"

namespace test {
namespace driver {

/**
 * Wrapped DSE session object
 */
class DseSession : public Session {
friend class DseCluster;
public:
  /**
   * Create the default DSE session object
   */
  DseSession()
    : Session() {}

  /**
   * Create the DSE session object from the native driver object
   *
   * @param session Native driver object
   */
  DseSession(CassSession* session)
    : Session(session) {}

  /**
   * Create the DSE session object from a shared reference
   *
   * @param session Shared reference
   */
  DseSession(Ptr session)
    : Session(session) {}

  /**
   * Create the DSE session object from a wrapped session
   *
   * @param session Wrapped session object
   */
  DseSession(Session session)
    : Session(session) {}

  /**
   * Execute a DSE batch statement synchronously
   *
   * @param batch Batch statement to execute
   * @param assert_ok True if error code for future should be asserted
   *                  CASS_OK; false otherwise (default: true)
   * @return Result object
   */
  Result execute(DseBatch batch, bool assert_ok = true) {
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
  Result execute(DseStatement statement, bool assert_ok = true) {
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
  Result execute(const std::string& query,
    CassConsistency consistency = CASS_CONSISTENCY_LOCAL_ONE,
    bool is_idempotent = false, bool assert_ok = true) {
    return Session::execute(query, consistency, is_idempotent, assert_ok);
  }

  /**
   * Execute a graph statement synchronously
   *
   * @param graph Graph statement to execute
   * @param assert_ok True if error code for future should be asserted
   *                  CASS_OK; false otherwise (default: true)
   * @return DSE graph result object
   */
  DseGraphResultSet execute(DseGraphStatement graph, bool assert_ok = true) {
    Future future(execute_async(graph));
    future.wait(assert_ok);
    return DseGraphResultSet(future);
  }

  /**
   * Execute a graph query synchronously
   *
   * @param query Query to execute as a graph statement
   * @param options Graph options to apply to the graph statement
   * @param assert_ok True if error code for future should be asserted
   *                  CASS_OK; false otherwise (default: true)
   * @return DSE graph result object
   */
  DseGraphResultSet execute(const std::string& query, DseGraphOptions options,
    bool assert_ok = true) {
    // Allow for NULL graph options without throwing an exception
    ::DseGraphOptions* graph_options = NULL;
    if (options) {
      graph_options = options.get();
    }

    // Create and execute the graph statement
    DseGraphStatement statement(dse_graph_statement_new(query.c_str(), graph_options));
    return execute(statement, assert_ok);
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
  Result execute_as(Batch batch, const std::string& name, bool assert_ok = true) {
    return execute_as(DseBatch(batch), name, assert_ok);
  }

  /**
   * Execute a DSE batch statement synchronously as another user
   *
   * @param batch Batch statement to execute
   * @param name Name to execute the batch statement as
   * @param assert_ok True if error code for future should be asserted
   *                  CASS_OK; false otherwise (default: true)
   */
  Result execute_as(DseBatch batch, const std::string& name, bool assert_ok = true) {
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
  Result execute_as(Statement statement, const std::string& name,
    bool assert_ok = true) {
    return execute_as(DseStatement(statement), name, assert_ok);
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
  Result execute_as(DseStatement statement, const std::string& name,
    bool assert_ok = true) {
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
    DseStatement statement(query);
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
  Future execute_async(DseBatch batch) {
    return Future(cass_session_execute_batch(get(), batch.get()));
  }

  /**
   * Execute a DSE statement asynchronously
   *
   * @param statement Query or bound statement to execute
   * @return Future object
   */
  Future execute_async(DseStatement statement) {
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
    return Session::execute_async(query, consistency, is_idempotent);
  }

  /**
   * Execute a graph statement asynchronously
   *
   * @param graph Graph statement to execute
   * @return Future object
   */
  Future execute_async(DseGraphStatement graph) {
    return Future(cass_session_execute_dse_graph(get(), graph.get()));
  }

  /**
   * Execute a graph query asynchronously
   *
   * @param query Query to execute as a graph statement
   * @param options Graph options to apply to the graph statement
   * @return Future object
   */
  Future execute_async(const std::string& query, DseGraphOptions options) {
    DseGraphStatement statement(dse_graph_statement_new(query.c_str(), options.get()));
    return execute_async(statement);
  }

  /**
   * Execute a batch statement asynchronously
   *
   * @param batch Batch statement to execute
   * @param name Name to execute the batch statement as
   * @return Future object
   */
  Future execute_async_as(Batch batch, const std::string& name) {
    return execute_async_as(DseBatch(batch), name);
  }

  /**
   * Execute a DSE batch statement asynchronously
   *
   * @param batch Batch statement to execute
   * @param name Name to execute the batch statement as
   * @return Future object
   */
  Future execute_async_as(DseBatch batch, const std::string& name) {
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
  Future execute_async_as(Statement statement, const std::string& name) {
    return execute_async_as(DseStatement(statement), name);
  }

  /**
   * Execute a DSE statement asynchronously
   *
   * @param statement Query or bound statement to execute
   * @param name Name to execute the statement as
   * @return Future object
   */
  Future execute_async_as(DseStatement statement, const std::string& name) {
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
    DseStatement statement(query);
    statement.set_consistency(consistency);
    statement.set_idempotent(is_idempotent);
    return execute_async_as(statement, name);
  }
};

} // namespace driver
} // namespace test

#endif // __TEST_DSE_SESSION_HPP__
