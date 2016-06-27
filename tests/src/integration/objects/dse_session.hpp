/*
  Copyright (c) 2016 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __TEST_DSE_SESSION_HPP__
#define __TEST_DSE_SESSION_HPP__
#include "objects/dse_cluster.hpp"

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
   * Execute a graph statement synchronously
   *
   * @param graph Graph statement to execute
   * @param assert_ok True if error code for future should be asserted
   *                  CASS_OK; false otherwise (default: true)
   * @return DSE graph result object
   */
  DseGraphResultSet execute(DseGraphStatement graph, bool assert_ok = true) {
    Future future(cass_session_execute_dse_graph(get(), graph.get()));
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
};

} // namespace driver
} // namespace test

#endif // __TEST_DSE_SESSION_HPP__
