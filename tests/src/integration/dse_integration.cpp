/*
  Copyright (c) 2014-2016 DataStax
*/
#include "dse_integration.hpp"

#define GRAPH_CREATE \
  "system.graph(name).option('graph.replication_config').set(replication)" \
  ".option('graph.system_replication_config').set(replication)" \
  ".option('graph.traversal_sources.g.evaluation_timeout').set(duration)" \
  ".ifNotExists().create();"

#define GRAPH_ALLOW_SCANS \
  "schema.config().option('graph.allow_scan').set('true')"

#define GRAPH_ENABLE_STRICT \
  "schema.config().option('graph.schema_mode').set('production')"

DseIntegration::DseIntegration()
  : Integration()
  , dse_session_() {}

void DseIntegration::SetUp() {
  // Call the parent setup class
  Integration::SetUp();

  // Create the DSE session object from the Cassandra session object
  dse_session_ = session_;
}

void DseIntegration::connect(Cluster cluster) {
  // Call the parent connect function
  Integration::connect(cluster);
  dse_session_ = session_;
}

void DseIntegration::connect() {
  // Call the parent connect function
  Integration::connect();
  dse_session_ = session_;
}

void DseIntegration::create_graph(const std::string& graph_name,
  const std::string& replication_strategy,
  const std::string& duration) {
    // Create the graph statement using the pre-determined replication config
    test::driver::DseGraphObject graph_object;
    graph_object.add<std::string>("name", graph_name);
    graph_object.add<std::string>("replication", replication_strategy);
    graph_object.add<std::string>("duration", duration);
    test::driver::DseGraphStatement graph_statement(GRAPH_CREATE);
    graph_statement.bind(graph_object);
    CHECK_FAILURE;

    // Execute the graph statement and ensure it was created
    dse_session_.execute(graph_statement);
    CHECK_FAILURE;

    // Enable testing functionality for the graph
    test::driver::DseGraphOptions options;
    options.set_name(graph_name);
    dse_session_.execute(GRAPH_ALLOW_SCANS, options);
    CHECK_FAILURE;
    dse_session_.execute(GRAPH_ENABLE_STRICT, options);
    CHECK_FAILURE;
  }

void DseIntegration::create_graph(const std::string& duration /*= "PT30S"*/) {
  create_graph(test_name_, replication_strategy_, duration);
}
