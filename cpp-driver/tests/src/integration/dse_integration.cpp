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

#include "dse_integration.hpp"

#define GRAPH_CREATE                                                       \
  "system.graph(name).option('graph.replication_config').set(replication)" \
  ".option('graph.system_replication_config').set(replication)"            \
  ".option('graph.traversal_sources.g.evaluation_timeout').set(duration)"  \
  ".ifNotExists()"

#define GRAPH_ALLOW_SCANS "schema.config().option('graph.allow_scan').set('true')"

#define GRAPH_ENABLE_STRICT "schema.config().option('graph.schema_mode').set('production')"

#define GRAPH_SCHEMA                                                                  \
  "schema.propertyKey('name').Text().ifNotExists().create();"                         \
  "schema.propertyKey('age').Int().ifNotExists().create();"                           \
  "schema.propertyKey('lang').Text().ifNotExists().create();"                         \
  "schema.propertyKey('weight').Float().ifNotExists().create();"                      \
  "schema.vertexLabel('person').properties('name', 'age').ifNotExists().create();"    \
  "schema.vertexLabel('software').properties('name', 'lang').ifNotExists().create();" \
  "schema.edgeLabel('created').properties('weight').connection('person', "            \
  "'software').ifNotExists().create();"                                               \
  "schema.edgeLabel('created').connection('software', 'software').add();"             \
  "schema.edgeLabel('knows').properties('weight').connection('person', "              \
  "'person').ifNotExists().create();"

#define GRAPH_DATA                                                                        \
  "Vertex marko = graph.addVertex(label, 'person', 'name', 'marko', 'age', 29);"          \
  "Vertex vadas = graph.addVertex(label, 'person', 'name', 'vadas', 'age', 27);"          \
  "Vertex lop = graph.addVertex(label, 'software', 'name', 'lop', 'lang', 'java');"       \
  "Vertex josh = graph.addVertex(label, 'person', 'name', 'josh', 'age', 32);"            \
  "Vertex ripple = graph.addVertex(label, 'software', 'name', 'ripple', 'lang', 'java');" \
  "Vertex peter = graph.addVertex(label, 'person', 'name', 'peter', 'age', 35);"          \
  "marko.addEdge('knows', vadas, 'weight', 0.5f);"                                        \
  "marko.addEdge('knows', josh, 'weight', 1.0f);"                                         \
  "marko.addEdge('created', lop, 'weight', 0.4f);"                                        \
  "josh.addEdge('created', ripple, 'weight', 1.0f);"                                      \
  "josh.addEdge('created', lop, 'weight', 0.4f);"                                         \
  "peter.addEdge('created', lop, 'weight', 0.2f);"

DseIntegration::DseIntegration()
    : Integration()
    , dse_session_() {}

void DseIntegration::SetUp() {
  // Call the parent setup class
  Integration::SetUp();

  // Create the DSE session object from the Cassandra session object
  dse_session_ = session_;
}

void DseIntegration::connect(dse::Cluster cluster) {
  // Call the parent connect function
  Integration::connect(cluster);
  dse_session_ = session_;
}

void DseIntegration::connect() {
  // Create the cluster configuration and establish the session connection
  cluster_ = default_cluster();
  dse_cluster_ = cluster_;
  connect(dse_cluster_);
}

Cluster DseIntegration::default_cluster() {
  return dse::Cluster::build()
      .with_contact_points(contact_points_)
      .with_randomized_contact_points(is_randomized_contact_points_)
      .with_schema_metadata(is_schema_metadata_);
}

void DseIntegration::create_graph(const std::string& graph_name,
                                  const std::string& replication_strategy,
                                  const std::string& duration) {
  // Create the graph statement using the pre-determined replication config
  dse::GraphObject graph_object;
  graph_object.add<std::string>("name", graph_name);
  graph_object.add<std::string>("replication", replication_strategy);
  graph_object.add<std::string>("duration", duration);
  std::string query = GRAPH_CREATE;
  if (server_version_ >= "6.8.0") {
    query.append(".classicEngine()");
  }
  query.append(".create()");
  dse::GraphStatement graph_statement(query);
  graph_statement.bind(graph_object);
  CHECK_FAILURE;

  // Execute the graph statement and ensure it was created
  dse_session_.execute(graph_statement);
  CHECK_FAILURE;

  // Enable testing functionality for the graph
  dse::GraphOptions options;
  options.set_name(graph_name);
  dse_session_.execute(GRAPH_ALLOW_SCANS, options);
  CHECK_FAILURE;
  dse_session_.execute(GRAPH_ENABLE_STRICT, options);
  CHECK_FAILURE;
}

void DseIntegration::populate_classic_graph(const std::string& graph_name) {
  // Initialize the graph pre-populated data
  dse::GraphOptions options;
  options.set_name(graph_name);
  dse_session_.execute(GRAPH_SCHEMA, options);
  CHECK_FAILURE;
  dse_session_.execute(GRAPH_DATA, options);
  CHECK_FAILURE;
}

void DseIntegration::create_graph(const std::string& duration /*= "PT30S"*/) {
  create_graph(test_name_, replication_strategy_, duration);
}
