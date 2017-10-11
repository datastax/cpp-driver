/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include "objects/dse_graph_edge.hpp"
#include "objects/dse_graph_result.hpp"

test::driver::dse::GraphResult test::driver::dse::GraphEdge::id() {
  return edge_.id;
}

test::driver::dse::GraphResult test::driver::dse::GraphEdge::label() {
  return edge_.label;
}

test::driver::dse::GraphResult test::driver::dse::GraphEdge::type() {
  return edge_.type;
}

test::driver::dse::GraphResult test::driver::dse::GraphEdge::properties() {
  return edge_.properties;
}

test::driver::dse::GraphVertex test::driver::dse::GraphEdge::in_vertex() {
  GraphResult vertex(edge_.in_vertex);
  return vertex.vertex();
}

test::driver::dse::GraphResult test::driver::dse::GraphEdge::in_vertex_label() {
  return edge_.in_vertex_label;
}

test::driver::dse::GraphVertex test::driver::dse::GraphEdge::out_vertex() {
  GraphResult vertex(edge_.out_vertex);
  return vertex.vertex();
}

test::driver::dse::GraphResult test::driver::dse::GraphEdge::out_vertex_label() {
  return edge_.out_vertex_label;
}
