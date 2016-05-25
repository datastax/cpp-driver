/*
  Copyright (c) 2014-2016 DataStax
*/
#include "objects/dse_graph_edge.hpp"
#include "objects/dse_graph_result.hpp"

test::driver::DseGraphResult test::driver::DseGraphEdge::id() {
  return edge_.id;
}

test::driver::DseGraphResult test::driver::DseGraphEdge::label() {
  return edge_.label;
}

test::driver::DseGraphResult test::driver::DseGraphEdge::type() {
  return edge_.type;
}

test::driver::DseGraphResult test::driver::DseGraphEdge::properties() {
  return edge_.properties;
}

test::driver::DseGraphVertex test::driver::DseGraphEdge::in_vertex() {
  DseGraphResult vertex(edge_.in_vertex);
  return vertex.vertex();
}

test::driver::DseGraphResult test::driver::DseGraphEdge::in_vertex_label() {
  return edge_.in_vertex_label;
}

test::driver::DseGraphVertex test::driver::DseGraphEdge::out_vertex() {
  DseGraphResult vertex(edge_.out_vertex);
  return vertex.vertex();
}

test::driver::DseGraphResult test::driver::DseGraphEdge::out_vertex_label() {
  return edge_.out_vertex_label;
}
