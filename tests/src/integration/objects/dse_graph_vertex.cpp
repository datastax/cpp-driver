/*
  Copyright (c) 2014-2016 DataStax
*/
#include "objects/dse_graph_vertex.hpp"
#include "objects/dse_graph_result.hpp"

test::driver::DseGraphResult test::driver::DseGraphVertex::id() {
  return vertex_.id;
}

test::driver::DseGraphResult test::driver::DseGraphVertex::label() {
  return vertex_.label;
}

test::driver::DseGraphResult test::driver::DseGraphVertex::type() {
  return vertex_.type;
}

test::driver::DseGraphResult test::driver::DseGraphVertex::properties() {
  return vertex_.properties;
}
