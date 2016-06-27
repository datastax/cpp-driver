/*
  Copyright (c) 2016 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
