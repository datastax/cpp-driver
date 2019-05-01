/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include "objects/dse_graph_vertex.hpp"
#include "objects/dse_graph_result.hpp"

test::driver::dse::GraphResult test::driver::dse::GraphVertex::id() { return vertex_.id; }

test::driver::dse::GraphResult test::driver::dse::GraphVertex::label() { return vertex_.label; }

test::driver::dse::GraphResult test::driver::dse::GraphVertex::type() { return vertex_.type; }

test::driver::dse::GraphResult test::driver::dse::GraphVertex::properties() {
  return vertex_.properties;
}
