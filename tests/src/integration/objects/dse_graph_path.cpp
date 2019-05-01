/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include "objects/dse_graph_path.hpp"
#include "objects/dse_graph_result.hpp"

test::driver::dse::GraphResult test::driver::dse::GraphPath::labels() { return path_.labels; }

test::driver::dse::GraphResult test::driver::dse::GraphPath::objects() { return path_.objects; }
