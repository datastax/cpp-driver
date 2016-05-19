/*
  Copyright (c) 2014-2016 DataStax
*/
#include "objects/dse_graph_path.hpp"
#include "objects/dse_graph_result.hpp"

test::driver::DseGraphResult test::driver::DseGraphPath::labels() {
  return path_.labels;
}

test::driver::DseGraphResult test::driver::DseGraphPath::objects() {
  return path_.objects;
}
