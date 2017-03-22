/*
  Copyright (c) 2017 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __TEST_DSE_GRAPH_EDGE_HPP__
#define __TEST_DSE_GRAPH_EDGE_HPP__
#include "dse.h"

#include "objects/dse_graph_vertex.hpp"

namespace test {
namespace driver {

// Forward declaration for circular dependency
class DseGraphResult;

/**
 * Wrapped DSE graph edge object
 */
class DseGraphEdge {
public:
  /**
   * Create the DSE graph edge object from the native driver DSE graph edge
   * object
   *
   * @param edge Native driver object
   */
  DseGraphEdge(DseGraphEdgeResult edge)
    : edge_(edge) {}

  /**
   * Get the DSE graph edge id
   *
   * @return DSE graph result representing the id
   */
  DseGraphResult id();

  /**
   * Get the DSE graph edge label
   *
   * @return Edge label
   */
  DseGraphResult label();

  /**
   * Get the DSE graph edge type
   *
   * @return DSE graph result type
   */
  DseGraphResult type();

  /**
   * Get the DSE graph edge properties
   *
   * @return DSE graph result representing the properties
   */
  DseGraphResult properties();

  /**
   * Get the DSE graph edge incoming/head vertex
   *
   * @return Incoming/Head vertex
   */
  DseGraphVertex in_vertex();

  /**
   * Get the DSE graph edge incoming/head vertex label
   *
   * @return Incoming/Head vertex label
   */
  DseGraphResult in_vertex_label();

  /**
   * Get the DSE graph edge outgoing/tail vertex
   *
   * @return Outgoing/Tail vertex
   */
  DseGraphVertex out_vertex();

  /**
   * Get the DSE graph edge outgoing/tail vertex label
   *
   * @return Outgoing/Tail vertex label
   */
  DseGraphResult out_vertex_label();

private:
  /**
   * Native driver object
   */
  DseGraphEdgeResult edge_;
};

} // namespace driver
} // namespace test

#endif // __TEST_DSE_GRAPH_EDGE_HPP__
