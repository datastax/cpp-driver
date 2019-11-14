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

#ifndef __TEST_DSE_GRAPH_EDGE_HPP__
#define __TEST_DSE_GRAPH_EDGE_HPP__
#include "dse.h"

#include "objects/dse_graph_vertex.hpp"

namespace test { namespace driver { namespace dse {

// Forward declaration for circular dependency
class GraphResult;

/**
 * Wrapped DSE graph edge object
 */
class GraphEdge {
public:
  /**
   * Create the DSE graph edge object from the native driver DSE graph edge
   * object
   *
   * @param edge Native driver object
   */
  GraphEdge(DseGraphEdgeResult edge)
      : edge_(edge) {}

  /**
   * Get the DSE graph edge id
   *
   * @return DSE graph result representing the id
   */
  GraphResult id();

  /**
   * Get the DSE graph edge label
   *
   * @return Edge label
   */
  GraphResult label();

  /**
   * Get the DSE graph edge type
   *
   * @return DSE graph result type
   */
  GraphResult type();

  /**
   * Get the DSE graph edge properties
   *
   * @return DSE graph result representing the properties
   */
  GraphResult properties();

  /**
   * Get the DSE graph edge incoming/head vertex
   *
   * @return Incoming/Head vertex
   */
  GraphVertex in_vertex();

  /**
   * Get the DSE graph edge incoming/head vertex label
   *
   * @return Incoming/Head vertex label
   */
  GraphResult in_vertex_label();

  /**
   * Get the DSE graph edge outgoing/tail vertex
   *
   * @return Outgoing/Tail vertex
   */
  GraphVertex out_vertex();

  /**
   * Get the DSE graph edge outgoing/tail vertex label
   *
   * @return Outgoing/Tail vertex label
   */
  GraphResult out_vertex_label();

private:
  /**
   * Native driver object
   */
  DseGraphEdgeResult edge_;
};

}}} // namespace test::driver::dse

#endif // __TEST_DSE_GRAPH_EDGE_HPP__
