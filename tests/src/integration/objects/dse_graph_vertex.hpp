/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __TEST_DSE_GRAPH_VERTEX_HPP__
#define __TEST_DSE_GRAPH_VERTEX_HPP__
#include "dse.h"

#include <string>

namespace test {
namespace driver {
namespace dse {

// Forward declaration for circular dependency
class GraphResult;

/**
 * Wrapped DSE graph vertex object
 */
class GraphVertex {
public:
  /**
   * Create the DSE graph vertex object from the native driver DSE graph vertex
   * object
   *
   * @param vertex Native driver object
   */
  GraphVertex(DseGraphVertexResult vertex)
    : vertex_(vertex) {}

  /**
   * Get the DSE graph vertex id
   *
   * @return DSE graph result representing the id
   */
  GraphResult id();

  /**
   * Get the DSE graph vertex label
   *
   * @return DSE graph result representing the label
   */
  GraphResult label();

  /**
   * Get the DSE graph vertex type
   *
   * @return DSE graph result representing the type
   */
  GraphResult type();

  /**
   * Get the DSE graph vertex properties
   *
   * @return DSE graph result representing the properties
   */
  GraphResult properties();

private:
  /**
   * Native driver object
   */
  DseGraphVertexResult vertex_;
};

} // namespace dse
} // namespace driver
} // namespace test

#endif // __TEST_DSE_GRAPH_VERTEX_HPP__
