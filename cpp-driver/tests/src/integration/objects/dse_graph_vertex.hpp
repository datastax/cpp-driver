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

#ifndef __TEST_DSE_GRAPH_VERTEX_HPP__
#define __TEST_DSE_GRAPH_VERTEX_HPP__
#include "dse.h"

#include <string>

namespace test { namespace driver { namespace dse {

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

}}} // namespace test::driver::dse

#endif // __TEST_DSE_GRAPH_VERTEX_HPP__
