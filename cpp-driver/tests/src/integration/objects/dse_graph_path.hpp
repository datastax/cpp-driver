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

#ifndef __TEST_DSE_GRAPH_PATH_HPP__
#define __TEST_DSE_GRAPH_PATH_HPP__
#include "dse.h"

namespace test { namespace driver { namespace dse {

// Forward declaration for circular dependency
class GraphResult;

/**
 * Wrapped DSE graph edge object
 */
class GraphPath {
public:
  /**
   * Create the DSE graph path object from the native driver DSE graph path
   * object
   *
   * @param edge Native driver object
   */
  GraphPath(DseGraphPathResult path)
      : path_(path) {}

  /**
   * Get the DSE graph edge label
   *
   * @return DSE graph result representing the path labels
   */
  GraphResult labels();

  /**
   * Get the DSE graph path objects
   *
   * @return DSE graph result representing the path objects
   */
  GraphResult objects();

private:
  /**
   * Native driver object
   */
  DseGraphPathResult path_;
};

}}} // namespace test::driver::dse

#endif // __TEST_DSE_GRAPH_PATH_HPP__
