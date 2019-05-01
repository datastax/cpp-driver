/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
