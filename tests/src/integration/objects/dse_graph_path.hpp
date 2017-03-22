/*
  Copyright (c) 2017 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __TEST_DSE_GRAPH_PATH_HPP__
#define __TEST_DSE_GRAPH_PATH_HPP__
#include "dse.h"

namespace test {
namespace driver {

// Forward declaration for circular dependency
class DseGraphResult;

/**
 * Wrapped DSE graph edge object
 */
class DseGraphPath {
public:
  /**
   * Create the DSE graph path object from the native driver DSE graph path
   * object
   *
   * @param edge Native driver object
   */
  DseGraphPath(DseGraphPathResult path)
    : path_(path) {}

  /**
   * Get the DSE graph edge label
   *
   * @return DSE graph result representing the path labels
   */
  DseGraphResult labels();

  /**
   * Get the DSE graph path objects
   *
   * @return DSE graph result representing the path objects
   */
  DseGraphResult objects();

private:
  /**
   * Native driver object
   */
  DseGraphPathResult path_;
};

} // namespace driver
} // namespace test

#endif // __TEST_DSE_GRAPH_PATH_HPP__
