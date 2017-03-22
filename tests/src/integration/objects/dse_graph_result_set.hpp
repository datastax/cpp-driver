/*
  Copyright (c) 2017 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __TEST_DSE_GRAPH_RESULT_SET_HPP__
#define __TEST_DSE_GRAPH_RESULT_SET_HPP__
#include "dse.h"

#include "objects/object_base.hpp"

#include "objects/future.hpp"
#include "objects/dse_graph_result.hpp"

#include <gtest/gtest.h>

namespace test {
namespace driver {

/**
 * Wrapped DSE graph result set object
 */
class DseGraphResultSet : public Object< ::DseGraphResultSet, dse_graph_resultset_free> {
public:
  class Exception : public test::Exception {
  public:
    Exception(const std::string& message)
      : test::Exception(message) {}
  };

  /**
   * Create the DSE graph result set object from the native driver DSE graph
   * result set object
   *
   * @param result_set Native driver object
   */
  DseGraphResultSet(::DseGraphResultSet* result_set)
    : Object< ::DseGraphResultSet, dse_graph_resultset_free>(result_set)
    , index_(0) {}

  /**
   * Create the DSE graph result set object from the shared reference
   *
   * @param result_set Shared reference
   */
  DseGraphResultSet(Ptr result_set)
    : Object< ::DseGraphResultSet, dse_graph_resultset_free>(result_set)
    , index_(0) {}

  /**
   * Create the DSE graph result set object from a future object
   *
   * @param future Wrapped driver object
   */
  DseGraphResultSet(Future future)
    : Object< ::DseGraphResultSet, dse_graph_resultset_free>(
        cass_future_get_dse_graph_resultset(future.get()))
    , future_(future)
    , index_(0) {}

  /**
   * Get the error code from the future
   *
   * @return Error code of the future
   * @throws DseGraphResultSet::Exception
   */
  CassError error_code() {
    if (!future_) {
      throw Exception("Future is invalid or was not used to create instance");
    }
    return future_.error_code();
  }

  /**
   * Get the human readable description of the error code
   *
   * @return Error description
   * @throws DseGraphResultSet::Exception
   */
  const std::string error_description() {
    if (!future_) {
      throw Exception("Future is invalid or was not used to create instance");
    }
    return future_.error_description();
  }

  /**
   * Get the error message of the future if an error occurred
   *
   * @return Error message
   * @throws DseGraphResultSet::Exception
   */
  const std::string error_message() {
    if (!future_) {
      throw Exception("Future is invalid or was not used to create instance");
    }
    return future_.error_message();
  }

  /**
   * Get the host address of the future
   *
   * @return Host address
   * @throws DseGraphResultSet::Exception
   */
  const std::string host_address() {
    if (!future_) {
      throw Exception("Future is invalid or was not used to create instance");
    }
    return future_.host();
  }

  /**
   * Get the number of results from the DSE graph result set
   *
   * @return The number of results in the DSE graph result set
   */
  size_t count() {
    return dse_graph_resultset_count(get());
  }

  /**
   * Get the current index into the DSE graph result set
   *
   * @return Current index into the DSE graph result set
   */
  size_t index() {
    return index_;
  }

  /**
   * Get the next DSE graph result from the DSE graph result set
   *
   * @return DSE graph result
   */
  DseGraphResult next() {
    try {
      DseGraphResult result(dse_graph_resultset_next(get()));
      ++index_;
      return result;
    } catch (DseGraphResult::Exception& e) {
      throw e;
    }
    return NULL;
  }

  /**
   * Generate a JSON style string for the DSE graph result
   *
   * NOTE: This can only be used once as it will invalidate the DSE graph result
   *       set iterator and the DSE graph result set cannot be reset due to the
   *       object template hiding access (private) to the reference object
   *       member.
   *
   * @param indent Number of characters/spaces to indent
   * @return JSON style string representing the DSE graph result
   */
  const std::string str(unsigned int indent = 0) {
    size_t reamining_count = count() - index();
    std::stringstream output;

    // Determine if the output can be generated
    if (reamining_count > 0) {
      output << Utils::indent("[", indent) << std::endl;
      for (size_t i = 0; i < reamining_count; ++i) {
        output << next().str(indent + INDENT_INCREMENT);
        if (i != (reamining_count - 1)) {
          output << "," << std::endl;
        }
      }
      output << std::endl << Utils::indent("]", indent);
    }

    // Return the output
    return output.str();
  }

private:
  /**
   * Future wrapped object
   */
  Future future_;
  /**
   * Counter variable to determine the current DSE graph result set index
   */
  size_t index_;

};

} // namespace driver
} // namespace test

#endif // __TEST_DSE_GRAPH_RESULT_SET_HPP__
