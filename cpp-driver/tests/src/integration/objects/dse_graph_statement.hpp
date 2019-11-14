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

#ifndef __TEST_DSE_GRAPH_STATEMENT_HPP__
#define __TEST_DSE_GRAPH_STATEMENT_HPP__
#include "dse.h"

#include "objects/object_base.hpp"

#include "objects/dse_graph_object.hpp"
#include "objects/dse_graph_options.hpp"

#include <gtest/gtest.h>

namespace test { namespace driver { namespace dse {

/**
 * Wrapped DSE graph statement object
 */
class GraphStatement : public Object<DseGraphStatement, dse_graph_statement_free> {
public:
  /**
   * Create the DSE graph statement object from the native driver DSE graph
   * statement object
   *
   * @param statement Native driver object
   */
  GraphStatement(DseGraphStatement* statement)
      : Object<DseGraphStatement, dse_graph_statement_free>(statement) {}

  /**
   * Create the DSE graph statement object from the shared reference
   *
   * @param statement Shared reference
   */
  GraphStatement(Ptr statement)
      : Object<DseGraphStatement, dse_graph_statement_free>(statement) {}

  /**
   * Create the statement object from a query without options
   *
   * @param query Query to create statement from
   */
  GraphStatement(const std::string& query)
      : Object<DseGraphStatement, dse_graph_statement_free>(
            dse_graph_statement_new(query.c_str(), NULL)) {}

  /**
   * Create the statement object from a query
   *
   * @param query Query to create statement from
   * @param options Graph options to apply to the graph statement
   */
  GraphStatement(const std::string& query, GraphOptions options)
      : Object<DseGraphStatement, dse_graph_statement_free>(
            dse_graph_statement_new(query.c_str(), options.get())) {}

  /**
   * Bind the DSE graph object (values) to the DSE graph statement
   *
   * @param object DSE graph object to bind to the DSE graph statement
   */
  void bind(GraphObject object) {
    object.finish();
    ASSERT_EQ(CASS_OK, dse_graph_statement_bind_values(get(), object.get()));
  }

  /**
   * Sets the graph statement's timestamp
   *
   * @param timestamp Timestamp (in milliseconds since EPOCH) to apply
   */
  void set_timestamp(cass_int64_t timestamp) {
    ASSERT_EQ(CASS_OK, dse_graph_statement_set_timestamp(get(), timestamp));
  }
};

}}} // namespace test::driver::dse

#endif // __TEST_DSE_GRAPH_STATEMENT_HPP__
