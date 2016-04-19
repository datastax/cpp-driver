/*
  Copyright (c) 2014-2016 DataStax

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
#ifndef __TEST_STATEMENT_HPP__
#define __TEST_STATEMENT_HPP__
#include "cassandra.h"

#include "objects/object_base.hpp"

#include "objects/future.hpp"

namespace test {
namespace driver {

/**
 * Wrapped statement object
 */
class Statement : public Object<CassStatement, cass_statement_free> {
public:
  /**
   * Create the statement object from the native driver statement object
   *
   * @param statement Native driver object
   */
  Statement(CassStatement* statement)
    : Object<CassStatement, cass_statement_free>(statement) {}

  /**
   * Create the statement object from the shared reference
   *
   * @param statement Shared reference
   */
  Statement(Ptr statement)
    : Object<CassStatement, cass_statement_free>(statement) {}

  /**
   * Create the statement object from a query
   *
   * @param query Query to create statement from
   * @param parameter_count Number of parameters contained in the query
   */
  Statement(const std::string& query, size_t parameter_count)
    : Object<CassStatement, cass_statement_free>(cass_statement_new(query.c_str(), parameter_count)) {}

  /**
   * Bind a value to the statement
   *
   * @param index Index to bind the value to
   * @param value<T> Value to bind to the statement at given index
   */
  template<typename T>
  void bind(size_t index, T value) {
    ASSERT_TRUE(object_) << "Invalid Statement: Statement should not be null";
    value.statement_bind(get(), index);
  }
};

/**
 * Wrapped batch object
 */
class Batch : public Object<CassBatch, cass_batch_free> {
public:
  /**
   * Create the batch object based on the type of batch statement to use
   *
   * @param batch_type Type of batch to create (default: Unlogged)
   */
  Batch(CassBatchType batch_type = CASS_BATCH_TYPE_UNLOGGED)
    : Object<CassBatch, cass_batch_free>(cass_batch_new(batch_type)) {}

  /**
   * Create the batch object from the native driver batch object
   *
   * @param batch Native driver object
   */
  Batch(CassBatch* batch)
    : Object<CassBatch, cass_batch_free>(batch) {}

  /**
   * Create the batch object from the shared reference
   *
   * @param batch Shared reference
   */
  Batch(Ptr batch)
    : Object<CassBatch, cass_batch_free>(batch) {}

  /**
   * Add a statement (query or bound) to the batch
   *
   * @param statement Query or bound statement to add
   * @param assert_ok True if error code for future should be asserted
   *                  CASS_OK; false otherwise (default: true)
   */
  void add(Statement statement, bool assert_ok = true) {
    CassError error_code = cass_batch_add_statement(get(),
      statement.get());
    if (assert_ok) {
      ASSERT_EQ(CASS_OK, error_code)
        << "Unable to Add Statement to Batch: " << cass_error_desc(error_code);
    }
  }
};

} // namespace driver
} // namespace test

#endif // __TEST_STATEMENT_HPP__

