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

#ifndef __TEST_STATEMENT_HPP__
#define __TEST_STATEMENT_HPP__
#include "cassandra.h"

#include "objects/object_base.hpp"

#include "objects/future.hpp"
#include "objects/retry_policy.hpp"

#include "driver_utils.hpp"

#include <gtest/gtest.h>

namespace test {
namespace driver {

// Forward declaration for circular dependency
class CustomPayload;

/**
 * Wrapped statement object
 */
class Statement : public Object<CassStatement, cass_statement_free> {
public:
  /**
   * Create an empty statement
   */
  Statement()
    : Object<CassStatement, cass_statement_free>() { }
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
   *                        (default: 0)
   */
  Statement(const std::string& query, size_t parameter_count = 0)
    : Object<CassStatement, cass_statement_free>(cass_statement_new(query.c_str(),
      parameter_count)) {}

  /**
   * Bind a value to the statement
   *
   * @param index Index to bind the value to
   * @param value<T> Value to bind to the statement at given index
   */
  template<typename T>
  void bind(size_t index, T value) {
    value.statement_bind(*this, index);
  }

  /**
   * Bind a value to the statement by name
   *
   * @param name Column name to bind the value to
   * @param value<T> Value to bind to the statement at given index
   */
  template<typename T>
  void bind(const std::string& name, T value) {
    value.statement_bind(*this, name);
  }

  /**
   * Assign/Set the statement's consistency level
   *
   * @param consistency Consistency to use for the statement
   */
  void set_consistency(CassConsistency consistency) {
    ASSERT_EQ(CASS_OK, cass_statement_set_consistency(get(), consistency));
  }

  /**
   * Assign/Set the statement's custom payload
   *
   * @param custom_payload Custom payload to use for the statement
   */
  void set_custom_payload(CustomPayload custom_payload);

  /**
   * Enable/Disable whether the statement is idempotent. Idempotent statements
   * are able to be automatically retried after timeouts/errors and can be
   * speculatively executed.
   *
   * @param enable True if statement is idempotent; false otherwise
   */
  void set_idempotent(bool enable) {
    ASSERT_EQ(CASS_OK, cass_statement_set_is_idempotent(get(),
      enable ? cass_true : cass_false));
  }

  /**
   * Enable/Disable the statement's recording of hosts attempted during its
   * execution
   *
   * @param enable True if attempted host should be recorded; false otherwise
   */
  void set_record_attempted_hosts(bool enable) {
    return internals::Utils::set_record_attempted_hosts(get(),
      enable);
  }

  /**
   * Assign/Set the statement's retry policy
   *
   * @param retry_policy Retry policy to use for the statement
   */
  void set_retry_policy(RetryPolicy retry_policy) {
    ASSERT_EQ(CASS_OK, cass_statement_set_retry_policy(get(),
      retry_policy.get()));
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

  /**
   * Assign/Set the batch's consistency level
   *
   * @param consistency Consistency to use for the batch
   */
  void set_consistency(CassConsistency consistency) {
    ASSERT_EQ(CASS_OK, cass_batch_set_consistency(get(), consistency));
  }

  /**
   * Enable/Disable whether the statements in a batch are idempotent. Idempotent
   * batches are able to be automatically retried after timeouts/errors and can
   * be speculatively executed.
   *
   * @param enable True if statement in a batch is idempotent; false otherwise
   */
  void set_idempotent(bool enable) {
    ASSERT_EQ(CASS_OK, cass_batch_set_is_idempotent(get(),
      enable ? cass_true : cass_false));
  }

  /**
   * Assign/Set the batch's retry policy
   *
   * @param retry_policy Retry policy to use for the batch
   */
  void set_retry_policy(RetryPolicy retry_policy) {
    ASSERT_EQ(CASS_OK, cass_batch_set_retry_policy(get(), retry_policy.get()));
  }
};

} // namespace driver
} // namespace test

#endif // __TEST_STATEMENT_HPP__
