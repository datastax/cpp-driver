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

#ifndef TEST_ERROR_RESULT_HPP
#define TEST_ERROR_RESULT_HPP

#include "cassandra.h"

#include "objects/future.hpp"
#include "objects/object_base.hpp"

#include <gtest/gtest.h>

namespace test { namespace driver {

/**
 * Wrapped error result object
 */
class ErrorResult : public Object<const CassErrorResult, cass_error_result_free> {
public:
  /**
   * Create an empty error result object
   */
  ErrorResult() {}

  /**
   * Create the error result object from the native driver object
   *
   * @param result Native driver object
   */
  ErrorResult(Future future)
      : Object<const CassErrorResult, cass_error_result_free>(future.error_result()) {}

  CassError error_code() const { return cass_error_result_code(get()); }

  CassConsistency consistency() const { return cass_error_result_consistency(get()); }

  int32_t responses_received() const { return cass_error_result_responses_received(get()); }

  int32_t responses_required() const { return cass_error_result_responses_required(get()); }

  int32_t num_failures() const { return cass_error_result_num_failures(get()); }

  bool data_present() const { return cass_error_result_data_present(get()) == cass_true; }

  CassWriteType write_type() const { return cass_error_result_write_type(get()); }

  std::string keyspace() const {
    const char* keyspace;
    size_t keyspace_length;
    EXPECT_EQ(CASS_OK, cass_error_result_keyspace(get(), &keyspace, &keyspace_length));
    return std::string(keyspace, keyspace_length);
  }

  std::string table() const {
    const char* table;
    size_t table_length;
    EXPECT_EQ(CASS_OK, cass_error_result_table(get(), &table, &table_length));
    return std::string(table, table_length);
  }

  std::string function() const {
    const char* function;
    size_t function_length;
    EXPECT_EQ(CASS_OK, cass_error_result_function(get(), &function, &function_length));
    return std::string(function, function_length);
  }

  size_t num_arg_types() const { return cass_error_num_arg_types(get()); }

  std::string arg_type(size_t index) const {
    const char* arg_type;
    size_t arg_type_length;
    EXPECT_EQ(CASS_OK, cass_error_result_arg_type(get(), index, &arg_type, &arg_type_length));
    return std::string(arg_type, arg_type_length);
  }
};

}} // namespace test::driver

#endif // TEST_ERROR_RESULT_HPP
