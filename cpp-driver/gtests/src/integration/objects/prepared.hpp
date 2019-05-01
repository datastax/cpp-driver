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

#ifndef __TEST_PREPARED_HPP__
#define __TEST_PREPARED_HPP__
#include "cassandra.h"

#include "objects/object_base.hpp"

#include "objects/statement.hpp"

namespace test { namespace driver {

/**
 * Wrapped prepared object
 */
class Prepared : public Object<const CassPrepared, cass_prepared_free> {
public:
  /**
   * Create the empty prepared object
   */
  Prepared()
      : Object<const CassPrepared, cass_prepared_free>() {}

  /**
   * Create the prepared object from the native driver object
   *
   * @param prepared Native driver object
   */
  Prepared(const CassPrepared* prepared)
      : Object<const CassPrepared, cass_prepared_free>(prepared) {}

  /**
   * Create the prepared object from a shared reference
   *
   * @param prepared Shared reference
   */
  Prepared(Ptr prepared)
      : Object<const CassPrepared, cass_prepared_free>(prepared) {}

  /**
   * Bind the prepared object and create a statement
   *
   * @return Statement
   */
  Statement bind() { return Statement(cass_prepared_bind(get())); }

  /**
   * Get the data type for a given column index
   *
   * @param index The column index to retrieve the data type
   * @return Data type at the specified column index
   */
  const CassDataType* data_type(size_t index) {
    return cass_prepared_parameter_data_type(get(), index);
  }

  /**
   * Get the data type for a given column name
   *
   * @param name The column name to retrieve the data type
   * @return Data type at the specified column name
   */
  const CassDataType* data_type(const std::string& name) {
    return cass_prepared_parameter_data_type_by_name(get(), name.c_str());
  }

  /**
   * Get the value type for a given column index
   *
   * @param index The column index to retrieve the value type
   * @return Value type at the specified column index
   */
  CassValueType value_type(size_t index) { return cass_data_type_type(data_type(index)); }

  /**
   * Get the value type for a given column name
   *
   * @param name The column name to retrieve the value type
   * @return Value type at the specified column index
   */
  CassValueType value_type(const std::string& name) { return cass_data_type_type(data_type(name)); }
};

}} // namespace test::driver

#endif // __TEST_PREPARED_HPP__
