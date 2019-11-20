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

#ifndef __TEST_DSE_NULLABLE_VALUE_HPP__
#define __TEST_DSE_NULLABLE_VALUE_HPP__
#include "dse.h"

#include "nullable_value.hpp"

namespace test { namespace driver { namespace dse {

/**
 * DSE NullableValue is a templated interface for all the DSE server data types
 * provided by the DataStax DSE C/C++ driver. This interface will perform
 * expectations on the value type and other miscellaneous needs for testing;
 * while also allowing the value to be NULL.
 */
template <typename T>
class NullableValue : public driver::NullableValue<T> {
public:
  /**
   * Constructor for a NULL value
   */
  NullableValue(){};

  /**
   * Constructor for a nullable value; convenience constructor
   *
   * @param value Typed value
   */
  explicit NullableValue(const typename T::ConvenienceType& value)
      : driver::NullableValue<T>(value) {}

  /**
   * Constructor for a nullable value using the wrapped type
   *
   * @param value Wrapped type value
   */
  explicit NullableValue(const T& value)
      : driver::NullableValue<T>(value) {}

  /**
   * Constructor for a nullable value using the drivers primitive/collection
   * value
   *
   * @param value CassValue from driver query
   */
  explicit NullableValue(const CassValue* value)
      : driver::NullableValue<T>(value) {}

  /**
   * Generate the native driver object from the wrapped type
   *
   * @return Generated native reference object; may be empty
   */
  typename T::Native to_native() const { return this->value_.to_native(); }
};

}}} // namespace test::driver::dse

#endif // __TEST_DSE_NULLABLE_VALUE_HPP__
