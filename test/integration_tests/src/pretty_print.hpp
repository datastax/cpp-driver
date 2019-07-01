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
#ifndef __PRETTY_PRINT_HPP__
#define __PRETTY_PRINT_HPP__

#include <boost/test/unit_test.hpp>
#include <iostream>

#include "cassandra.h"

namespace boost { namespace test_tools { namespace tt_detail {

template <>
struct print_log_value<CassError> {
  /**
   * Pretty print CassError values
   *
   * @param output_stream Output stream to pretty print value to
   * @param error_code CassError value to pretty print
   */
  void operator()(std::ostream& output_stream, const CassError& error_code);
};

template <>
struct print_log_value<CassValueType> {
  /**
   * Pretty print CassValueType values
   *
   * @param output_stream Output stream to pretty print value to
   * @param value_type CassValueType value to pretty print
   */
  void operator()(std::ostream& output_stream, const CassValueType& value_type);
};

}}} // namespace boost::test_tools::tt_detail

#endif // __PRETTY_PRINT_HPP__