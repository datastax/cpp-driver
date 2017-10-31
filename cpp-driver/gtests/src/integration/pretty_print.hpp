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
#include "cassandra.h"

#include <ostream>

/*********************************************************
 * Teach Google test how to print values from the driver *
 ********************************************************/

/**
 * Pretty print CassConsistency values
 *
 * @param consistency CassConsistency value to pretty print
 * @param output_stream Output stream to pretty print value to
 */
void PrintTo(CassConsistency consistency, std::ostream* output_stream);

/**
 * Pretty print CassError values
 *
 * @param error_code CassError value to pretty print
 * @param output_stream Output stream to pretty print value to
 */
void PrintTo(CassError error_code, std::ostream* output_stream);

/**
 * Pretty print CassValueType values
 *
 * @param value_type CassValueType value to pretty print
 * @param output_stream Output stream to pretty print value to
 */
void PrintTo(CassValueType value_type, std::ostream* output_stream);

#endif // __PRETTY_PRINT_HPP__
