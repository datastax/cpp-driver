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

#ifndef __TEST_DSE_VALUES_HPP__
#define __TEST_DSE_VALUES_HPP__

#include "values.hpp"
#include "values/dse_date_range.hpp"
#include "values/dse_line_string.hpp"
#include "values/dse_point.hpp"
#include "values/dse_polygon.hpp"

namespace test { namespace driver { namespace dse {

typedef NullableValue<values::dse::DateRange> DateRange;
typedef NullableValue<values::dse::LineString> LineString;
typedef NullableValue<values::dse::Point> Point;
typedef NullableValue<values::dse::Polygon> Polygon;

}}} // namespace test::driver::dse

#endif // __DRIVER_VALUE_DSE_VALUES_HPP__
