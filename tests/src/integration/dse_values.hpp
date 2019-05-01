/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
