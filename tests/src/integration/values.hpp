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

#ifndef __TEST_VALUES_HPP__
#define __TEST_VALUES_HPP__

#include "values/ascii.hpp"
#include "values/blob.hpp"
#include "values/boolean.hpp"
#include "values/date.hpp"
#include "values/decimal.hpp"
#include "values/double.hpp"
#include "values/duration.hpp"
#include "values/float.hpp"
#include "values/inet.hpp"
#include "values/integer.hpp"
#include "values/list.hpp"
#include "values/map.hpp"
#include "values/set.hpp"
#include "values/time.hpp"
#include "values/timestamp.hpp"
#include "values/uuid.hpp"
#include "values/varchar.hpp"
#include "values/varint.hpp"

namespace test { namespace driver {

typedef NullableValue<values::Ascii> Ascii;
typedef NullableValue<values::BigInteger> BigInteger;
typedef NullableValue<values::Blob> Blob;
typedef NullableValue<values::Boolean> Boolean;
typedef NullableValue<values::Counter> Counter;
typedef NullableValue<values::Date> Date;
typedef NullableValue<values::Decimal> Decimal;
typedef NullableValue<values::Double> Double;
typedef NullableValue<values::Duration> Duration;
typedef NullableValue<values::Float> Float;
typedef NullableValue<values::Inet> Inet;
typedef NullableValue<values::Integer> Integer;
typedef NullableValue<values::SmallInteger> SmallInteger;
typedef NullableValue<values::Text> Text;
typedef NullableValue<values::Time> Time;
typedef NullableValue<values::Timestamp> Timestamp;
typedef NullableValue<values::TimeUuid> TimeUuid;
typedef NullableValue<values::TinyInteger> TinyInteger;
typedef NullableValue<values::Uuid> Uuid;
typedef NullableValue<values::Varchar> Varchar;
typedef NullableValue<values::Varint> Varint;

}} // namespace test::driver

#endif //__TEST_VALUES_HPP__
