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

#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include "small_vector.hpp"

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_SUITE(small_vector)

BOOST_AUTO_TEST_CASE(simple)
{
  cass::SmallVector<int, 5> vec;
  BOOST_CHECK(vec.fixed().data.address() == vec.data());
  BOOST_CHECK(vec.fixed().is_used == true);

  for (int i = 0; i < 5; ++i) {
    vec.push_back(i);
    BOOST_CHECK(vec.fixed().data.address() == vec.data());
    BOOST_CHECK(vec.fixed().is_used == true);
  }

  vec.push_back(5);
  BOOST_CHECK(vec.fixed().data.address() != vec.data());
  BOOST_CHECK(vec.fixed().is_used == false);
}

BOOST_AUTO_TEST_SUITE_END()
