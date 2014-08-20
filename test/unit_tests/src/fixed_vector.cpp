#define BOOST_TEST_DYN_LINK
#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include "fixed_vector.hpp"

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_SUITE(fixed_vector)

BOOST_AUTO_TEST_CASE(simple)
{
  cass::FixedVector<int, 5> vec;
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
