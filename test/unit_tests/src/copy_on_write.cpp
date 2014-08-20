#define BOOST_TEST_DYN_LINK
#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include "copy_on_write_ptr.hpp"

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_SUITE(copy_on_write)

BOOST_AUTO_TEST_CASE(simple)
{
  std::vector<int>* ptr = new std::vector<int>;
  cass::CopyOnWritePtr<std::vector<int> > vec(ptr);

  // Only a single reference so no copy should be made
  BOOST_CHECK(static_cast<const cass::CopyOnWritePtr<std::vector<int> >&>(vec).operator->() == ptr);
  vec->push_back(1);
  BOOST_CHECK(static_cast<const cass::CopyOnWritePtr<std::vector<int> >&>(vec).operator->() == ptr);

  // Make const reference to object
  const cass::CopyOnWritePtr<std::vector<int> > const_vec(vec);
  BOOST_CHECK((*const_vec)[0] == 1);
  BOOST_CHECK(const_vec.operator->() == ptr);

  // Force copy to be made
  vec->push_back(2);
  BOOST_CHECK(static_cast<const cass::CopyOnWritePtr<std::vector<int> >&>(vec).operator->() != ptr);
  BOOST_CHECK(const_vec.operator->() == ptr);
}

BOOST_AUTO_TEST_SUITE_END()

