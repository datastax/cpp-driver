#define BOOST_TEST_DYN_LINK
#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include <algorithm>

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/cstdint.hpp>
#include <boost/format.hpp>
#include <boost/thread.hpp>
#include <boost/chrono.hpp>

#include "cassandra.h"
#include "test_utils.hpp"

struct V1Tests : test_utils::SingleSessionTest {
    V1Tests() : SingleSessionTest(1, 0, 1) {
      test_utils::execute_query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT)
                                             % test_utils::SIMPLE_KEYSPACE % "1"));
      test_utils::execute_query(session, str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));
    }
};

BOOST_FIXTURE_TEST_SUITE(v1, V1Tests)

BOOST_AUTO_TEST_CASE(test)
{
}

BOOST_AUTO_TEST_SUITE_END()
