#define BOOST_TEST_DYN_LINK
#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/cstdint.hpp>
#include <boost/format.hpp>
#include <boost/thread.hpp>

#include "cassandra.h"
#include "test_utils.hpp"

struct FutureCallbacksTests : public test_utils::SingleSessionTest {
    FutureCallbacksTests() : test_utils::SingleSessionTest(3, 0) {
      test_utils::execute_query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT)
                                                   % test_utils::SIMPLE_KEYSPACE % "1"));
      test_utils::execute_query(session, str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));
    }
};

BOOST_FIXTURE_TEST_SUITE(future_callbacks, FutureCallbacksTests)

BOOST_AUTO_TEST_CASE(test_connect)
{
}

BOOST_AUTO_TEST_CASE(test_close)
{
}

BOOST_AUTO_TEST_CASE(test_result)
{
}

BOOST_AUTO_TEST_CASE(test_after_set)
{
}

BOOST_AUTO_TEST_SUITE_END()

