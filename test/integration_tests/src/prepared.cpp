#define BOOST_TEST_DYN_LINK
#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include "cassandra.h"
#include "test_utils.hpp"

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/cstdint.hpp>
#include <boost/format.hpp>
#include <boost/thread.hpp>

struct PreparedTests : test_utils::MultipleNodesTest {
    PreparedTests() : SingleSessionTest(2, 0) {
    }

    ~PreparedTests() {

    }
};

BOOST_FIXTURE_TEST_SUITE(basics, PreparedTests)



BOOST_AUTO_TEST_SUITE_END()
