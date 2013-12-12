#define NTDDI_VERSION 0x06000000
#define _WIN32_WINNT 0x0600
#include <boost/test/unit_test.hpp>
#include "cql/cql.hpp"
#include "cql/cql_error.hpp"
#include "cql/internal/cql_util.hpp"
#include "cql/internal/cql_defines.hpp"
#include "cql/internal/cql_serialization.hpp"

BOOST_AUTO_TEST_SUITE(serialization)

BOOST_AUTO_TEST_CASE(decode_double_ptr)
{
    double d = 0;
    cql::cql_byte_t d_buffer[] = {0x40, 0x09, 0x25, 0xca, 0xcb, 0xeb, 0xa6, 0x57};
    cql::decode_double(d_buffer, d);
    BOOST_CHECK_CLOSE(3.1434532100000001, d, 0.0000000000000001); // not what you expect due to double limits
}

BOOST_AUTO_TEST_SUITE_END()
